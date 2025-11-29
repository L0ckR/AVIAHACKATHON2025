"""Baseline GRU model and helpers for next-action prediction."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

import torch
import torch.nn as nn


class Vocabulary:
    """Token-to-id helper that keeps pad/unk stable."""

    def __init__(self, pad_token: str = "<pad>", unk_token: str = "<unk>"):
        self.pad_token = pad_token
        self.unk_token = unk_token
        self.token_to_id = {pad_token: 0, unk_token: 1}
        self.id_to_token = [pad_token, unk_token]
        self.pad_id = 0
        self.unk_id = 1

    def add(self, token: Optional[str]) -> int:
        if token is None:
            return self.unk_id
        if token in self.token_to_id:
            return self.token_to_id[token]
        idx = len(self.id_to_token)
        self.token_to_id[token] = idx
        self.id_to_token.append(token)
        return idx

    def encode(self, tokens: Iterable[Optional[str]], grow: bool = True) -> List[int]:
        ids = []
        for tok in tokens:
            if grow:
                ids.append(self.add(tok))
            else:
                ids.append(self.token_to_id.get(tok, self.unk_id))
        return ids

    def __len__(self) -> int:
        return len(self.id_to_token)


@dataclass
class ModelConfig:
    num_actions: int
    num_products: Optional[int] = None
    d_model: int = 64
    num_layers: int = 1
    dropout: float = 0.1
    use_product_context: bool = False


class NextActionGRU(nn.Module):
    """Small GRU encoder with heads for action (and optional product)."""

    def __init__(self, cfg: ModelConfig):
        super().__init__()
        self.cfg = cfg
        self.action_emb = nn.Embedding(cfg.num_actions, cfg.d_model, padding_idx=0)
        self.seasonal_proj = nn.Linear(4, cfg.d_model)

        self.product_emb = (
            nn.Embedding(cfg.num_products, cfg.d_model, padding_idx=0)
            if cfg.use_product_context and cfg.num_products
            else None
        )

        input_dim = cfg.d_model + cfg.d_model
        if self.product_emb is not None:
            input_dim += cfg.d_model

        self.gru = nn.GRU(
            input_size=input_dim,
            hidden_size=cfg.d_model,
            num_layers=cfg.num_layers,
            dropout=cfg.dropout if cfg.num_layers > 1 else 0.0,
            batch_first=True,
        )
        self.action_head = nn.Linear(cfg.d_model, cfg.num_actions)
        self.product_head = (
            nn.Linear(cfg.d_model, cfg.num_products) if cfg.num_products else None
        )

    def forward(
        self,
        action_ids: torch.Tensor,
        seasonal_feats: torch.Tensor,
        product_ids: Optional[torch.Tensor] = None,
        lengths: Optional[torch.Tensor] = None,
    ):
        """action_ids/product_ids: [B, T], seasonal_feats: [B, T, 4]."""

        emb = [self.action_emb(action_ids), self.seasonal_proj(seasonal_feats)]
        if self.product_emb is not None and product_ids is not None:
            emb.append(self.product_emb(product_ids))

        x = torch.cat(emb, dim=-1)

        if lengths is not None:
            packed = nn.utils.rnn.pack_padded_sequence(
                x, lengths.cpu(), batch_first=True, enforce_sorted=False
            )
            _, h = self.gru(packed)
        else:
            _, h = self.gru(x)

        h_last = h[-1]
        action_logits = self.action_head(h_last)
        product_logits = self.product_head(h_last) if self.product_head else None
        return action_logits, product_logits


def collate_sequences(
    batch: List[dict],
    action_vocab: Vocabulary,
    product_vocab: Optional[Vocabulary] = None,
    device: Optional[torch.device] = None,
    grow_vocabs: bool = True,
):
    """Pad a list of sequence dicts from data_pipeline.build_sequences."""

    max_len = max(len(sample["history_actions"]) for sample in batch)
    batch_size = len(batch)

    action_hist = torch.full(
        (batch_size, max_len),
        action_vocab.pad_id,
        dtype=torch.long,
    )
    seasonal = torch.zeros((batch_size, max_len, 4), dtype=torch.float)

    product_hist = None
    if product_vocab is not None:
        product_hist = torch.full(
            (batch_size, max_len),
            product_vocab.pad_id,
            dtype=torch.long,
        )

    target_actions = torch.empty(batch_size, dtype=torch.long)
    target_products = None
    if product_vocab is not None:
        target_products = torch.empty(batch_size, dtype=torch.long)

    lengths = []

    for i, sample in enumerate(batch):
        actions = sample["history_actions"]
        action_ids = action_vocab.encode(actions, grow=grow_vocabs)
        target_actions[i] = (
            action_vocab.add(sample["target_action"])
            if grow_vocabs
            else action_vocab.token_to_id.get(sample["target_action"], action_vocab.unk_id)
        )

        seasonal_feats = sample.get("history_seasonal") or []
        seq_len = len(action_ids)
        lengths.append(seq_len)

        action_hist[i, :seq_len] = torch.tensor(action_ids, dtype=torch.long)
        if seasonal_feats:
            seasonal[i, :seq_len, :] = torch.tensor(seasonal_feats, dtype=torch.float)

        if product_vocab is not None and sample.get("history_products") is not None:
            prod_ids = product_vocab.encode(sample["history_products"], grow=grow_vocabs)
            product_hist[i, :seq_len] = torch.tensor(prod_ids, dtype=torch.long)
            target_products[i] = (
                product_vocab.add(sample["target_product"])
                if grow_vocabs
                else product_vocab.token_to_id.get(sample["target_product"], product_vocab.unk_id)
            )

    lengths_t = torch.tensor(lengths, dtype=torch.long)

    if device:
        action_hist = action_hist.to(device)
        seasonal = seasonal.to(device)
        target_actions = target_actions.to(device)
        lengths_t = lengths_t.to(device)
        if product_hist is not None:
            product_hist = product_hist.to(device)
        if target_products is not None:
            target_products = target_products.to(device)

    return {
        "action_hist": action_hist,
        "product_hist": product_hist,
        "seasonal": seasonal,
        "lengths": lengths_t,
        "target_actions": target_actions,
        "target_products": target_products,
    }
