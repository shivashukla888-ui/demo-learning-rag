"""Small local retrieval layer: no external vector store and no embedding tokens."""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from pathlib import Path


class PlaybookRetriever:
    def __init__(self, path: str | None = None):
        configured = path or os.getenv("LLM_PLAYBOOK_PATH", "/app/config/typology-playbooks.json")
        self.path = Path(configured)
        self.records = json.loads(self.path.read_text(encoding="utf-8")) if self.path.is_file() else []
        documents = [self._tokens(self._document(item)) for item in self.records]
        document_frequency = Counter(token for document in documents for token in set(document))
        self.idf = {token: math.log((1 + len(documents)) / (1 + frequency)) + 1
                    for token, frequency in document_frequency.items()}
        self.vectors = [self._vector(document) for document in documents]

    def retrieve(self, typology: str, asset_class: str, signals: list[str], limit: int = 2) -> list[dict[str, str]]:
        if not self.records:
            return []
        query = " ".join([typology, asset_class, *signals])
        query_vector = self._vector(self._tokens(query))
        scores = [self._cosine(vector, query_vector) for vector in self.vectors]
        ranked = sorted(range(len(scores)), key=lambda index: (-scores[index], self.records[index]["id"]))
        selected = []
        for index in ranked:
            record = self.records[index]
            if scores[index] <= 0 and selected:
                break
            selected.append({"id": record["id"], "guidance": record["guidance"][:280]})
            if len(selected) >= max(1, min(limit, 3)):
                break
        return selected

    def _document(self, item: dict) -> str:
        return " ".join([item.get("typology", ""), item.get("assetClass", ""), item.get("title", ""), item.get("guidance", "")])

    def _tokens(self, value: str) -> list[str]:
        return [token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) > 2]

    def _vector(self, tokens: list[str]) -> dict[str, float]:
        counts = Counter(tokens)
        return {token: count * self.idf.get(token, 1.0) for token, count in counts.items()}

    def _cosine(self, left: dict[str, float], right: dict[str, float]) -> float:
        if not left or not right:
            return 0.0
        numerator = sum(value * right.get(token, 0.0) for token, value in left.items())
        left_norm = math.sqrt(sum(value * value for value in left.values()))
        right_norm = math.sqrt(sum(value * value for value in right.values()))
        return numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0
