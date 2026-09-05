"""Independent input/output controls for the LLM boundary."""

from __future__ import annotations

import json
import math
import re
from typing import Any


class GuardrailViolation(ValueError):
    pass


class LLMGuardrail:
    _sensitive_patterns = {
        "OpenAI/API secret": r"\b(?:sk|rk|pk)-[A-Za-z0-9_-]{16,}\b",
        "bearer/JWT token": r"\b(?:bearer\s+)?eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b",
        "cloud access key": r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
        "private key": r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        "email address": r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}",
        "IBAN": r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b",
        "payment card-like number": r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)",
        "long account-like number": r"(?<![A-Z0-9-])\d{10,}(?![A-Z0-9-])",
        "credential assignment": r"\b(?:password|passwd|secret|api[_ -]?key)\s*[:=]\s*\S+",
        "direct-identifier field": r"\b(?:full[_ -]?name|client[_ -]?name|customer[_ -]?name|date[_ -]?of[_ -]?birth|dob|national[_ -]?id|tax[_ -]?id|home[_ -]?address)\s*[:=]\s*\S+",
    }
    _injection_patterns = {
        "instruction override": r"\b(?:ignore|disregard|override)\b.{0,40}\b(?:previous|system|developer|instructions?)\b",
        "prompt extraction": r"\b(?:reveal|print|show|return)\b.{0,40}\b(?:system prompt|developer message|hidden instructions?)\b",
        "data exfiltration": r"\b(?:exfiltrate|leak|send)\b.{0,40}\b(?:secret|credential|private data)\b",
    }
    _prohibited_determinations = {
        "autonomous guilt finding": r"\b(?:is guilty|committed market abuse|definitely fraudulent|proven misconduct)\b",
        "autonomous closure": r"\b(?:automatically close|close the case without|no human review)\b",
    }

    def estimate_tokens(self, value: Any) -> int:
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return max(1, math.ceil(len(text) / 4))

    def validate_input(self, value: Any, max_tokens: int) -> int:
        serialized = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        self._reject(serialized, self._sensitive_patterns, "Sensitive data")
        self._reject(serialized, self._injection_patterns, "Prompt injection")
        estimated = self.estimate_tokens(serialized)
        if estimated > max_tokens:
            raise GuardrailViolation(f"Evidence packet exceeds the {max_tokens}-token input budget")
        return estimated

    def validate_output(self, value: dict[str, Any], allowed_refs: set[str]) -> None:
        serialized = json.dumps(value, ensure_ascii=False)
        self._reject(serialized, self._sensitive_patterns, "Sensitive output")
        self._reject(serialized, self._prohibited_determinations, "Prohibited determination")
        cited = value.get("citedEvidenceRefs", [])
        unsupported = sorted(set(cited).difference(allowed_refs))
        if unsupported:
            raise GuardrailViolation("LLM cited evidence outside the supplied allowlist: " + ", ".join(unsupported))
        if not cited:
            raise GuardrailViolation("LLM response contains no verified evidence references")
        narrative = " ".join(str(value) for key, value in value.items() if key != "citedEvidenceRefs")
        absent = [reference for reference in cited if reference not in narrative]
        if absent:
            raise GuardrailViolation("LLM listed citations that are not attached to its narrative: " + ", ".join(absent))

    def _reject(self, text: str, patterns: dict[str, str], prefix: str) -> None:
        matches = [label for label, pattern in patterns.items() if re.search(pattern, text, re.IGNORECASE | re.DOTALL)]
        if matches:
            raise GuardrailViolation(prefix + " blocked by guardrail: " + ", ".join(matches))
