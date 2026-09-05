import json
import hashlib
import os
import re
from pathlib import Path
from threading import Lock
from typing import Any

from .llm_guardrails import GuardrailViolation, LLMGuardrail
from .models import (CopilotAnalysis, EvidenceSummaryRequest,
                     InvestigationCopilotRequest, InvestigationCopilotResponse)
from .playbook_retriever import PlaybookRetriever


SYSTEM_INSTRUCTIONS = """You are an evidence-grounded trade-surveillance writing assistant.
Summarise only facts present in the supplied structured evidence. Do not determine misconduct,
infer missing ownership, calculate or change the priority score, or recommend autonomous closure.
Use cautious language such as 'potential', 'may', and 'requires investigator review'. Cite supplied
source references inline. If the supplied evidence is insufficient, state that the conclusion is not
supported by the case evidence. Return one concise investigator-readable paragraph."""

COPILOT_INSTRUCTIONS = """You are a controlled trade-surveillance investigation copilot.
Use only the compact evidence packet and retrieved playbook guidance. Produce one concise case summary,
one plausible legitimate counter-hypothesis, up to four next investigative actions, missing evidence,
and a confidence note. Attach exact supplied evidence references inline in square brackets and list the
same references in citedEvidenceRefs. Never infer identities, invent evidence, change the priority score,
determine misconduct, or recommend automatic case closure. When evidence is weak, say so explicitly.
The output supports a human investigator and is never a final decision."""


class OpenAIEvidenceSummarizer:
    def __init__(self, client: Any | None = None) -> None:
        self.enabled = os.getenv("LLM_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("OPENAI_MODEL", "").strip()
        self.max_output_tokens = max(100, min(800, int(os.getenv("LLM_SUMMARY_MAX_OUTPUT_TOKENS", "220"))))
        self._client = client
        self.guardrail = LLMGuardrail()

    @property
    def configured(self) -> bool:
        return self.enabled and bool(self.api_key) and bool(self.model)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured": self.configured,
            "provider": "openai" if self.enabled else "disabled",
            "model": self.model if self.configured else None,
            "keyPresent": bool(self.api_key),
            "purpose": "evidence-grounded summary only",
            "rawTradeDataAllowed": False,
            "directIdentifiersAllowed": False,
            "requestSchemaAllowlisted": True,
            "storeResponses": False,
            "maxOutputTokens": self.max_output_tokens,
        }

    def _client_instance(self) -> Any:
        if self._client is None:
            from openai import OpenAI

            self._client = OpenAI(api_key=self.api_key, timeout=20.0, max_retries=1)
        return self._client

    def summarize(self, request: EvidenceSummaryRequest) -> str:
        if not self.configured:
            raise RuntimeError("OpenAI evidence summarisation is not configured")

        evidence = request.model_dump()
        self.guardrail.validate_input(evidence, 2500)
        response = self._client_instance().responses.create(
            model=self.model,
            instructions=SYSTEM_INSTRUCTIONS,
            input="Create an evidence-grounded case summary from this JSON:\n" + json.dumps(evidence, ensure_ascii=False),
            max_output_tokens=self.max_output_tokens,
            store=False,
            tool_choice="none",
        )
        summary = (response.output_text or "").strip()
        if not summary:
            raise RuntimeError("OpenAI returned an empty evidence summary")
        return summary

    def _validate_sanitized(self, evidence: dict[str, Any]) -> None:
        serialized = json.dumps(evidence, ensure_ascii=False)
        prohibited = {
            "email address": r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}",
            "long account-like number": r"(?<![A-Z0-9-])\d{10,}(?![A-Z0-9-])",
        }
        matches = [label for label, pattern in prohibited.items() if re.search(pattern, serialized, re.IGNORECASE)]
        if matches:
            raise ValueError("Evidence package contains prohibited direct identifiers: " + ", ".join(matches))


class TokenEfficientInvestigationCopilot:
    """One-call, retrieval-grounded LLM workflow with independent DLP and citation guardrails."""

    def __init__(self, client: Any | None = None, cache_root: Path | None = None,
                 playbook_path: str | None = None) -> None:
        self.enabled = os.getenv("LLM_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("OPENAI_MODEL", "").strip()
        self.prompt_version = os.getenv("LLM_PROMPT_VERSION", "investigation-copilot-v1").strip()
        self.min_priority = max(0, min(100, int(os.getenv("LLM_MIN_PRIORITY", "70"))))
        self.min_coverage = max(0.0, min(1.0, float(os.getenv("LLM_MIN_EVIDENCE_COVERAGE", "0.75"))))
        self.min_disagreement = max(0.0, min(1.0, float(os.getenv("LLM_MIN_MODEL_DISAGREEMENT", "0.35"))))
        self.max_input_tokens = max(500, min(5000, int(os.getenv("LLM_MAX_INPUT_TOKENS", "1800"))))
        self.max_output_tokens = max(150, min(1200, int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "450"))))
        self.cache_root = cache_root or Path(os.getenv("LLM_CACHE_ROOT", "/data/llm-cache"))
        self.guardrail = LLMGuardrail()
        self.retriever = PlaybookRetriever(playbook_path)
        self._client = client
        self._lock = Lock()
        self._metrics = {"requests": 0, "apiCalls": 0, "cacheHits": 0, "gated": 0, "blocked": 0,
                         "inputTokens": 0, "outputTokens": 0}

    @property
    def configured(self) -> bool:
        return self.enabled and bool(self.api_key) and bool(self.model)

    def status(self) -> dict[str, Any]:
        with self._lock:
            metrics = dict(self._metrics)
        return {
            "enabled": self.enabled, "configured": self.configured,
            "provider": "openai" if self.enabled else "disabled", "model": self.model or None,
            "purpose": "summary + counter-hypothesis + next-best-action + evidence-gap analysis",
            "invocation": "one structured call for eligible cases only", "promptVersion": self.prompt_version,
            "eligibility": {"minimumPriority": self.min_priority, "minimumEvidenceCoverage": self.min_coverage,
                            "minimumModelDisagreement": self.min_disagreement},
            "tokenBudget": {"maximumInput": self.max_input_tokens, "maximumOutput": self.max_output_tokens},
            "controls": {"rawParquetAllowed": False, "directIdentifiersAllowed": False, "storeResponses": False,
                         "schemaAllowlisted": True, "localRetrieval": True, "localResultCache": True,
                         "citationVerification": True, "promptInjectionDetection": True},
            "metrics": metrics,
        }

    def analyse(self, request: InvestigationCopilotRequest) -> InvestigationCopilotResponse:
        with self._lock:
            self._metrics["requests"] += 1
        if request.evidenceCoverage < self.min_coverage:
            return self._gated(request.caseId, "Evidence coverage is below the LLM grounding threshold")
        if request.priority < self.min_priority and request.modelDisagreement < self.min_disagreement:
            return self._gated(request.caseId, "Local models agree this case does not require LLM analysis")
        if not self.configured:
            raise RuntimeError("OpenAI investigation copilot is not configured")

        try:
            self.guardrail.validate_input(request.model_dump(), 1_000_000)
            playbooks = self.retriever.retrieve(request.typology, request.assetClass,
                                                 request.riskDrivers + request.dataGaps, limit=2)
            packet = self._compact_packet(request, playbooks)
            estimated_input = self.guardrail.validate_input(packet, self.max_input_tokens)
            cache_key = self._cache_key(packet)
            cached = self._read_cache(cache_key)
            if cached:
                with self._lock:
                    self._metrics["cacheHits"] += 1
                return InvestigationCopilotResponse(
                    status="CACHED", caseId=request.caseId,
                    analysis=CopilotAnalysis.model_validate(cached["analysis"]), model=self.model,
                    playbookRefs=cached.get("playbookRefs", []), grounded=True,
                    tokenControl={"cacheHit": True, "apiCallMade": False, "estimatedInputTokens": estimated_input,
                                  "maxInputTokens": self.max_input_tokens, "maxOutputTokens": self.max_output_tokens})

            text_config: dict[str, Any] = {"format": {"type": "json_schema", "name": "investigation_copilot",
                                            "strict": True, "schema": CopilotAnalysis.model_json_schema()}}
            if self.model.lower().startswith(("gpt-5", "gpt-6")):
                text_config["verbosity"] = "low"
            response = self._client_instance().responses.create(
                model=self.model,
                instructions=COPILOT_INSTRUCTIONS,
                input=json.dumps(packet, ensure_ascii=False, separators=(",", ":")),
                max_output_tokens=self.max_output_tokens,
                store=False,
                tool_choice="none",
                prompt_cache_key=f"tsn-{request.typology.lower()}-{request.assetClass.lower()}",
                text=text_config,
            )
            usage = self._usage(response)
            with self._lock:
                self._metrics["apiCalls"] += 1
                self._metrics["inputTokens"] += usage["inputTokens"]
                self._metrics["outputTokens"] += usage["outputTokens"]
            playbook_refs = [item["id"] for item in playbooks]
            token_control = {"cacheHit": False, "apiCallMade": True, "estimatedInputTokens": estimated_input,
                             "actualInputTokens": usage["inputTokens"], "actualOutputTokens": usage["outputTokens"],
                             "maxInputTokens": self.max_input_tokens, "maxOutputTokens": self.max_output_tokens}
            try:
                raw = json.loads((response.output_text or "").strip())
                analysis = CopilotAnalysis.model_validate(raw)
                self.guardrail.validate_output(analysis.model_dump(), set(request.evidenceRefs))
            except (GuardrailViolation, ValueError, json.JSONDecodeError):
                with self._lock:
                    self._metrics["blocked"] += 1
                return InvestigationCopilotResponse(
                    status="ABSTAINED", caseId=request.caseId,
                    reason="Generated content did not pass output safety and grounding controls",
                    model=self.model, playbookRefs=playbook_refs, grounded=False,
                    tokenControl=token_control)
            self._write_cache(cache_key, {"analysis": analysis.model_dump(), "playbookRefs": playbook_refs})
            return InvestigationCopilotResponse(
                status="GENERATED", caseId=request.caseId, analysis=analysis, model=self.model,
                playbookRefs=playbook_refs, grounded=True,
                tokenControl=token_control)
        except GuardrailViolation:
            with self._lock:
                self._metrics["blocked"] += 1
            raise

    def _client_instance(self) -> Any:
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key, timeout=25.0, max_retries=1)
        return self._client

    def _compact_packet(self, request: InvestigationCopilotRequest, playbooks: list[dict[str, str]]) -> dict:
        return {
            "id": request.caseId, "typ": request.typology, "rgn": request.region, "asset": request.assetClass,
            "priority": request.priority, "coverage": request.evidenceCoverage,
            "modelDisagreement": request.modelDisagreement,
            "drivers": [item[:240] for item in request.riskDrivers[:5]],
            "reducers": [item[:240] for item in request.riskReducers[:3]],
            "timeline": [item[:200] for item in request.timeline[:8]],
            "relationships": [item[:220] for item in request.entityRelationships[:4]],
            "refs": request.evidenceRefs[:20], "gaps": [item[:200] for item in request.dataGaps[:5]],
            "playbooks": playbooks,
        }

    def _gated(self, case_id: str, reason: str) -> InvestigationCopilotResponse:
        with self._lock:
            self._metrics["gated"] += 1
        return InvestigationCopilotResponse(
            status="GATED", caseId=case_id, reason=reason, grounded=False,
            tokenControl={"cacheHit": False, "apiCallMade": False, "tokensUsed": 0,
                          "maxInputTokens": self.max_input_tokens, "maxOutputTokens": self.max_output_tokens})

    def _cache_key(self, packet: dict) -> str:
        value = json.dumps({"model": self.model, "prompt": self.prompt_version, "packet": packet},
                           sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(value.encode()).hexdigest()

    def _read_cache(self, key: str) -> dict | None:
        path = self.cache_root / f"{key}.json"
        try:
            return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None
        except (OSError, json.JSONDecodeError):
            return None

    def _write_cache(self, key: str, value: dict) -> None:
        self.cache_root.mkdir(parents=True, exist_ok=True)
        path = self.cache_root / f"{key}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, path)

    def _usage(self, response: Any) -> dict[str, int]:
        usage = getattr(response, "usage", None)
        def read(name: str) -> int:
            if isinstance(usage, dict):
                return int(usage.get(name, 0) or 0)
            return int(getattr(usage, name, 0) or 0) if usage is not None else 0
        return {"inputTokens": read("input_tokens"), "outputTokens": read("output_tokens")}
