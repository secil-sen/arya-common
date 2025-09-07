# created by Secil Sen
from __future__ import annotations
import os
import time
import uuid
from typing import Optional, Dict, Any
import httpx

class RAGNotifier:
    """
    RAG notification client.
    Environment variables:
      - RAG_BASE_URL: e.g. https://rag.example.com
      - RAG_API_KEY : Bearer token, optional
    """
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = (base_url or os.getenv("RAG_BASE_URL") or "").rstrip("/")
        if not self.base_url:
            raise RuntimeError("RAG_BASE_URL is not defined.")
        self.api_key = api_key or os.getenv("RAG_API_KEY") or ""
        self.timeout = timeout

    def _headers(self, idem: Optional[str]) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        if idem:
            h["Idempotency-Key"] = idem
        return h

    def notify_meeting_created(
        self,
        *,
        meeting_id: str,
        org_id: str,
        title: Optional[str] = None,
        started_at: Optional[str] = None,  # ISO8601 recommended
        metadata: Optional[Dict[str, Any]] = None,
        idem_key: Optional[str] = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
    ) -> None:
        """
        Notifies RAG about a new meeting being added.
        Expected payload (example):
          {
            "type": "meeting_created",
            "meeting_id": "...",
            "org_id": "...",
            "title": "...",
            "started_at": "...",
            "metadata": {...}
          }
        """
        url = f"{self.base_url}/meetings/notify"
        payload = {
            "type": "meeting_created",
            "meeting_id": meeting_id,
            "org_id": org_id,
            "title": title,
            "started_at": started_at,
            "metadata": metadata or {},
        }
        idem = idem_key or str(uuid.uuid4())
        self._post_with_retry(url, payload, idem, max_retries, backoff_base)

    def notify_finalize_ready(
        self,
        *,
        meeting_id: str,
        org_id: str,
        object_uri: str,
        version: int,
        count: int,
        checksum: str,
        idem_key: Optional[str] = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
    ) -> None:
        """
        Sends notification to RAG when finalize JSONL writing is completed.
        Expected payload (example):
          {
            "type": "finalize_ready",
            "meeting_id": "...",
            "org_id": "...",
            "object_uri": "s3://.../final.jsonl.zst",
            "version": 1,
            "count": 123,
            "checksum": "sha256..."
          }
        """
        url = f"{self.base_url}/meetings/notify"
        payload = {
            "type": "finalize_ready",
            "meeting_id": meeting_id,
            "org_id": org_id,
            "object_uri": object_uri,
            "version": version,
            "count": count,
            "checksum": checksum,
        }
        idem = idem_key or str(uuid.uuid4())
        self._post_with_retry(url, payload, idem, max_retries, backoff_base)

    def _post_with_retry(
        self,
        url: str,
        json_payload: Dict[str, Any],
        idem_key: str,
        max_retries: int,
        backoff_base: float,
    ) -> None:
        for attempt in range(1, max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    resp = client.post(url, json=json_payload, headers=self._headers(idem_key))
                # 2xx success
                if 200 <= resp.status_code < 300:
                    return
                # 409 idempotency conflict can be considered success
                if resp.status_code == 409:
                    return
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            except Exception:
                if attempt == max_retries:
                    raise
                time.sleep(backoff_base * (2 ** (attempt - 1)))


# Simple helper functions (for use without creating a class)
def notify_meeting_created(**kwargs) -> None:
    RAGNotifier().notify_meeting_created(**kwargs)


def notify_finalize_ready(**kwargs) -> None:
    RAGNotifier().notify_finalize_ready(**kwargs)