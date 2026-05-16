# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_streaming.py
# ======================================================

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping

from app.brokers.streaming import (
    BrokerStreamEvent,
    BrokerStreamEventSink,
    BrokerStreamHealth,
    BrokerStreamMonitor,
)
from app.services.alpaca_account_resolver import (
    AlpacaAccountResolutionError,
    LaravelAlpacaAccountResolver,
    ResolvedAlpacaAccountContext,
)
from app.shared.config import get_settings

ALPACA_PAPER_STREAM_URL = "wss://paper-api.alpaca.markets/stream"
ALPACA_LIVE_STREAM_URL = "wss://api.alpaca.markets/stream"
ALPACA_TRADE_UPDATES_STREAM = "trade_updates"
logger = logging.getLogger(__name__)


class AlpacaStreamConfigurationError(RuntimeError):
    """Raised when a stream runner is missing safe configuration."""


def alpaca_stream_url(environment: str) -> str:
    return ALPACA_LIVE_STREAM_URL if str(environment).strip().lower() == "live" else ALPACA_PAPER_STREAM_URL


def build_alpaca_auth_message(*, key_id: str, secret: str) -> dict[str, str]:
    return {"action": "auth", "key": key_id, "secret": secret}


def build_alpaca_subscribe_message(streams: Iterable[str] | None = None) -> dict[str, Any]:
    requested_streams = list(streams or [ALPACA_TRADE_UPDATES_STREAM])
    return {"action": "listen", "data": {"streams": requested_streams}}


def redact_alpaca_stream_message(message: Mapping[str, Any]) -> dict[str, Any]:
    redacted = dict(message)
    if "key" in redacted:
        redacted["key"] = "***"
    if "secret" in redacted:
        redacted["secret"] = "***"
    if isinstance(redacted.get("data"), Mapping):
        redacted["data"] = dict(redacted["data"])
    return redacted


@dataclass(frozen=True)
class AlpacaStreamRuntimeScope:
    uid: str
    account_id: str | int
    product_id: str
    slot_number: int = 1
    account_ref: str | int | None = None

    @property
    def base_path(self) -> str:
        return (
            f"users/{self.uid}/accounts/{self.account_id}/{self.product_id}"
            f"/current/slots/slot_{self.slot_number}"
        )


class FirestoreBrokerStreamEventSink:
    """Writes sanitized broker stream state to the account-scoped runtime path."""

    def __init__(self, *, firestore_client: Any, scope: AlpacaStreamRuntimeScope) -> None:
        self._client = firestore_client
        self._scope = scope

    def write_stream_event(self, event: BrokerStreamEvent) -> None:
        safe_payload = event.to_customer_payload()
        safe_payload.update(
            {
                "account_ref": self._scope.account_ref,
                "product_id": self._scope.product_id,
                "stream_health": {
                    "stream_status": "stream_connected",
                    "last_event_at": event.received_at.isoformat(),
                    "safe_user_message": "Broker stream status updated.",
                },
                "last_event_at": event.received_at.isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        self._document_ref(f"{self._scope.base_path}/broker_stream/current").set(safe_payload, merge=True)

    def write_stream_health(self, health: BrokerStreamHealth) -> None:
        payload = {
            "broker": health.broker,
            "account_ref": self._scope.account_ref,
            "product_id": self._scope.product_id,
            "stream_health": health.to_runtime_metadata(),
            "last_event_at": health.last_event_at.isoformat() if health.last_event_at else None,
            "safe_user_message": health.safe_user_message,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self._document_ref(f"{self._scope.base_path}/broker_stream/current").set(payload, merge=True)

    def _document_ref(self, path: str) -> Any:
        segments = [segment for segment in path.strip("/").split("/") if segment]
        if len(segments) % 2 != 0:
            raise ValueError(f"Firestore document path must have an even segment count: {path}")
        ref = self._client
        for index, segment in enumerate(segments):
            ref = ref.collection(segment) if index % 2 == 0 else ref.document(segment)
        return ref


def write_missing_credentials_health(*, sink: BrokerStreamEventSink, account_ref: str | int | None = None) -> None:
    sink.write_stream_health(
        BrokerStreamHealth(
            broker="alpaca",
            status="broker_stream_credentials_missing",
            account_ref=account_ref,
            reason_code="broker_stream_credentials_missing",
            safe_user_message="Broker stream credentials are missing for this linked account.",
        )
    )


def build_alpaca_stream_transport_from_context(
    *,
    account_context: ResolvedAlpacaAccountContext,
    sink: BrokerStreamEventSink,
    account_ref: str | int | None = None,
    max_messages: int | None = None,
    max_run_seconds: float | None = None,
) -> AlpacaWebsocketTransport:
    if not account_context.key_id or not account_context.secret:
        write_missing_credentials_health(sink=sink, account_ref=account_ref)
        raise AlpacaStreamConfigurationError("Alpaca stream credentials are missing for the linked broker account.")
    return AlpacaWebsocketTransport(
        key_id=account_context.key_id,
        secret=account_context.secret,
        environment=account_context.environment,
        account_ref=account_ref,
        sink=sink,
        max_messages=max_messages,
        max_run_seconds=max_run_seconds,
    )


class AlpacaWebsocketTransport:
    """Real Alpaca trade_updates stream transport behind the monitoring boundary."""

    def __init__(
        self,
        *,
        key_id: str,
        secret: str,
        environment: str = "paper",
        account_ref: str | int | None = None,
        sink: BrokerStreamEventSink,
        streams: Iterable[str] | None = None,
        stale_after_seconds: int = 120,
        max_messages: int | None = None,
        max_run_seconds: float | None = None,
    ) -> None:
        self._key_id = key_id
        self._secret = secret
        self._url = alpaca_stream_url(environment)
        self._streams = list(streams or [ALPACA_TRADE_UPDATES_STREAM])
        self._max_messages = max_messages
        self._max_run_seconds = max_run_seconds
        self._monitor = BrokerStreamMonitor(
            broker="alpaca",
            account_ref=account_ref,
            sink=sink,
            stale_after_seconds=stale_after_seconds,
        )

    async def run(self) -> int:
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover - exercised only when optional dependency is absent.
            raise AlpacaStreamConfigurationError("The websockets package is required for Alpaca stream monitoring.") from exc

        message_count = 0
        started_at = datetime.now(UTC)
        completed_validation_window = False
        try:
            async with websockets.connect(self._url, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps(build_alpaca_auth_message(key_id=self._key_id, secret=self._secret)))
                auth_message = await websocket.recv()
                if self._is_auth_failure(auth_message):
                    self._monitor.mark_auth_failed()
                    return message_count
                self._monitor.mark_auth_acknowledged()

                await websocket.send(json.dumps(build_alpaca_subscribe_message(self._streams)))
                subscribe_message = await websocket.recv()
                if self._is_auth_failure(subscribe_message):
                    self._monitor.mark_auth_failed()
                    return message_count
                self._monitor.mark_subscribed(subscribe_message)

                self._monitor.mark_connected()
                while not self._monitor.closed:
                    receive_timeout = None
                    if self._max_run_seconds is not None:
                        elapsed = (datetime.now(UTC) - started_at).total_seconds()
                        remaining = self._max_run_seconds - elapsed
                        if remaining <= 0:
                            completed_validation_window = True
                            break
                        receive_timeout = max(0.1, min(5.0, remaining))
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=receive_timeout)
                    except TimeoutError:
                        continue
                    self._monitor.handle_message(message)
                    message_count += 1
                    if self._max_messages is not None and message_count >= self._max_messages:
                        break
        except asyncio.CancelledError:
            self._monitor.close()
            raise
        except Exception as exc:
            self._monitor.handle_disconnect(exc)
        finally:
            if not self._monitor.closed:
                if completed_validation_window:
                    self._monitor.mark_idle_connected()
                else:
                    self._monitor.close()
        return message_count

    def close(self) -> None:
        self._monitor.close()

    @staticmethod
    def _is_auth_failure(message: str | bytes | Mapping[str, Any]) -> bool:
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="ignore")
        if isinstance(message, str):
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                return False
        else:
            payload = dict(message)
        text = json.dumps(payload).lower()
        return "unauthorized" in text or "auth failed" in text or "authorization" in text and "failed" in text


def _firestore_client() -> Any:
    from google.cloud import firestore

    settings = get_settings()
    if settings.firestore_project_id:
        return firestore.Client(project=settings.firestore_project_id, database=settings.firestore_database_id)
    return firestore.Client(database=settings.firestore_database_id)


def _build_runner(argv: list[str] | None = None) -> AlpacaWebsocketTransport:
    parser = argparse.ArgumentParser(description="Run sanitized Alpaca trade_updates stream monitoring.")
    parser.add_argument("--environment", choices=["paper", "live"], default=os.getenv("ALPACA_STREAM_ENVIRONMENT", "paper"))
    parser.add_argument("--uid")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--slot-number", type=int, default=1)
    parser.add_argument("--account-ref")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--max-run-seconds", type=float)
    parser.add_argument("--use-env-credentials", action="store_true")
    args = parser.parse_args(argv)
    settings = get_settings()
    account_context: ResolvedAlpacaAccountContext | None = None
    if not args.use_env_credentials:
        try:
            account_context = LaravelAlpacaAccountResolver(settings).resolve_runtime_account_for_slot(
                account_id=int(args.account_id),
                slot_number=max(1, int(args.slot_number)),
                product_id=str(args.product_id),
            )
        except (AlpacaAccountResolutionError, ValueError) as exc:
            if not args.uid:
                raise AlpacaStreamConfigurationError(
                    "Linked Alpaca account credentials could not be resolved and no uid was provided for stream health writeback."
                ) from exc
    resolved_uid = account_context.uid if account_context is not None else args.uid
    if not resolved_uid:
        raise AlpacaStreamConfigurationError("A uid is required for broker stream health writeback.")
    scope = AlpacaStreamRuntimeScope(
        uid=resolved_uid,
        account_id=args.account_id,
        product_id=args.product_id,
        slot_number=args.slot_number,
        account_ref=args.account_ref,
    )
    sink = FirestoreBrokerStreamEventSink(firestore_client=_firestore_client(), scope=scope)
    if account_context is not None:
        return build_alpaca_stream_transport_from_context(
            account_context=account_context,
            sink=sink,
            account_ref=args.account_ref,
            max_messages=args.max_messages,
            max_run_seconds=args.max_run_seconds,
        )
    key_id = settings.alpaca_api_key_id
    secret = settings.alpaca_api_secret
    if not key_id or not secret:
        write_missing_credentials_health(sink=sink, account_ref=args.account_ref)
        raise AlpacaStreamConfigurationError("Alpaca stream credentials are not configured.")
    return AlpacaWebsocketTransport(
        key_id=key_id,
        secret=secret,
        environment=args.environment,
        account_ref=args.account_ref,
        sink=sink,
        max_messages=args.max_messages,
        max_run_seconds=args.max_run_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    transport = _build_runner(argv)
    asyncio.run(transport.run())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
