# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_paper_trading.py
# ======================================================

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from email.message import Message
from socket import timeout as SocketTimeout
from typing import Callable
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.brokers.models import (
    BrokerAccountState,
    BrokerAssetState,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPositionState,
)
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})
SUPPORTED_CRYPTO_ASSET_TYPES = frozenset({"crypto"})
SUPPORTED_ADMIN_CRYPTO_MONITOR_SYMBOLS = frozenset({"UNI/USD", "LINK/USD", "BTC/USD", "ETH/USD"})
SUPPORTED_ADMIN_CRYPTO_MONITOR_UIDS = frozenset({"admin-runtime-monitor-prime", "admin-runtime-monitor-execution"})
SUPPORTED_PRODUCT_KEYS = frozenset({"stocks.bismel1"})
RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
NON_RETRYABLE_HTTP_STATUSES = frozenset({400, 401, 403, 404, 422})

logger = logging.getLogger(__name__)


class HttpRequestProtocol(Protocol):
    def request_json(
        self,
        *,
        url: str,
        method: str,
        headers: dict[str, str],
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        ...


class UrllibRequestJsonClient:
    def request_json(
        self,
        *,
        url: str,
        method: str,
        headers: dict[str, str],
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        resolved_headers = dict(headers)
        if body is not None:
            resolved_headers["Content-Type"] = "application/json"
        request = Request(url=url, headers=resolved_headers, data=body, method=method)
        with urlopen(request, timeout=27) as response:
            raw_payload = response.read().decode("utf-8")
        return json.loads(raw_payload) if raw_payload else {}


class AlpacaPaperTradingError(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        retryable: bool = False,
        http_status: int | None = None,
        raw_response: dict[str, object] | None = None,
        error_category: str | None = None,
        attempt_count: int = 1,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.http_status = http_status
        self.raw_response = raw_response
        self.error_category = error_category or code.removeprefix("broker_")
        self.attempt_count = attempt_count
        self.retry_count = max(0, attempt_count - 1)
        self.retry_after = retry_after


@dataclass(frozen=True)
class AlpacaBrokerRetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 2.0
    jitter_seconds: float = 0.05


@dataclass(frozen=True)
class AlpacaPaperAccountState:
    buying_power: float | None
    open_positions_count: int
    equity: float | None = None
    total_exposure: float | None = None


@dataclass(frozen=True)
class AlpacaPaperAssetState:
    symbol: str
    tradable: bool
    status: str | None = None


@dataclass(frozen=True)
class AlpacaPaperPositionState:
    symbol: str
    qty: float
    market_value: float | None = None
    avg_entry_price: float | None = None


@dataclass(frozen=True)
class AlpacaPaperSubmissionState:
    account: AlpacaPaperAccountState
    asset: AlpacaPaperAssetState
    position: AlpacaPaperPositionState | None


@dataclass(frozen=True)
class AlpacaPaperExecutionResult:
    action: str
    submitted: bool
    order_status: str
    order_id: str | None
    client_order_id: str | None
    side: str | None
    notional: float | None
    add_tier: int | None = None
    skipped_reason: str | None = None
    retry_count: int = 0
    broker_error_code: str | None = None
    broker_error_message: str | None = None
    raw_response: dict[str, object] | None = None


class AlpacaPaperTradingAdapter:
    def __init__(
        self,
        settings: AppConfig,
        http_client: HttpRequestProtocol | None = None,
        retry_policy: AlpacaBrokerRetryPolicy | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or UrllibRequestJsonClient()
        self._retry_policy = retry_policy or AlpacaBrokerRetryPolicy()
        self._sleeper = sleeper or time.sleep

    def submit_first_lot_buy(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        notional: float,
        client_order_id: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        self._ensure_tradable_context(
            symbol=symbol,
            asset_type=asset_type,
            product_key=product_key,
            credential_context=credential_context,
        )
        return self._submit_notional_buy(
            symbol=symbol,
            notional=notional,
            client_order_id=client_order_id,
            action="FirstLot",
            credential_context=credential_context,
        )

    def submit_multi_buy(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        notional: float,
        client_order_id: str,
        action: str = "MULTI",
        add_tier: int | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        self._ensure_tradable_context(
            symbol=symbol,
            asset_type=asset_type,
            product_key=product_key,
            credential_context=credential_context,
        )
        return self._submit_notional_buy(
            symbol=symbol,
            notional=notional,
            client_order_id=client_order_id,
            action=action,
            add_tier=add_tier,
            credential_context=credential_context,
        )

    def submit_market_order_qty(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_order_id: str,
        action: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        normalized_side = (side or "").strip().lower()
        if normalized_side not in {"buy", "sell"}:
            raise ValueError(f"Market order side must be 'buy' or 'sell'. Received {side!r}.")
        if qty <= 0:
            raise ValueError("Market order qty must be greater than zero.")
        request_model = BrokerOrderRequest(
            account_id=None if credential_context is None else credential_context.alpaca_account_id,
            symbol=symbol,
            side=normalized_side,
            order_type="market",
            time_in_force=_resolve_market_order_time_in_force(symbol),
            qty=qty,
            client_order_id=client_order_id,
            execution_mode=None if credential_context is None else credential_context.environment,
            metadata={"action": action, "credential_context": credential_context},
        )
        return _paper_execution_result_from_broker_order_result(
            self.submit_order(request_model),
            action=action,
        )

    def submit_market_order_notional(
        self,
        *,
        symbol: str,
        side: str,
        notional: float,
        client_order_id: str,
        action: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        normalized_side = (side or "").strip().lower()
        if normalized_side != "buy":
            raise ValueError("Notional market orders are currently supported for buy side only.")
        if notional <= 0:
            raise ValueError("Market order notional must be greater than zero.")
        request_model = BrokerOrderRequest(
            account_id=None if credential_context is None else credential_context.alpaca_account_id,
            symbol=symbol,
            side=normalized_side,
            order_type="market",
            time_in_force=_resolve_market_order_time_in_force(symbol),
            notional=round(notional, 2),
            client_order_id=client_order_id,
            execution_mode=None if credential_context is None else credential_context.environment,
            metadata={"action": action, "credential_context": credential_context},
        )
        return _paper_execution_result_from_broker_order_result(
            self.submit_order(request_model),
            action=action,
        )

    def close_position(
        self,
        account_id: str | None = None,
        symbol: str | None = None,
        metadata: dict[str, object] | None = None,
        *,
        asset_type: str | None = None,
        product_key: str | None = None,
        action: str | None = None,
        client_order_id: str | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult | BrokerOrderResult:
        if account_id is not None and action is None and asset_type is None and product_key is None and client_order_id is None:
            if symbol is None:
                raise ValueError("Broker close_position requires a symbol.")
            return self.broker_close_position(account_id=account_id, symbol=symbol, metadata=metadata)
        if symbol is None or asset_type is None or product_key is None or action is None or client_order_id is None:
            raise ValueError("Runtime close_position requires symbol, asset_type, product_key, action, and client_order_id.")
        self._ensure_tradable_context(
            symbol=symbol,
            asset_type=asset_type,
            product_key=product_key,
            credential_context=credential_context,
        )
        return self.close_position_symbol(
            symbol=symbol,
            action=action,
            client_order_id=client_order_id,
            credential_context=credential_context,
        )

    def close_position_symbol(
        self,
        *,
        symbol: str,
        action: str,
        client_order_id: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/positions/{symbol.upper()}",
            method="DELETE",
            headers=self._headers(client_order_id=client_order_id, credential_context=credential_context),
            operation="close_position_symbol",
            symbol=symbol.upper(),
            side="sell",
            client_order_id=client_order_id,
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "submitted")),
            order_id=_maybe_string(payload.get("id")),
            client_order_id=client_order_id,
            side="sell",
            notional=None,
            raw_response=payload,
        )

    def get_submission_state(
        self,
        *,
        symbol: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperSubmissionState:
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        headers = self._headers(client_order_id="prime-state-probe", credential_context=credential_context)
        account_payload = self._request_json(
            url=f"{base_url}/v2/account",
            method="GET",
            headers=headers,
            operation="get_submission_state_account",
            symbol=symbol.upper(),
            client_order_id="prime-state-probe",
        )
        positions_payload = self._request_json(
            url=f"{base_url}/v2/positions",
            method="GET",
            headers=headers,
            operation="get_submission_state_positions",
            symbol=symbol.upper(),
            client_order_id="prime-state-probe",
        )
        asset_payload = self._request_json(
            url=f"{base_url}/v2/assets/{symbol.upper()}",
            method="GET",
            headers=headers,
            operation="get_submission_state_asset",
            symbol=symbol.upper(),
            client_order_id="prime-state-probe",
        )
        position_payload = None
        try:
            position_payload = self._request_json(
                url=f"{base_url}/v2/positions/{symbol.upper()}",
                method="GET",
                headers=headers,
                operation="get_submission_state_position",
                symbol=symbol.upper(),
                client_order_id="prime-state-probe",
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status != 404:
                raise
        broker_account = _broker_account_state_from_alpaca_payload(
            payload=account_payload,
            account_id=None if credential_context is None else credential_context.alpaca_account_id,
        )
        broker_asset = _broker_asset_state_from_alpaca_payload(payload=asset_payload, symbol=symbol)
        broker_position = None if not position_payload else _broker_position_state_from_alpaca_payload(
            payload=position_payload,
            symbol=symbol,
            account_id=None if credential_context is None else credential_context.alpaca_account_id,
        )
        return _submission_state_from_broker_models(
            account=broker_account,
            asset=broker_asset,
            position=broker_position,
            open_positions_count=len(positions_payload) if isinstance(positions_payload, list) else 0,
            total_exposure=_sum_position_market_value(positions_payload),
        )

    def list_recent_orders(
        self,
        *,
        credential_context: ResolvedAlpacaAccountContext | None = None,
        limit: int = 50,
    ) -> list[dict[str, object]]:
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/orders?status=all&limit={max(1, int(limit))}",
            method="GET",
            headers=self._headers(client_order_id="prime-orders-probe", credential_context=credential_context),
            operation="list_recent_orders",
            client_order_id="prime-orders-probe",
        )
        return payload if isinstance(payload, list) else []

    def submit_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        credential_context = request.metadata.get("credential_context") if isinstance(request.metadata, dict) else None
        if not isinstance(credential_context, ResolvedAlpacaAccountContext):
            credential_context = None
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        try:
            payload = self._request_json(
                url=f"{base_url}/v2/orders",
                method="POST",
                headers=self._headers(client_order_id=request.client_order_id, credential_context=credential_context),
                payload=_alpaca_order_payload_from_broker_request(request),
                operation="submit_order",
                symbol=request.symbol,
                side=request.side,
                client_order_id=request.client_order_id,
            )
        except AlpacaPaperTradingError as exc:
            return _broker_order_result_from_alpaca_error(request=request, error=exc)
        return _broker_order_result_from_alpaca_payload(payload=payload, request=request)

    def broker_close_position(self, account_id: str, symbol: str, metadata: dict[str, object] | None = None) -> BrokerOrderResult:
        resolved_symbol = symbol.strip().upper()
        client_order_id = str((metadata or {}).get("client_order_id") or f"broker-close-{resolved_symbol.lower()}")
        base_url = self._resolve_base_url(None).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/positions/{resolved_symbol}",
            method="DELETE",
            headers=self._headers(client_order_id=client_order_id, credential_context=None),
            operation="close_position",
            symbol=resolved_symbol,
            side="sell",
            client_order_id=client_order_id,
        )
        raw_payload = payload if isinstance(payload, dict) else {}
        return BrokerOrderResult(
            ok=True,
            broker="alpaca",
            account_id=account_id,
            symbol=_maybe_string(raw_payload.get("symbol")) or resolved_symbol,
            order_id=_maybe_string(raw_payload.get("id")),
            client_order_id=_maybe_string(raw_payload.get("client_order_id")) or client_order_id,
            status=_maybe_string(raw_payload.get("status")) or "submitted",
            side=_maybe_string(raw_payload.get("side")) or "sell",
            notional=_maybe_float(raw_payload.get("notional")),
            raw_response=payload,
        )

    def broker_cancel_order(self, account_id: str, order_id: str, metadata: dict[str, object] | None = None) -> BrokerOrderResult:
        resolved_order_id = order_id.strip()
        client_order_id = str((metadata or {}).get("client_order_id") or f"broker-cancel-{resolved_order_id}")
        base_url = self._resolve_base_url(None).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/orders/{resolved_order_id}",
            method="DELETE",
            headers=self._headers(client_order_id=client_order_id, credential_context=None),
            operation="cancel_order",
            client_order_id=client_order_id,
        )
        raw_payload = payload if isinstance(payload, dict) else {}
        return BrokerOrderResult(
            ok=True,
            broker="alpaca",
            account_id=account_id,
            order_id=resolved_order_id,
            client_order_id=client_order_id,
            status=_maybe_string(raw_payload.get("status")) or "canceled",
            side=_maybe_string(raw_payload.get("side")),
            notional=_maybe_float(raw_payload.get("notional")),
            raw_response=payload,
        )

    def get_account(self, account_id: str) -> BrokerAccountState:
        base_url = self._resolve_base_url(None).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/account",
            method="GET",
            headers=self._headers(client_order_id="broker-account-probe", credential_context=None),
            operation="get_account",
            client_order_id="broker-account-probe",
        )
        return _broker_account_state_from_alpaca_payload(payload=payload if isinstance(payload, dict) else {}, account_id=account_id)

    def get_positions(self, account_id: str) -> list[BrokerPositionState]:
        base_url = self._resolve_base_url(None).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/positions",
            method="GET",
            headers=self._headers(client_order_id="broker-positions-probe", credential_context=None),
            operation="get_positions",
            client_order_id="broker-positions-probe",
        )
        if not isinstance(payload, list):
            return []
        return [
            _broker_position_state_from_alpaca_payload(
                payload=item,
                symbol=_maybe_string(item.get("symbol")) or "UNKNOWN",
                account_id=account_id,
            )
            for item in payload
            if isinstance(item, dict)
        ]

    def get_position(self, account_id: str, symbol: str) -> BrokerPositionState | None:
        resolved_symbol = symbol.strip().upper()
        base_url = self._resolve_base_url(None).rstrip("/")
        try:
            payload = self._request_json(
                url=f"{base_url}/v2/positions/{resolved_symbol}",
                method="GET",
                headers=self._headers(client_order_id="broker-position-probe", credential_context=None),
                operation="get_position",
                symbol=resolved_symbol,
                client_order_id="broker-position-probe",
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status == 404:
                return None
            raise
        return _broker_position_state_from_alpaca_payload(payload=payload if isinstance(payload, dict) else {}, symbol=resolved_symbol, account_id=account_id)

    def get_asset(self, symbol: str) -> BrokerAssetState | None:
        resolved_symbol = symbol.strip().upper()
        base_url = self._resolve_base_url(None).rstrip("/")
        try:
            payload = self._request_json(
                url=f"{base_url}/v2/assets/{resolved_symbol}",
                method="GET",
                headers=self._headers(client_order_id="broker-asset-probe", credential_context=None),
                operation="get_asset",
                symbol=resolved_symbol,
                client_order_id="broker-asset-probe",
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status == 404:
                return None
            raise
        return _broker_asset_state_from_alpaca_payload(payload=payload if isinstance(payload, dict) else {}, symbol=resolved_symbol)

    def health_check(self, account_id: str | None = None) -> dict[str, object]:
        try:
            account = self.get_account(account_id or "default")
        except Exception as exc:
            return {
                "ok": False,
                "broker": "alpaca",
                "account_id": account_id,
                "status": "unhealthy",
                "error": str(exc),
            }
        return {
            "ok": True,
            "broker": "alpaca",
            "account_id": account.account_id,
            "status": account.status or "available",
        }

    def cancel_order(
        self,
        account_id: str | None = None,
        order_id: str | None = None,
        metadata: dict[str, object] | None = None,
        *,
        client_order_id: str | None = None,
        action: str | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult | BrokerOrderResult:
        if account_id is not None and action is None and client_order_id is None and credential_context is None:
            if order_id is None:
                raise ValueError("Broker cancel_order requires an order_id.")
            return self.broker_cancel_order(account_id=account_id, order_id=order_id, metadata=metadata)
        if order_id is None or action is None:
            raise ValueError("Runtime cancel_order requires order_id and action.")
        resolved_order_id = order_id.strip()
        if resolved_order_id == "":
            raise ValueError("Cancel order requires a non-empty order_id.")
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/orders/{resolved_order_id}",
            method="DELETE",
            headers=self._headers(
                client_order_id=client_order_id or f"execution-cancel-{resolved_order_id}",
                credential_context=credential_context,
            ),
            operation="cancel_order_runtime",
            client_order_id=client_order_id or f"execution-cancel-{resolved_order_id}",
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "canceled")),
            order_id=resolved_order_id,
            client_order_id=client_order_id,
            side=_maybe_string(payload.get("side")),
            notional=_maybe_float(payload.get("notional")),
            raw_response={
                "cancel_status": str(payload.get("status", "canceled")),
                **(payload if isinstance(payload, dict) else {}),
            },
        )

    def _submit_notional_buy(
        self,
        *,
        symbol: str,
        notional: float,
        client_order_id: str,
        action: str,
        add_tier: int | None = None,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        if notional <= 0:
            raise ValueError("Market order notional must be greater than zero.")
        request_model = BrokerOrderRequest(
            account_id=None if credential_context is None else credential_context.alpaca_account_id,
            symbol=symbol,
            side="buy",
            order_type="market",
            time_in_force=_resolve_market_order_time_in_force(symbol),
            notional=round(notional, 2),
            client_order_id=client_order_id,
            execution_mode=None if credential_context is None else credential_context.environment,
            metadata={"action": action, "add_tier": add_tier, "credential_context": credential_context},
        )
        return _paper_execution_result_from_broker_order_result(
            self.submit_order(request_model),
            action=action,
            add_tier=add_tier,
        )

    def _headers(
        self,
        *,
        client_order_id: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> dict[str, str]:
        del client_order_id
        key_id = credential_context.key_id if credential_context is not None else self._settings.alpaca_api_key_id
        secret = credential_context.secret if credential_context is not None else self._settings.alpaca_api_secret
        if not key_id or not secret:
            raise RuntimeError("Alpaca credentials are required for Prime Stocks execution.")
        return {
            "Accept": "application/json",
            "APCA-API-KEY-ID": key_id,
            "APCA-API-SECRET-KEY": secret,
        }

    def _request_json(
        self,
        *,
        url: str,
        method: str,
        headers: dict[str, str],
        payload: dict[str, object] | None = None,
        operation: str = "alpaca_rest",
        symbol: str | None = None,
        side: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, object] | list[object]:
        policy = self._retry_policy
        max_attempts = max(1, int(policy.max_attempts))
        latest_error: AlpacaPaperTradingError | None = None
        for attempt in range(1, max_attempts + 1):
            started = time.monotonic()
            try:
                response = self._http_client.request_json(url=url, method=method, headers=headers, payload=payload)
                self._log_broker_request(
                    operation=operation,
                    symbol=symbol,
                    side=side,
                    client_order_id=client_order_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    retryable=False,
                    status_code=None,
                    error_category=None,
                    elapsed_ms=_elapsed_ms(started),
                )
                return response
            except AlpacaPaperTradingError as exc:
                latest_error = exc
            except HTTPError as exc:
                latest_error = _normalize_alpaca_http_error(exc)
            except (TimeoutError, SocketTimeout) as exc:
                latest_error = AlpacaPaperTradingError(
                    code="broker_api_timeout",
                    message="Alpaca request timed out during broker execution.",
                    retryable=True,
                    error_category="retryable_timeout",
                )
            except URLError as exc:
                latest_error = AlpacaPaperTradingError(
                    code="broker_network_error",
                    message="Alpaca request could not be reached during broker execution.",
                    retryable=True,
                    error_category="retryable_network_error",
                )
            except json.JSONDecodeError as exc:
                latest_error = AlpacaPaperTradingError(
                    code="broker_parse_error",
                    message="Alpaca returned invalid JSON during broker execution.",
                    retryable=False,
                    error_category="parse_error",
                )
            except ValueError as exc:
                # JSON clients commonly raise ValueError for malformed JSON; keep it non-retryable.
                latest_error = AlpacaPaperTradingError(
                    code="broker_parse_error",
                    message="Alpaca returned invalid JSON during broker execution.",
                    retryable=False,
                    error_category="parse_error",
                )
            elapsed_ms = _elapsed_ms(started)
            latest_error.attempt_count = attempt
            latest_error.retry_count = max(0, attempt - 1)
            should_retry = latest_error.retryable and attempt < max_attempts
            self._log_broker_request(
                operation=operation,
                symbol=symbol,
                side=side,
                client_order_id=client_order_id,
                attempt=attempt,
                max_attempts=max_attempts,
                retryable=should_retry,
                status_code=latest_error.http_status,
                error_category=latest_error.error_category,
                elapsed_ms=elapsed_ms,
            )
            if not should_retry:
                raise latest_error
            self._sleeper(_resolve_retry_delay(policy=policy, attempt=attempt, retry_after=latest_error.retry_after))
        if latest_error is not None:
            latest_error.attempt_count = max_attempts
            latest_error.retry_count = max(0, max_attempts - 1)
            raise latest_error
        raise AlpacaPaperTradingError(
            code="broker_unknown_error",
            message="Alpaca request failed before any response was available.",
            retryable=False,
            error_category="unknown_error",
        )

    @staticmethod
    def _log_broker_request(
        *,
        operation: str,
        symbol: str | None,
        side: str | None,
        client_order_id: str | None,
        attempt: int,
        max_attempts: int,
        retryable: bool,
        status_code: int | None,
        error_category: str | None,
        elapsed_ms: int,
    ) -> None:
        if error_category is None:
            logger.info(
                "Alpaca broker request completed broker=alpaca operation=%s symbol=%s side=%s client_order_id=%s attempt=%s max_attempts=%s elapsed_ms=%s",
                operation,
                symbol,
                side,
                client_order_id,
                attempt,
                max_attempts,
                elapsed_ms,
            )
            return
        logger.warning(
            "Alpaca broker request failed broker=alpaca operation=%s symbol=%s side=%s client_order_id=%s attempt=%s max_attempts=%s retryable=%s error_category=%s status_code=%s elapsed_ms=%s",
            operation,
            symbol,
            side,
            client_order_id,
            attempt,
            max_attempts,
            retryable,
            error_category,
            status_code,
            elapsed_ms,
        )

    def _resolve_base_url(self, credential_context: ResolvedAlpacaAccountContext | None) -> str:
        if credential_context is not None and credential_context.environment == "live":
            return self._settings.alpaca_live_trading_base_url
        return self._settings.alpaca_trading_base_url

    @staticmethod
    def _ensure_tradable_context(
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        credential_context: ResolvedAlpacaAccountContext | None,
    ) -> None:
        normalized_asset_type = (asset_type or "").strip().lower()
        normalized_product_key = (product_key or "").strip().lower()
        if normalized_product_key not in SUPPORTED_PRODUCT_KEYS:
            raise ValueError(f"Prime Stocks paper execution only supports product_key='stocks.bismel1'. Received {product_key!r}.")
        if normalized_asset_type in SUPPORTED_STOCK_ASSET_TYPES:
            return
        normalized_symbol = (symbol or "").strip().upper()
        uid = (credential_context.uid if credential_context is not None else "").strip()
        if (
            normalized_asset_type in SUPPORTED_CRYPTO_ASSET_TYPES
            and uid in SUPPORTED_ADMIN_CRYPTO_MONITOR_UIDS
            and normalized_symbol in SUPPORTED_ADMIN_CRYPTO_MONITOR_SYMBOLS
        ):
            return
        raise ValueError(f"Prime Stocks paper execution accepts stock/equity symbols only outside admin monitors. Received asset_type={asset_type!r}.")


def _maybe_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _alpaca_order_payload_from_broker_request(request: BrokerOrderRequest) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": request.symbol,
        "side": request.side,
        "type": request.order_type,
        "time_in_force": request.time_in_force,
        "client_order_id": request.client_order_id,
    }
    if request.qty is not None:
        payload["qty"] = _format_qty(request.qty)
    if request.notional is not None:
        payload["notional"] = round(request.notional, 2)
    if request.limit_price is not None:
        payload["limit_price"] = request.limit_price
    if request.stop_price is not None:
        payload["stop_price"] = request.stop_price
    return payload


def _resolve_market_order_time_in_force(symbol: str, requested: str = "day") -> str:
    normalized_requested = (requested or "day").strip().lower()
    normalized_symbol = (symbol or "").strip().upper()
    if _looks_like_crypto_pair(normalized_symbol) and normalized_requested == "day":
        return "gtc"
    return normalized_requested


def _looks_like_crypto_pair(symbol: str) -> bool:
    return "/" in (symbol or "").strip()


def _broker_order_result_from_alpaca_payload(
    *,
    payload: dict[str, object] | list[object],
    request: BrokerOrderRequest,
) -> BrokerOrderResult:
    raw_payload = payload if isinstance(payload, dict) else {}
    order_id = _maybe_string(raw_payload.get("id"))
    return BrokerOrderResult(
        ok=True,
        broker="alpaca",
        account_id=request.account_id,
        symbol=_maybe_string(raw_payload.get("symbol")) or request.symbol,
        order_id=order_id,
        broker_order_id=order_id,
        client_order_id=_maybe_string(raw_payload.get("client_order_id")) or request.client_order_id,
        status=_maybe_string(raw_payload.get("status")) or "submitted",
        side=_maybe_string(raw_payload.get("side")) or request.side,
        qty=_maybe_float(raw_payload.get("qty")) if raw_payload.get("qty") is not None else request.qty,
        notional=_maybe_float(raw_payload.get("notional")) if raw_payload.get("notional") is not None else request.notional,
        requested_qty=request.qty,
        requested_notional=request.notional,
        filled_qty=_maybe_float(raw_payload.get("filled_qty")),
        filled_avg_price=_maybe_float(raw_payload.get("filled_avg_price")),
        raw_response=payload,
    )


def _broker_order_result_from_alpaca_error(
    *,
    request: BrokerOrderRequest,
    error: AlpacaPaperTradingError,
) -> BrokerOrderResult:
    return BrokerOrderResult(
        ok=False,
        broker="alpaca",
        account_id=request.account_id,
        symbol=request.symbol,
        client_order_id=request.client_order_id,
        status="rejected",
        side=request.side,
        qty=request.qty,
        notional=request.notional,
        requested_qty=request.qty,
        requested_notional=request.notional,
        raw_response=error.raw_response,
        error_code=error.code,
        error_category=error.error_category,
        error_message=error.message,
        error_message_safe=error.message,
        retryable=error.retryable,
        attempt_count=error.attempt_count,
        broker_status_code=error.http_status,
    )


def _paper_execution_result_from_broker_order_result(
    result: BrokerOrderResult,
    *,
    action: str,
    add_tier: int | None = None,
) -> AlpacaPaperExecutionResult:
    return AlpacaPaperExecutionResult(
        action=action,
        submitted=result.ok,
        order_status=str(result.status or "submitted"),
        order_id=result.order_id,
        client_order_id=result.client_order_id,
        side=result.side,
        notional=result.notional,
        add_tier=add_tier,
        skipped_reason=None if result.ok else result.error_code,
        retry_count=max(0, result.attempt_count - 1),
        raw_response=result.raw_response if isinstance(result.raw_response, dict) else None,
        broker_error_code=result.error_code,
        broker_error_message=result.error_message_safe or result.error_message,
    )


def _broker_account_state_from_alpaca_payload(
    *,
    payload: dict[str, object],
    account_id: str | int | None,
) -> BrokerAccountState:
    return BrokerAccountState(
        broker="alpaca",
        account_id=account_id,
        status=_maybe_string(payload.get("status")),
        currency=_maybe_string(payload.get("currency")),
        equity=_maybe_float(payload.get("equity")),
        cash=_maybe_float(payload.get("cash")),
        buying_power=_maybe_float(payload.get("buying_power")),
        portfolio_value=_maybe_float(payload.get("portfolio_value")),
        raw_response=payload,
    )


def _broker_asset_state_from_alpaca_payload(*, payload: dict[str, object], symbol: str) -> BrokerAssetState:
    return BrokerAssetState(
        broker="alpaca",
        symbol=_maybe_string(payload.get("symbol")) or symbol,
        asset_class=_maybe_string(payload.get("class")) or _maybe_string(payload.get("asset_class")) or "stock",
        tradable=bool(payload.get("tradable", False)),
        marginable=_maybe_bool(payload.get("marginable")),
        fractionable=_maybe_bool(payload.get("fractionable")),
        shortable=_maybe_bool(payload.get("shortable")),
        status=_maybe_string(payload.get("status")),
        raw_response=payload,
    )


def _broker_position_state_from_alpaca_payload(
    *,
    payload: dict[str, object],
    symbol: str,
    account_id: str | int | None,
) -> BrokerPositionState:
    return BrokerPositionState(
        broker="alpaca",
        account_id=account_id,
        symbol=_maybe_string(payload.get("symbol")) or symbol,
        asset_class=_maybe_string(payload.get("asset_class")),
        side=_maybe_string(payload.get("side")),
        qty=_maybe_float(payload.get("qty")) or 0.0,
        avg_entry_price=_maybe_float(payload.get("avg_entry_price")),
        current_price=_maybe_float(payload.get("current_price")),
        market_value=_maybe_float(payload.get("market_value")),
        unrealized_pl=_maybe_float(payload.get("unrealized_pl")),
        unrealized_plpc=_maybe_float(payload.get("unrealized_plpc")),
        raw_response=payload,
    )


def _submission_state_from_broker_models(
    *,
    account: BrokerAccountState,
    asset: BrokerAssetState,
    position: BrokerPositionState | None,
    open_positions_count: int,
    total_exposure: float | None,
) -> AlpacaPaperSubmissionState:
    return AlpacaPaperSubmissionState(
        account=AlpacaPaperAccountState(
            buying_power=account.buying_power,
            open_positions_count=open_positions_count,
            equity=account.equity,
            total_exposure=total_exposure,
        ),
        asset=AlpacaPaperAssetState(
            symbol=asset.symbol,
            tradable=asset.tradable,
            status=asset.status,
        ),
        position=None
        if position is None
        else AlpacaPaperPositionState(
            symbol=position.symbol,
            qty=position.qty,
            market_value=position.market_value,
            avg_entry_price=position.avg_entry_price,
        ),
    )


def _maybe_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _maybe_bool(value: object) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


def _resolve_retry_delay(*, policy: AlpacaBrokerRetryPolicy, attempt: int, retry_after: float | None) -> float:
    if retry_after is not None and retry_after >= 0:
        return min(float(retry_after), policy.max_delay_seconds)
    delay = policy.base_delay_seconds * (2 ** max(0, attempt - 1))
    if policy.jitter_seconds > 0:
        delay += random.uniform(0, policy.jitter_seconds)
    return min(delay, policy.max_delay_seconds)


def _format_qty(value: float) -> str:
    return format(float(value), ".9f").rstrip("0").rstrip(".")


def _sum_position_market_value(payload: dict[str, object] | list[object]) -> float | None:
    if not isinstance(payload, list):
        return None
    total = 0.0
    for item in payload:
        if not isinstance(item, dict):
            continue
        market_value = _maybe_float(item.get("market_value"))
        if market_value is None:
            continue
        total += abs(market_value)
    return total


def _normalize_alpaca_http_error(exc: HTTPError) -> AlpacaPaperTradingError:
    raw_text = ""
    raw_payload: dict[str, object] | None = None
    try:
        raw_text = exc.read().decode("utf-8")
        raw_payload = json.loads(raw_text) if raw_text else None
    except Exception:
        raw_payload = None
    message = _extract_alpaca_error_message(raw_payload) or raw_text or f"Alpaca HTTP {exc.code}"
    normalized_message = message.strip()
    lowered = normalized_message.lower()
    retry_after = _extract_retry_after(exc.headers)
    if exc.code == 429:
        return AlpacaPaperTradingError(
            code="broker_rate_limited",
            message=normalized_message,
            retryable=True,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="retryable_rate_limit",
            retry_after=retry_after,
        )
    if exc.code in {500, 502, 503, 504}:
        return AlpacaPaperTradingError(
            code="broker_api_error",
            message=normalized_message,
            retryable=True,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="retryable_server_error",
            retry_after=retry_after,
        )
    if exc.code == 401:
        return AlpacaPaperTradingError(
            code="broker_auth_error",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="auth_error",
        )
    if exc.code == 403:
        category = "account_restricted" if "account" in lowered or "restricted" in lowered else "permission_error"
        return AlpacaPaperTradingError(
            code=f"broker_{category}",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category=category,
        )
    if "insufficient buying power" in lowered:
        return AlpacaPaperTradingError(
            code="broker_insufficient_buying_power",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="insufficient_buying_power",
        )
    if "insufficient" in lowered and "position" in lowered:
        return AlpacaPaperTradingError(
            code="broker_insufficient_position",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="insufficient_position",
        )
    if "not tradable" in lowered:
        return AlpacaPaperTradingError(
            code="broker_asset_not_tradable",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="asset_not_tradable",
        )
    if "market is closed" in lowered or "market closed" in lowered:
        return AlpacaPaperTradingError(
            code="broker_market_closed",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="market_closed",
        )
    if "duplicate" in lowered and "client" in lowered:
        return AlpacaPaperTradingError(
            code="broker_duplicate_client_order_id",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="duplicate_client_order_id",
        )
    if "invalid" in lowered and "order" in lowered:
        return AlpacaPaperTradingError(
            code="broker_invalid_order",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="invalid_order_request",
        )
    if exc.code in NON_RETRYABLE_HTTP_STATUSES:
        return AlpacaPaperTradingError(
            code="broker_rejected",
            message=normalized_message,
            retryable=False,
            http_status=exc.code,
            raw_response=raw_payload,
            error_category="rejected_non_retryable",
        )
    return AlpacaPaperTradingError(
        code="broker_rejected",
        message=normalized_message,
        retryable=exc.code in RETRYABLE_HTTP_STATUSES,
        http_status=exc.code,
        raw_response=raw_payload,
        error_category="unknown_error",
    )


def _extract_alpaca_error_message(payload: dict[str, object] | None) -> str | None:
    if not payload:
        return None
    for key in ("message", "error", "detail"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    return None


def _extract_retry_after(headers: Message | None) -> float | None:
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
