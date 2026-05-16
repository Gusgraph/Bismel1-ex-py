# اعوز بالله من الشياطين و ان يحضرون بسم الله الرحمن الرحيم الله لا إله إلا هو الحي القيوم
# Bismillahi ar-Rahmani ar-Rahim Audhu billahi min ash-shayatin wa an yahdurun Bismillah ar-Rahman ar-Rahim Allah la ilaha illa huwa al-hayy al-qayyum. Tamsa Allahu ala ayunihim
# version: 1
# ======================================================
# - App Name: Bismel1-ex-py
# - Gusgraph LLC -
# - Author: Gus Kazem
# - https://Gusgraph.com
# - File Path: app/brokers/alpaca_sdk_trading.py
# ======================================================

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from socket import timeout as SocketTimeout
from typing import Any, Callable, Protocol
from urllib.error import URLError

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderType, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import ClosePositionRequest, GetOrdersRequest, MarketOrderRequest

from app.brokers.alpaca_paper_trading import (
    AlpacaBrokerRetryPolicy,
    AlpacaPaperAccountState,
    AlpacaPaperAssetState,
    AlpacaPaperExecutionResult,
    AlpacaPaperPositionState,
    AlpacaPaperSubmissionState,
    AlpacaPaperTradingError,
    SUPPORTED_ADMIN_CRYPTO_MONITOR_SYMBOLS,
    SUPPORTED_ADMIN_CRYPTO_MONITOR_UIDS,
    SUPPORTED_CRYPTO_ASSET_TYPES,
    SUPPORTED_PRODUCT_KEYS,
    SUPPORTED_STOCK_ASSET_TYPES,
    _broker_account_state_from_alpaca_payload,
    _broker_asset_state_from_alpaca_payload,
    _broker_order_result_from_alpaca_error,
    _broker_order_result_from_alpaca_payload,
    _broker_position_state_from_alpaca_payload,
    _elapsed_ms,
    _format_qty,
    _maybe_float,
    _maybe_string,
    _paper_execution_result_from_broker_order_result,
    _resolve_retry_delay,
    _submission_state_from_broker_models,
    _sum_position_market_value,
)
from app.brokers.models import (
    BrokerAccountState,
    BrokerAssetState,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPositionState,
)
from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


logger = logging.getLogger(__name__)


class AlpacaTradingClientProtocol(Protocol):
    def submit_order(self, order_data: MarketOrderRequest) -> Any:
        ...

    def close_position(self, symbol_or_asset_id: str, close_options: ClosePositionRequest | None = None) -> Any:
        ...

    def cancel_order_by_id(self, order_id: str) -> Any:
        ...

    def get_account(self) -> Any:
        ...

    def get_all_positions(self) -> Any:
        ...

    def get_open_position(self, symbol_or_asset_id: str) -> Any:
        ...

    def get_asset(self, symbol_or_asset_id: str) -> Any:
        ...

    def get_orders(self, filter: GetOrdersRequest | None = None) -> Any:
        ...


class AlpacaSdkClientFactory(Protocol):
    def __call__(self, *, key_id: str, secret: str, paper: bool, url_override: str | None) -> AlpacaTradingClientProtocol:
        ...


def _default_sdk_client_factory(
    *,
    key_id: str,
    secret: str,
    paper: bool,
    url_override: str | None,
) -> AlpacaTradingClientProtocol:
    return TradingClient(
        api_key=key_id,
        secret_key=secret,
        paper=paper,
        raw_data=False,
        url_override=url_override,
    )


@dataclass(frozen=True)
class _ResolvedSdkContext:
    client: AlpacaTradingClientProtocol
    account_id: str | int | None
    environment: str | None


class AlpacaSdkBrokerAdapter:
    """Alpaca SDK transport behind the existing Bismel1 broker contract."""

    def __init__(
        self,
        settings: AppConfig,
        client_factory: AlpacaSdkClientFactory | None = None,
        retry_policy: AlpacaBrokerRetryPolicy | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or _default_sdk_client_factory
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
            time_in_force="day",
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
            time_in_force="day",
            notional=round(notional, 2),
            client_order_id=client_order_id,
            execution_mode=None if credential_context is None else credential_context.environment,
            metadata={"action": action, "credential_context": credential_context},
        )
        return _paper_execution_result_from_broker_order_result(
            self.submit_order(request_model),
            action=action,
        )

    def submit_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        sdk_context = self._resolve_sdk_context(request.metadata.get("credential_context") if isinstance(request.metadata, dict) else None)
        try:
            payload = self._sdk_call(
                lambda: sdk_context.client.submit_order(order_data=_market_order_request_from_broker_request(request)),
                operation="submit_order",
                symbol=request.symbol,
                side=request.side,
                client_order_id=request.client_order_id,
            )
        except AlpacaPaperTradingError as exc:
            return _broker_order_result_from_alpaca_error(request=request, error=exc)
        return _broker_order_result_from_alpaca_payload(payload=_sdk_payload(payload), request=request)

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
        result = self.broker_close_position(
            account_id=str(credential_context.alpaca_account_id) if credential_context is not None else "default",
            symbol=symbol,
            metadata={"client_order_id": client_order_id, "credential_context": credential_context},
        )
        return _paper_execution_result_from_broker_order_result(result, action=action)

    def broker_close_position(self, account_id: str, symbol: str, metadata: dict[str, object] | None = None) -> BrokerOrderResult:
        resolved_symbol = symbol.strip().upper()
        client_order_id = str((metadata or {}).get("client_order_id") or f"broker-close-{resolved_symbol.lower()}")
        sdk_context = self._resolve_sdk_context((metadata or {}).get("credential_context"))
        try:
            payload = self._sdk_call(
                lambda: sdk_context.client.close_position(resolved_symbol, close_options=None),
                operation="close_position",
                symbol=resolved_symbol,
                side="sell",
                client_order_id=client_order_id,
            )
        except AlpacaPaperTradingError as exc:
            request = BrokerOrderRequest(
                account_id=account_id,
                symbol=resolved_symbol,
                side="sell",
                order_type="market",
                time_in_force="day",
                qty=1,
                client_order_id=client_order_id,
                metadata=metadata or {},
            )
            return _broker_order_result_from_alpaca_error(request=request, error=exc)
        raw_payload = _sdk_payload(payload)
        return BrokerOrderResult(
            ok=True,
            broker="alpaca",
            account_id=account_id,
            symbol=_maybe_string(raw_payload.get("symbol")) or resolved_symbol,
            order_id=_maybe_string(raw_payload.get("id")),
            broker_order_id=_maybe_string(raw_payload.get("id")),
            client_order_id=_maybe_string(raw_payload.get("client_order_id")) or client_order_id,
            status=_maybe_string(raw_payload.get("status")) or "submitted",
            side=_maybe_string(raw_payload.get("side")) or "sell",
            notional=_maybe_float(raw_payload.get("notional")),
            raw_response=raw_payload,
        )

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
        result = self.broker_cancel_order(
            account_id=str(credential_context.alpaca_account_id) if credential_context is not None else "default",
            order_id=order_id,
            metadata={"client_order_id": client_order_id, "credential_context": credential_context},
        )
        return _paper_execution_result_from_broker_order_result(result, action=action)

    def broker_cancel_order(self, account_id: str, order_id: str, metadata: dict[str, object] | None = None) -> BrokerOrderResult:
        resolved_order_id = order_id.strip()
        client_order_id = str((metadata or {}).get("client_order_id") or f"broker-cancel-{resolved_order_id}")
        sdk_context = self._resolve_sdk_context((metadata or {}).get("credential_context"))
        try:
            payload = self._sdk_call(
                lambda: sdk_context.client.cancel_order_by_id(resolved_order_id),
                operation="cancel_order",
                client_order_id=client_order_id,
            )
        except AlpacaPaperTradingError as exc:
            request = BrokerOrderRequest(
                account_id=account_id,
                symbol="UNKNOWN",
                side="sell",
                order_type="market",
                time_in_force="day",
                qty=1,
                client_order_id=client_order_id,
                metadata=metadata or {},
            )
            return _broker_order_result_from_alpaca_error(request=request, error=exc)
        raw_payload = _sdk_payload(payload)
        return BrokerOrderResult(
            ok=True,
            broker="alpaca",
            account_id=account_id,
            order_id=resolved_order_id,
            broker_order_id=resolved_order_id,
            client_order_id=client_order_id,
            status=_maybe_string(raw_payload.get("status")) or "canceled",
            side=_maybe_string(raw_payload.get("side")),
            notional=_maybe_float(raw_payload.get("notional")),
            raw_response=raw_payload,
        )

    def get_account(self, account_id: str) -> BrokerAccountState:
        sdk_context = self._resolve_sdk_context(None)
        payload = self._sdk_call(
            sdk_context.client.get_account,
            operation="get_account",
            client_order_id="broker-account-probe",
        )
        return _broker_account_state_from_alpaca_payload(payload=_sdk_payload(payload), account_id=account_id)

    def get_positions(self, account_id: str) -> list[BrokerPositionState]:
        sdk_context = self._resolve_sdk_context(None)
        payload = self._sdk_call(
            sdk_context.client.get_all_positions,
            operation="get_positions",
            client_order_id="broker-positions-probe",
        )
        raw_payload = _sdk_payload(payload)
        if not isinstance(raw_payload, list):
            return []
        return [
            _broker_position_state_from_alpaca_payload(
                payload=item,
                symbol=_maybe_string(item.get("symbol")) or "UNKNOWN",
                account_id=account_id,
            )
            for item in raw_payload
            if isinstance(item, dict)
        ]

    def get_position(self, account_id: str, symbol: str) -> BrokerPositionState | None:
        resolved_symbol = symbol.strip().upper()
        sdk_context = self._resolve_sdk_context(None)
        try:
            payload = self._sdk_call(
                lambda: sdk_context.client.get_open_position(resolved_symbol),
                operation="get_position",
                symbol=resolved_symbol,
                client_order_id="broker-position-probe",
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status == 404:
                return None
            raise
        return _broker_position_state_from_alpaca_payload(payload=_sdk_payload(payload), symbol=resolved_symbol, account_id=account_id)

    def get_asset(self, symbol: str) -> BrokerAssetState | None:
        resolved_symbol = symbol.strip().upper()
        sdk_context = self._resolve_sdk_context(None)
        try:
            payload = self._sdk_call(
                lambda: sdk_context.client.get_asset(resolved_symbol),
                operation="get_asset",
                symbol=resolved_symbol,
                client_order_id="broker-asset-probe",
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status == 404:
                return None
            raise
        return _broker_asset_state_from_alpaca_payload(payload=_sdk_payload(payload), symbol=resolved_symbol)

    def get_submission_state(
        self,
        *,
        symbol: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperSubmissionState:
        sdk_context = self._resolve_sdk_context(credential_context)
        account_payload = self._sdk_call(
            sdk_context.client.get_account,
            operation="get_submission_state_account",
            symbol=symbol.upper(),
            client_order_id="prime-state-probe",
        )
        positions_payload = self._sdk_call(
            sdk_context.client.get_all_positions,
            operation="get_submission_state_positions",
            symbol=symbol.upper(),
            client_order_id="prime-state-probe",
        )
        asset_payload = self._sdk_call(
            lambda: sdk_context.client.get_asset(symbol.upper()),
            operation="get_submission_state_asset",
            symbol=symbol.upper(),
            client_order_id="prime-state-probe",
        )
        position_payload = None
        try:
            position_payload = self._sdk_call(
                lambda: sdk_context.client.get_open_position(symbol.upper()),
                operation="get_submission_state_position",
                symbol=symbol.upper(),
                client_order_id="prime-state-probe",
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status != 404:
                raise
        raw_positions = _sdk_payload(positions_payload)
        broker_account = _broker_account_state_from_alpaca_payload(
            payload=_sdk_payload(account_payload),
            account_id=sdk_context.account_id,
        )
        broker_asset = _broker_asset_state_from_alpaca_payload(payload=_sdk_payload(asset_payload), symbol=symbol)
        broker_position = None if position_payload is None else _broker_position_state_from_alpaca_payload(
            payload=_sdk_payload(position_payload),
            symbol=symbol,
            account_id=sdk_context.account_id,
        )
        return _submission_state_from_broker_models(
            account=broker_account,
            asset=broker_asset,
            position=broker_position,
            open_positions_count=len(raw_positions) if isinstance(raw_positions, list) else 0,
            total_exposure=_sum_position_market_value(raw_positions),
        )

    def list_recent_orders(
        self,
        *,
        credential_context: ResolvedAlpacaAccountContext | None = None,
        limit: int = 50,
    ) -> list[dict[str, object]]:
        sdk_context = self._resolve_sdk_context(credential_context)
        payload = self._sdk_call(
            lambda: sdk_context.client.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.ALL, limit=max(1, int(limit)))),
            operation="list_recent_orders",
            client_order_id="prime-orders-probe",
        )
        raw_payload = _sdk_payload(payload)
        return raw_payload if isinstance(raw_payload, list) else []

    def health_check(self, account_id: str | None = None) -> dict[str, object]:
        try:
            account = self.get_account(account_id or "default")
        except Exception as exc:
            return {
                "ok": False,
                "broker": "alpaca",
                "transport": "sdk",
                "account_id": account_id,
                "status": "unhealthy",
                "error": str(exc),
            }
        return {
            "ok": True,
            "broker": "alpaca",
            "transport": "sdk",
            "account_id": account.account_id,
            "status": account.status or "available",
        }

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
            time_in_force="day",
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

    def _resolve_sdk_context(self, credential_context: object | None) -> _ResolvedSdkContext:
        resolved_context = credential_context if isinstance(credential_context, ResolvedAlpacaAccountContext) else None
        key_id = resolved_context.key_id if resolved_context is not None else self._settings.alpaca_api_key_id
        secret = resolved_context.secret if resolved_context is not None else self._settings.alpaca_api_secret
        if not key_id or not secret:
            raise RuntimeError("Alpaca credentials are required for SDK broker execution.")
        environment = resolved_context.environment if resolved_context is not None else ("paper" if self._settings.alpaca_trading_base_url else None)
        paper = environment != "live"
        client = self._client_factory(
            key_id=key_id,
            secret=secret,
            paper=paper,
            url_override=self._resolve_url_override(resolved_context),
        )
        return _ResolvedSdkContext(
            client=client,
            account_id=None if resolved_context is None else resolved_context.alpaca_account_id,
            environment=environment,
        )

    def _resolve_url_override(self, credential_context: ResolvedAlpacaAccountContext | None) -> str | None:
        base_url = self._settings.alpaca_live_trading_base_url if credential_context is not None and credential_context.environment == "live" else self._settings.alpaca_trading_base_url
        if base_url in {"https://paper-api.alpaca.markets", "https://api.alpaca.markets"}:
            return None
        return base_url

    def _sdk_call(
        self,
        fn: Callable[[], Any],
        *,
        operation: str,
        symbol: str | None = None,
        side: str | None = None,
        client_order_id: str | None = None,
    ) -> Any:
        policy = self._retry_policy
        max_attempts = max(1, int(policy.max_attempts))
        latest_error: AlpacaPaperTradingError | None = None
        for attempt in range(1, max_attempts + 1):
            started = time.monotonic()
            try:
                response = fn()
                _log_sdk_request(
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
            except APIError as exc:
                latest_error = _normalize_sdk_api_error(exc)
            except (TimeoutError, SocketTimeout) as exc:
                latest_error = AlpacaPaperTradingError(
                    code="broker_api_timeout",
                    message="Alpaca SDK request timed out during broker execution.",
                    retryable=True,
                    error_category="retryable_timeout",
                )
            except URLError as exc:
                latest_error = AlpacaPaperTradingError(
                    code="broker_network_error",
                    message="Alpaca SDK request could not be reached during broker execution.",
                    retryable=True,
                    error_category="retryable_network_error",
                )
            except (json.JSONDecodeError, ValueError) as exc:
                latest_error = AlpacaPaperTradingError(
                    code="broker_parse_error",
                    message="Alpaca SDK returned an unparseable broker response.",
                    retryable=False,
                    error_category="sdk_parse_error",
                )
            elapsed_ms = _elapsed_ms(started)
            latest_error.attempt_count = attempt
            latest_error.retry_count = max(0, attempt - 1)
            should_retry = latest_error.retryable and attempt < max_attempts
            _log_sdk_request(
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
            message="Alpaca SDK request failed before any response was available.",
            retryable=False,
            error_category="unknown_error",
        )

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


def _market_order_request_from_broker_request(request: BrokerOrderRequest) -> MarketOrderRequest:
    if request.order_type != "market":
        raise ValueError("Alpaca SDK adapter currently supports normalized market orders only.")
    return MarketOrderRequest(
        symbol=request.symbol,
        qty=request.qty,
        notional=None if request.notional is None else round(request.notional, 2),
        side=OrderSide.BUY if request.side == "buy" else OrderSide.SELL,
        type=OrderType.MARKET,
        time_in_force=TimeInForce.DAY,
        client_order_id=request.client_order_id,
    )


def _sdk_payload(payload: Any) -> dict[str, object] | list[object]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return [_sdk_payload(item) for item in payload]
    model_dump = getattr(payload, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json", exclude_none=True)
    if hasattr(payload, "__dict__"):
        return {
            key: _sdk_scalar(value)
            for key, value in vars(payload).items()
            if not key.startswith("_") and value is not None
        }
    return {}


def _sdk_scalar(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _normalize_sdk_api_error(exc: APIError) -> AlpacaPaperTradingError:
    status_code = _maybe_int(getattr(exc, "status_code", None))
    message = str(getattr(exc, "message", None) or exc)
    lowered = message.lower()
    raw_response = _safe_sdk_error_payload(exc)
    retryable = status_code in {429, 500, 502, 503, 504}
    if status_code == 429:
        return AlpacaPaperTradingError(
            code="broker_rate_limited",
            message=message,
            retryable=True,
            http_status=status_code,
            raw_response=raw_response,
            error_category="retryable_rate_limit",
        )
    if status_code in {500, 502, 503, 504}:
        return AlpacaPaperTradingError(
            code="broker_api_error",
            message=message,
            retryable=True,
            http_status=status_code,
            raw_response=raw_response,
            error_category="retryable_server_error",
        )
    if status_code == 401:
        return AlpacaPaperTradingError(
            code="broker_auth_error",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="auth_error",
        )
    if status_code == 403:
        category = "account_restricted" if "account" in lowered or "restricted" in lowered else "permission_error"
        return AlpacaPaperTradingError(
            code=f"broker_{category}",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category=category,
        )
    if "insufficient buying power" in lowered:
        return AlpacaPaperTradingError(
            code="broker_insufficient_buying_power",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="insufficient_buying_power",
        )
    if "insufficient" in lowered and "position" in lowered:
        return AlpacaPaperTradingError(
            code="broker_insufficient_position",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="insufficient_position",
        )
    if "not tradable" in lowered:
        return AlpacaPaperTradingError(
            code="broker_asset_not_tradable",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="asset_not_tradable",
        )
    if "market is closed" in lowered or "market closed" in lowered:
        return AlpacaPaperTradingError(
            code="broker_market_closed",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="market_closed",
        )
    if "duplicate" in lowered and "client" in lowered:
        return AlpacaPaperTradingError(
            code="broker_duplicate_client_order_id",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="duplicate_client_order_id",
        )
    if "invalid" in lowered and "order" in lowered:
        return AlpacaPaperTradingError(
            code="broker_invalid_order",
            message=message,
            http_status=status_code,
            raw_response=raw_response,
            error_category="invalid_order_request",
        )
    if status_code in {400, 401, 403, 404, 422}:
        return AlpacaPaperTradingError(
            code="broker_rejected",
            message=message,
            retryable=False,
            http_status=status_code,
            raw_response=raw_response,
            error_category="rejected_non_retryable",
        )
    return AlpacaPaperTradingError(
        code="broker_rejected",
        message=message,
        retryable=retryable,
        http_status=status_code,
        raw_response=raw_response,
        error_category="unknown_error",
    )


def _safe_sdk_error_payload(exc: APIError) -> dict[str, object] | None:
    payload: dict[str, object] = {}
    code = getattr(exc, "code", None)
    message = getattr(exc, "message", None)
    status_code = getattr(exc, "status_code", None)
    if code is not None:
        payload["code"] = str(code)
    if message is not None:
        payload["message"] = str(message)
    if status_code is not None:
        payload["status_code"] = status_code
    return payload or None


def _maybe_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _log_sdk_request(
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
            "Alpaca SDK request completed broker=alpaca transport=sdk operation=%s symbol=%s side=%s client_order_id=%s attempt=%s max_attempts=%s elapsed_ms=%s",
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
        "Alpaca SDK request failed broker=alpaca transport=sdk operation=%s symbol=%s side=%s client_order_id=%s attempt=%s max_attempts=%s retryable=%s error_category=%s status_code=%s elapsed_ms=%s",
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

