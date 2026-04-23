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
from dataclasses import dataclass
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.services.alpaca_account_resolver import ResolvedAlpacaAccountContext
from app.shared.config import AppConfig


SUPPORTED_STOCK_ASSET_TYPES = frozenset({"stock", "stocks", "equity", "equities"})
SUPPORTED_PRODUCT_KEYS = frozenset({"stocks.bismel1"})


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
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.http_status = http_status
        self.raw_response = raw_response


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
    ) -> None:
        self._settings = settings
        self._http_client = http_client or UrllibRequestJsonClient()

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
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
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
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
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
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/orders",
            method="POST",
            headers=self._headers(client_order_id=client_order_id, credential_context=credential_context),
            payload={
                "symbol": symbol.upper(),
                "side": normalized_side,
                "type": "market",
                "time_in_force": "day",
                "qty": _format_qty(qty),
                "client_order_id": client_order_id,
            },
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "submitted")),
            order_id=_maybe_string(payload.get("id")),
            client_order_id=_maybe_string(payload.get("client_order_id")) or client_order_id,
            side=normalized_side,
            notional=_maybe_float(payload.get("notional")),
            raw_response=payload,
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
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/orders",
            method="POST",
            headers=self._headers(client_order_id=client_order_id, credential_context=credential_context),
            payload={
                "symbol": symbol.upper(),
                "side": normalized_side,
                "type": "market",
                "time_in_force": "day",
                "notional": round(notional, 2),
                "client_order_id": client_order_id,
            },
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "submitted")),
            order_id=_maybe_string(payload.get("id")),
            client_order_id=_maybe_string(payload.get("client_order_id")) or client_order_id,
            side=normalized_side,
            notional=round(notional, 2),
            raw_response=payload,
        )

    def close_position(
        self,
        *,
        symbol: str,
        asset_type: str,
        product_key: str,
        action: str,
        client_order_id: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
        self._ensure_stock_context(asset_type=asset_type, product_key=product_key)
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
        )
        positions_payload = self._request_json(
            url=f"{base_url}/v2/positions",
            method="GET",
            headers=headers,
        )
        asset_payload = self._request_json(
            url=f"{base_url}/v2/assets/{symbol.upper()}",
            method="GET",
            headers=headers,
        )
        position_payload = None
        try:
            position_payload = self._request_json(
                url=f"{base_url}/v2/positions/{symbol.upper()}",
                method="GET",
                headers=headers,
            )
        except AlpacaPaperTradingError as exc:
            if exc.http_status != 404:
                raise
        return AlpacaPaperSubmissionState(
            account=AlpacaPaperAccountState(
                buying_power=_maybe_float(account_payload.get("buying_power")),
                open_positions_count=len(positions_payload) if isinstance(positions_payload, list) else 0,
                equity=_maybe_float(account_payload.get("equity")),
                total_exposure=_sum_position_market_value(positions_payload),
            ),
            asset=AlpacaPaperAssetState(
                symbol=symbol.upper(),
                tradable=bool(asset_payload.get("tradable", False)),
                status=_maybe_string(asset_payload.get("status")),
            ),
            position=None
            if not position_payload
            else AlpacaPaperPositionState(
                symbol=symbol.upper(),
                qty=_maybe_float(position_payload.get("qty")) or 0.0,
                market_value=_maybe_float(position_payload.get("market_value")),
            ),
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
        )
        return payload if isinstance(payload, list) else []

    def cancel_order(
        self,
        *,
        order_id: str,
        client_order_id: str | None,
        action: str,
        credential_context: ResolvedAlpacaAccountContext | None = None,
    ) -> AlpacaPaperExecutionResult:
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
        base_url = self._resolve_base_url(credential_context).rstrip("/")
        payload = self._request_json(
            url=f"{base_url}/v2/orders",
            method="POST",
            headers=self._headers(client_order_id=client_order_id, credential_context=credential_context),
            payload={
                "symbol": symbol.upper(),
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
                "notional": round(notional, 2),
                "client_order_id": client_order_id,
            },
        )
        return AlpacaPaperExecutionResult(
            action=action,
            submitted=True,
            order_status=str(payload.get("status", "submitted")),
            order_id=_maybe_string(payload.get("id")),
            client_order_id=_maybe_string(payload.get("client_order_id")) or client_order_id,
            side="buy",
            notional=round(notional, 2),
            add_tier=add_tier,
            raw_response=payload,
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
    ) -> dict[str, object] | list[object]:
        try:
            return self._http_client.request_json(url=url, method=method, headers=headers, payload=payload)
        except AlpacaPaperTradingError:
            raise
        except HTTPError as exc:
            raise _normalize_alpaca_http_error(exc) from exc
        except TimeoutError as exc:
            raise AlpacaPaperTradingError(
                code="broker_api_timeout",
                message="Alpaca request timed out during Prime Stocks execution.",
                retryable=True,
            ) from exc
        except URLError as exc:
            raise AlpacaPaperTradingError(
                code="broker_api_timeout",
                message="Alpaca request could not be reached during Prime Stocks execution.",
                retryable=True,
            ) from exc
        except json.JSONDecodeError as exc:
            raise AlpacaPaperTradingError(
                code="broker_invalid_response",
                message="Alpaca returned invalid JSON during Prime Stocks execution.",
                retryable=False,
            ) from exc

    def _resolve_base_url(self, credential_context: ResolvedAlpacaAccountContext | None) -> str:
        if credential_context is not None and credential_context.environment == "live":
            return self._settings.alpaca_live_trading_base_url
        return self._settings.alpaca_trading_base_url

    @staticmethod
    def _ensure_stock_context(*, asset_type: str, product_key: str) -> None:
        normalized_asset_type = (asset_type or "").strip().lower()
        normalized_product_key = (product_key or "").strip().lower()
        if normalized_product_key not in SUPPORTED_PRODUCT_KEYS:
            raise ValueError(f"Prime Stocks paper execution only supports product_key='stocks.bismel1'. Received {product_key!r}.")
        if normalized_asset_type not in SUPPORTED_STOCK_ASSET_TYPES:
            raise ValueError(f"Prime Stocks paper execution accepts stock/equity symbols only. Received asset_type={asset_type!r}.")


def _maybe_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _maybe_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _format_qty(value: float) -> str:
    return format(float(value), ".8f").rstrip("0").rstrip(".")


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
    if exc.code == 429:
        return AlpacaPaperTradingError(
            code="broker_rate_limited",
            message=normalized_message,
            retryable=True,
            http_status=exc.code,
            raw_response=raw_payload,
        )
    if exc.code in {500, 502, 503, 504}:
        return AlpacaPaperTradingError(
            code="broker_api_error",
            message=normalized_message,
            retryable=True,
            http_status=exc.code,
            raw_response=raw_payload,
        )
    if "insufficient buying power" in lowered:
        return AlpacaPaperTradingError(
            code="broker_insufficient_buying_power",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
        )
    if "not tradable" in lowered:
        return AlpacaPaperTradingError(
            code="broker_asset_not_tradable",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
        )
    if "market is closed" in lowered or "market closed" in lowered:
        return AlpacaPaperTradingError(
            code="broker_market_closed",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
        )
    if "invalid" in lowered and "order" in lowered:
        return AlpacaPaperTradingError(
            code="broker_invalid_order",
            message=normalized_message,
            http_status=exc.code,
            raw_response=raw_payload,
        )
    return AlpacaPaperTradingError(
        code="broker_rejected",
        message=normalized_message,
        retryable=False,
        http_status=exc.code,
        raw_response=raw_payload,
    )


def _extract_alpaca_error_message(payload: dict[str, object] | None) -> str | None:
    if not payload:
        return None
    for key in ("message", "error", "detail"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    return None
