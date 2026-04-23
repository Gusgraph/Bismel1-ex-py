from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.shared.config import AppConfig

if TYPE_CHECKING:
    from app.services.firestore_runtime_store import PrimeStocksRuntimeConfigRecord


class AlpacaAccountResolutionError(RuntimeError):
    """Raised when the runtime cannot resolve a linked Alpaca account context."""


@dataclass(frozen=True)
class ResolvedAlpacaAccountContext:
    uid: str
    account_id: int
    alpaca_account_id: int
    broker_connection_id: int
    broker_credential_id: int
    environment: str
    data_feed: str
    access_mode: str
    trade_enabled: bool
    key_id: str
    secret: str
    slot_number: int = 1
    entitlement: dict[str, object] = field(default_factory=dict)
    product_id: str | None = None
    broker_name: str | None = None
    runtime_path: str | None = None
    linkage_status: str | None = None


@dataclass(frozen=True)
class RuntimeAccountTarget:
    uid: str
    account_id: int
    alpaca_account_id: int
    slot_number: int = 1
    environment: str = "paper"
    account_label: str | None = None
    entitlement: dict[str, object] = field(default_factory=dict)
    product_id: str | None = None
    runtime_path: str | None = None


class HttpJsonRequestProtocol:
    def request_json(self, *, url: str, headers: dict[str, str]) -> dict[str, object]:
        raise NotImplementedError


class UrllibJsonRequestClient(HttpJsonRequestProtocol):
    def request_json(self, *, url: str, headers: dict[str, str]) -> dict[str, object]:
        request = Request(url=url, headers=headers, method="GET")
        with urlopen(request, timeout=27) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload) if payload else {}


class LaravelAlpacaAccountResolver:
    def __init__(
        self,
        settings: AppConfig,
        http_client: HttpJsonRequestProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or UrllibJsonRequestClient()

    def resolve_runtime_account(self, runtime_config: "PrimeStocksRuntimeConfigRecord") -> ResolvedAlpacaAccountContext:
        if runtime_config.account_id is None or runtime_config.alpaca_account_id is None:
            raise AlpacaAccountResolutionError(
                "Prime Stocks runtime config is missing account_id or alpaca_account_id for linked Alpaca routing."
            )

        if not self._settings.laravel_runtime_bridge_url:
            raise AlpacaAccountResolutionError("Laravel runtime bridge URL is not configured.")
        if not self._settings.laravel_runtime_bridge_token:
            raise AlpacaAccountResolutionError("Laravel runtime bridge token is not configured.")

        query = urlencode(
            {
                "account_id": runtime_config.account_id,
                "alpaca_account_id": runtime_config.alpaca_account_id,
            }
        )
        separator = "&" if "?" in self._settings.laravel_runtime_bridge_url else "?"
        url = f"{self._settings.laravel_runtime_bridge_url.rstrip('/')}{separator}{query}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._settings.laravel_runtime_bridge_token}",
        }

        try:
            payload = self._http_client.request_json(url=url, headers=headers)
        except HTTPError as exc:
            if exc.code == 401:
                raise AlpacaAccountResolutionError("Laravel runtime bridge rejected the configured bearer token.") from exc
            if exc.code == 404:
                raise AlpacaAccountResolutionError("Selected linked Alpaca account was not found for the runtime account.") from exc
            raise AlpacaAccountResolutionError(
                f"Laravel runtime bridge returned HTTP {exc.code} while resolving the linked Alpaca account."
            ) from exc
        except URLError as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge could not be reached while resolving the linked Alpaca account."
            ) from exc
        except json.JSONDecodeError as exc:
            raise AlpacaAccountResolutionError("Laravel runtime bridge returned invalid JSON for the linked Alpaca account.") from exc

        try:
            return ResolvedAlpacaAccountContext(
                account_id=int(payload["account_id"]),
                uid=str(payload["uid"]).strip(),
                alpaca_account_id=int(payload["alpaca_account_id"]),
                broker_connection_id=int(payload["broker_connection_id"]),
                broker_credential_id=int(payload["broker_credential_id"]),
                slot_number=max(1, int(payload.get("slot_number", 1))),
                environment=_normalize_environment(str(payload.get("environment", "paper"))),
                data_feed=str(payload.get("data_feed", "iex")).strip().lower() or "iex",
                access_mode=str(payload.get("access_mode", "read_only")).strip().lower() or "read_only",
                trade_enabled=bool(payload.get("trade_enabled", False)),
                entitlement=_normalize_entitlement(payload.get("entitlement")),
                key_id=str(payload["key_id"]).strip(),
                secret=str(payload["secret"]).strip(),
                product_id=_maybe_string(payload.get("product_id")),
                broker_name=_maybe_string(payload.get("broker_name")),
                runtime_path=_maybe_string(payload.get("runtime_path")),
                linkage_status=_maybe_string(payload.get("linkage_status")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge response is missing required linked Alpaca account fields."
            ) from exc

    def resolve_runtime_account_for_slot(
        self,
        *,
        account_id: int,
        slot_number: int,
        product_id: str,
    ) -> ResolvedAlpacaAccountContext:
        if account_id is None:
            raise AlpacaAccountResolutionError(
                "Execution runtime config is missing account_id for linked Alpaca routing."
            )

        if not self._settings.laravel_runtime_bridge_url:
            raise AlpacaAccountResolutionError("Laravel runtime bridge URL is not configured.")
        if not self._settings.laravel_runtime_bridge_token:
            raise AlpacaAccountResolutionError("Laravel runtime bridge token is not configured.")

        query = urlencode(
            {
                "account_id": int(account_id),
                "slot_number": max(1, int(slot_number)),
                "product": (product_id or "").strip() or "execution",
            }
        )
        separator = "&" if "?" in self._settings.laravel_runtime_bridge_url else "?"
        url = f"{self._settings.laravel_runtime_bridge_url.rstrip('/')}{separator}{query}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._settings.laravel_runtime_bridge_token}",
        }

        try:
            payload = self._http_client.request_json(url=url, headers=headers)
        except HTTPError as exc:
            if exc.code == 401:
                raise AlpacaAccountResolutionError("Laravel runtime bridge rejected the configured bearer token.") from exc
            if exc.code == 404:
                raise AlpacaAccountResolutionError("Selected linked Alpaca account was not found for the runtime slot.") from exc
            raise AlpacaAccountResolutionError(
                f"Laravel runtime bridge returned HTTP {exc.code} while resolving the linked Alpaca slot."
            ) from exc
        except URLError as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge could not be reached while resolving the linked Alpaca slot."
            ) from exc
        except json.JSONDecodeError as exc:
            raise AlpacaAccountResolutionError("Laravel runtime bridge returned invalid JSON for the linked Alpaca slot.") from exc

        try:
            return ResolvedAlpacaAccountContext(
                account_id=int(payload["account_id"]),
                uid=str(payload["uid"]).strip(),
                alpaca_account_id=int(payload["alpaca_account_id"]),
                broker_connection_id=int(payload["broker_connection_id"]),
                broker_credential_id=int(payload["broker_credential_id"]),
                slot_number=max(1, int(payload.get("slot_number", slot_number))),
                environment=_normalize_environment(str(payload.get("environment", "paper"))),
                data_feed=str(payload.get("data_feed", "iex")).strip().lower() or "iex",
                access_mode=str(payload.get("access_mode", "read_only")).strip().lower() or "read_only",
                trade_enabled=bool(payload.get("trade_enabled", False)),
                entitlement=_normalize_entitlement(payload.get("entitlement")),
                key_id=str(payload["key_id"]).strip(),
                secret=str(payload["secret"]).strip(),
                product_id=_maybe_string(payload.get("product_id")),
                broker_name=_maybe_string(payload.get("broker_name")),
                runtime_path=_maybe_string(payload.get("runtime_path")),
                linkage_status=_maybe_string(payload.get("linkage_status")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge response is missing required linked Alpaca slot fields."
            ) from exc

    def list_runtime_targets(self, *, product_id: str = "prime_stocks") -> list[RuntimeAccountTarget]:
        if not self._settings.laravel_runtime_bridge_url:
            raise AlpacaAccountResolutionError("Laravel runtime bridge URL is not configured.")
        if not self._settings.laravel_runtime_bridge_token:
            raise AlpacaAccountResolutionError("Laravel runtime bridge token is not configured.")

        separator = "&" if "?" in self._settings.laravel_runtime_bridge_url else "?"
        query = urlencode(
            {
                "fanout": 1,
                "product": (product_id or "").strip() or "prime_stocks",
            }
        )
        url = f"{self._settings.laravel_runtime_bridge_url.rstrip('/')}{separator}{query}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._settings.laravel_runtime_bridge_token}",
        }

        try:
            payload = self._http_client.request_json(url=url, headers=headers)
        except HTTPError as exc:
            if exc.code == 401:
                raise AlpacaAccountResolutionError("Laravel runtime bridge rejected the configured bearer token.") from exc
            raise AlpacaAccountResolutionError(
                f"Laravel runtime bridge returned HTTP {exc.code} while resolving Prime Stocks scheduler targets."
            ) from exc
        except URLError as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge could not be reached while resolving Prime Stocks scheduler targets."
            ) from exc
        except json.JSONDecodeError as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge returned invalid JSON for Prime Stocks scheduler targets."
            ) from exc

        targets = payload.get("targets", [])
        if not isinstance(targets, list):
            raise AlpacaAccountResolutionError("Laravel runtime bridge returned invalid scheduler target payload.")

        resolved_targets: list[RuntimeAccountTarget] = []
        for target in targets:
            if not isinstance(target, dict):
                continue
            try:
                resolved_targets.append(
                    RuntimeAccountTarget(
                        uid=str(target["uid"]).strip(),
                        account_id=int(target["account_id"]),
                        alpaca_account_id=int(target["alpaca_account_id"]),
                        slot_number=max(1, int(target.get("slot_number", 1))),
                        environment=_normalize_environment(str(target.get("environment", "paper"))),
                        account_label=_maybe_string(target.get("account_label")),
                        entitlement=_normalize_entitlement(target.get("entitlement")),
                        product_id=_maybe_string(target.get("product_id")),
                        runtime_path=_maybe_string(target.get("runtime_path")),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise AlpacaAccountResolutionError(
                    "Laravel runtime bridge returned invalid Prime Stocks scheduler target fields."
                ) from exc

        return resolved_targets


def _normalize_environment(environment: str) -> str:
    return "live" if environment.strip().lower() == "live" else "paper"


def _normalize_entitlement(payload: object) -> dict[str, object]:
    return payload if isinstance(payload, dict) else {}


def _maybe_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
