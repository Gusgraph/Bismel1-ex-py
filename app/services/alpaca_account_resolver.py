from __future__ import annotations

import json
from dataclasses import dataclass
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
                alpaca_account_id=int(payload["alpaca_account_id"]),
                broker_connection_id=int(payload["broker_connection_id"]),
                broker_credential_id=int(payload["broker_credential_id"]),
                environment=_normalize_environment(str(payload.get("environment", "paper"))),
                data_feed=str(payload.get("data_feed", "iex")).strip().lower() or "iex",
                access_mode=str(payload.get("access_mode", "read_only")).strip().lower() or "read_only",
                trade_enabled=bool(payload.get("trade_enabled", False)),
                key_id=str(payload["key_id"]).strip(),
                secret=str(payload["secret"]).strip(),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AlpacaAccountResolutionError(
                "Laravel runtime bridge response is missing required linked Alpaca account fields."
            ) from exc


def _normalize_environment(environment: str) -> str:
    return "live" if environment.strip().lower() == "live" else "paper"
