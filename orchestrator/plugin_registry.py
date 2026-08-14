"""Provider plugin discovery with a locked production transport boundary.

Wave 5 makes the model transport replaceable at the orchestration boundary,
not at deployment time.  The only production plugin intentionally shipped and
accepted here is the existing cookie-backed Web Claude adapter.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol, runtime_checkable

from .provider_adapter import LEGACY_WEB_PROVIDER, ProviderAdapter

PRODUCTION_PROVIDER = LEGACY_WEB_PROVIDER
COOKIE_WEB_TRANSPORT = "cookie_web_claude"


@dataclass(frozen=True, slots=True)
class ProviderPluginMetadata:
    name: str
    transport: str
    version: int = 1
    production: bool = True
    capabilities: tuple[str, ...] = ("completion", "buffered_stream")

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.transport.strip():
            raise ValueError("plugin name and transport are required")
        if self.version < 1:
            raise ValueError("plugin version must be at least 1")


@runtime_checkable
class ProviderPlugin(Protocol):
    metadata: ProviderPluginMetadata

    def create_adapter(self, account: Mapping[str, str]) -> ProviderAdapter: ...


class ProviderPluginError(RuntimeError):
    pass


class ProviderPluginRegistry:
    """Thread-safe plugin registry enforcing the production allowlist."""

    production_provider = PRODUCTION_PROVIDER
    allowed_production_transports = frozenset({COOKIE_WEB_TRANSPORT})

    def __init__(self) -> None:
        self._plugins: dict[str, ProviderPlugin] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _validate_metadata(metadata: ProviderPluginMetadata) -> None:
        if (
            metadata.name == PRODUCTION_PROVIDER
            and metadata.transport != COOKIE_WEB_TRANSPORT
        ):
            raise ProviderPluginError(
                "legacy_web is restricted to the cookie-backed Web Claude transport"
            )
        if metadata.production and metadata.name != PRODUCTION_PROVIDER:
            raise ProviderPluginError(
                "production provider plugins are restricted to the "
                "cookie-backed Web Claude transport"
            )

    def register(self, plugin: ProviderPlugin, *, replace: bool = False) -> None:
        if not isinstance(plugin, ProviderPlugin):
            raise TypeError("plugin must implement ProviderPlugin")
        metadata = plugin.metadata
        self._validate_metadata(metadata)
        with self._lock:
            if metadata.name == PRODUCTION_PROVIDER and metadata.name in self._plugins:
                raise ProviderPluginError(
                    "legacy_web provider plugin is locked and cannot be replaced"
                )
            if metadata.name in self._plugins and not replace:
                raise ProviderPluginError(
                    f"provider plugin is already registered: {metadata.name}"
                )
            self._plugins[metadata.name] = plugin

    def metadata(self) -> tuple[ProviderPluginMetadata, ...]:
        with self._lock:
            return tuple(
                self._plugins[name].metadata for name in sorted(self._plugins)
            )

    def get(self, name: str) -> ProviderPlugin:
        with self._lock:
            plugin = self._plugins.get(name)
        if plugin is None:
            raise ProviderPluginError(f"provider plugin is not registered: {name}")
        return plugin

    def create_adapter(
        self,
        account: Mapping[str, str],
        *,
        provider: str = PRODUCTION_PROVIDER,
    ) -> ProviderAdapter:
        if provider != PRODUCTION_PROVIDER:
            raise ProviderPluginError(
                "provider switching is disabled; production uses Web Claude cookies"
            )
        plugin = self.get(provider)
        metadata = plugin.metadata
        if (
            metadata.name != PRODUCTION_PROVIDER
            or metadata.transport != COOKIE_WEB_TRANSPORT
        ):
            raise ProviderPluginError(
                "legacy_web is restricted to the cookie-backed Web Claude transport"
            )
        adapter = plugin.create_adapter(account)
        if not isinstance(adapter, ProviderAdapter):
            raise ProviderPluginError(
                f"plugin {provider} returned an invalid provider adapter"
            )
        return adapter


@dataclass(frozen=True, slots=True)
class CookieWebClaudePlugin:
    """Plugin wrapper around the legacy adapter without creating import cycles."""

    adapter_factory: Callable[[dict[str, str]], ProviderAdapter]
    metadata: ProviderPluginMetadata = ProviderPluginMetadata(
        name=PRODUCTION_PROVIDER,
        transport=COOKIE_WEB_TRANSPORT,
    )

    def create_adapter(self, account: Mapping[str, str]) -> ProviderAdapter:
        required = {"org_id", "cookie_string"}
        missing = sorted(required - set(account))
        if missing:
            raise ProviderPluginError(
                f"Web Claude account is missing: {', '.join(missing)}"
            )
        return self.adapter_factory({str(key): str(value) for key, value in account.items()})


def build_production_registry(
    adapter_factory: Callable[[dict[str, str]], ProviderAdapter],
) -> ProviderPluginRegistry:
    registry = ProviderPluginRegistry()
    registry.register(CookieWebClaudePlugin(adapter_factory=adapter_factory))
    return registry
