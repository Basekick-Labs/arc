"""
Arc Core - Plugin System
Copyright (C) 2025 Basekick Labs

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
"""

from typing import Dict, Any, Optional, Callable, List
from abc import ABC, abstractmethod
from enum import Enum
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


class PluginHook(Enum):
    """Available plugin hooks in Arc Core"""
    # Write hooks
    PRE_WRITE = "pre_write"
    POST_WRITE = "post_write"
    WRITE_ERROR = "write_error"

    # Query hooks
    PRE_QUERY = "pre_query"
    POST_QUERY = "post_query"
    QUERY_ERROR = "query_error"

    # Storage hooks
    PRE_STORAGE_WRITE = "pre_storage_write"
    POST_STORAGE_WRITE = "post_storage_write"

    # Export hooks
    PRE_EXPORT = "pre_export"
    POST_EXPORT = "post_export"

    # Authentication hooks
    PRE_AUTH = "pre_auth"
    POST_AUTH = "post_auth"

    # Startup/Shutdown hooks
    STARTUP = "startup"
    SHUTDOWN = "shutdown"


@dataclass
class PluginContext:
    """Context passed to plugin hooks"""
    hook: PluginHook
    timestamp: datetime
    request_id: Optional[str] = None
    user_id: Optional[str] = None
    database: Optional[str] = None
    table: Optional[str] = None
    data: Optional[Any] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class Plugin(ABC):
    """
    Base class for Arc plugins.

    Plugins can implement hooks to extend Arc Core functionality.
    Enterprise features are implemented as plugins.
    """

    def __init__(self, name: str, version: str, config: Dict[str, Any] = None):
        self.name = name
        self.version = version
        self.config = config or {}
        self.enabled = True
        self.logger = logging.getLogger(f"plugin.{name}")

    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize the plugin.
        Called during Arc startup.

        Returns:
            bool: True if initialization successful
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Shutdown the plugin.
        Called during Arc shutdown.
        """
        pass

    async def on_hook(self, hook: PluginHook, context: PluginContext) -> PluginContext:
        """
        Called when a registered hook is triggered.

        Args:
            hook: The hook that was triggered
            context: Context data for the hook

        Returns:
            PluginContext: Modified context (can modify data, metadata)
        """
        return context

    def get_supported_hooks(self) -> List[PluginHook]:
        """
        Return list of hooks this plugin supports.
        Override to register for specific hooks.
        """
        return []


class PluginManager:
    """
    Manages plugin lifecycle and hook execution.
    """

    def __init__(self):
        self.plugins: Dict[str, Plugin] = {}
        self.hooks: Dict[PluginHook, List[Plugin]] = {hook: [] for hook in PluginHook}
        self.enabled = True

    def register(self, plugin: Plugin) -> None:
        """Register a plugin"""
        if plugin.name in self.plugins:
            logger.warning(f"Plugin {plugin.name} already registered, replacing")

        self.plugins[plugin.name] = plugin

        # Register plugin for its supported hooks
        for hook in plugin.get_supported_hooks():
            if plugin not in self.hooks[hook]:
                self.hooks[hook].append(plugin)

        logger.info(f"Registered plugin: {plugin.name} v{plugin.version}")

    def unregister(self, plugin_name: str) -> None:
        """Unregister a plugin"""
        if plugin_name not in self.plugins:
            return

        plugin = self.plugins[plugin_name]

        # Remove from all hooks
        for hook in PluginHook:
            if plugin in self.hooks[hook]:
                self.hooks[hook].remove(plugin)

        del self.plugins[plugin_name]
        logger.info(f"Unregistered plugin: {plugin_name}")

    async def initialize_all(self) -> None:
        """Initialize all registered plugins"""
        for plugin in self.plugins.values():
            try:
                success = await plugin.initialize()
                if success:
                    logger.info(f"Plugin {plugin.name} initialized successfully")
                else:
                    logger.error(f"Plugin {plugin.name} initialization failed")
                    plugin.enabled = False
            except Exception as e:
                logger.error(f"Error initializing plugin {plugin.name}: {e}")
                plugin.enabled = False

    async def shutdown_all(self) -> None:
        """Shutdown all registered plugins"""
        for plugin in self.plugins.values():
            try:
                await plugin.shutdown()
                logger.info(f"Plugin {plugin.name} shut down successfully")
            except Exception as e:
                logger.error(f"Error shutting down plugin {plugin.name}: {e}")

    async def execute_hook(self, hook: PluginHook, context: PluginContext) -> PluginContext:
        """
        Execute all plugins registered for a hook.
        Plugins are executed in registration order.

        Args:
            hook: The hook to execute
            context: Context data

        Returns:
            PluginContext: Modified context after all plugins
        """
        if not self.enabled:
            return context

        for plugin in self.hooks[hook]:
            if not plugin.enabled:
                continue

            try:
                context = await plugin.on_hook(hook, context)
            except Exception as e:
                logger.error(f"Error executing hook {hook.value} in plugin {plugin.name}: {e}")
                # Continue executing other plugins even if one fails

        return context

    def get_plugin(self, name: str) -> Optional[Plugin]:
        """Get a plugin by name"""
        return self.plugins.get(name)

    def list_plugins(self) -> List[Dict[str, Any]]:
        """List all registered plugins"""
        return [
            {
                "name": p.name,
                "version": p.version,
                "enabled": p.enabled,
                "hooks": [h.value for h in p.get_supported_hooks()]
            }
            for p in self.plugins.values()
        ]


# Global plugin manager instance
_plugin_manager: Optional[PluginManager] = None


def get_plugin_manager() -> PluginManager:
    """Get the global plugin manager instance"""
    global _plugin_manager
    if _plugin_manager is None:
        _plugin_manager = PluginManager()
    return _plugin_manager


def init_plugin_manager() -> PluginManager:
    """Initialize the plugin manager"""
    global _plugin_manager
    _plugin_manager = PluginManager()
    return _plugin_manager


# Example plugin implementation (for reference)
class ExamplePlugin(Plugin):
    """
    Example plugin showing how to implement a plugin.
    This would be in the arc-enterprise repository.
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(
            name="example",
            version="1.0.0",
            config=config
        )

    async def initialize(self) -> bool:
        self.logger.info("Example plugin initializing...")
        # Initialize resources (DB connections, etc.)
        return True

    async def shutdown(self) -> None:
        self.logger.info("Example plugin shutting down...")
        # Cleanup resources

    def get_supported_hooks(self) -> List[PluginHook]:
        return [
            PluginHook.PRE_WRITE,
            PluginHook.POST_QUERY,
        ]

    async def on_hook(self, hook: PluginHook, context: PluginContext) -> PluginContext:
        if hook == PluginHook.PRE_WRITE:
            # Example: Add metadata before write
            context.metadata["processed_by"] = self.name
            context.metadata["processed_at"] = datetime.now().isoformat()

        elif hook == PluginHook.POST_QUERY:
            # Example: Log query execution
            self.logger.info(f"Query executed: {context.request_id}")

        return context
