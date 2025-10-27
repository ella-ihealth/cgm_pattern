"""Rule package that ensures registration on import."""
from __future__ import annotations

import pkgutil
from importlib import import_module, reload

from ..registry import clear_registry

_EXCLUDED_MODULES = {"utils"}


def _discover_rule_modules() -> list[str]:
    """Return sorted module names containing rule definitions."""

    module_names: list[str] = []
    for module_info in pkgutil.iter_modules(__path__):
        if module_info.ispkg:
            continue
        name = module_info.name
        if name.startswith("_") or name in _EXCLUDED_MODULES:
            continue
        module_names.append(name)
    module_names.sort()
    return module_names


_RULE_MODULES = _discover_rule_modules()


def register_all_rules() -> None:
    """Import each rule module; classes self-register via decorator."""

    for module_name in _RULE_MODULES:
        import_module(f"{__name__}.{module_name}")


def reload_rules(*, clear: bool = True) -> None:
    """Reload rule modules and optionally reset the registry first."""

    if clear:
        clear_registry()
    for module_name in _RULE_MODULES:
        module = import_module(f"{__name__}.{module_name}")
        reload(module)


register_all_rules()

__all__ = list(_RULE_MODULES)
