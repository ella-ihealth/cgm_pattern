"""Registry for discovering and executing pattern rules."""
from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Dict, Type

from .models import PatternContext, PatternDetection, PatternInputBundle
from .rule_base import PatternRule
from .pattern_metadata import should_evaluate_rule


class RuleRegistry:
    """Keeps track of available rules by id."""

    def __init__(self) -> None:
        self._rules: Dict[str, PatternRule] = {}

    def register(self, rule_cls: Type[PatternRule]) -> Type[PatternRule]:
        if rule_cls.id in self._rules:
            raise ValueError(f"Rule '{rule_cls.id}' already registered")
        self._rules[rule_cls.id] = rule_cls()
        return rule_cls

    def clear(self) -> None:
        """Remove all registered rules."""

        self._rules.clear()

    def get(self, rule_id: str) -> PatternRule:
        return self._rules[rule_id]

    def items(self) -> Iterable[tuple[str, PatternRule]]:
        return self._rules.items()

    def values(self) -> Iterable[PatternRule]:
        return self._rules.values()

    def detect_all(
        self,
        window: PatternInputBundle,
        context: PatternContext,
        predicate: Callable[[PatternRule], bool] | None = None,
    ) -> list[PatternDetection]:
        """Run every registered rule, optionally filtering."""

        outputs: list[PatternDetection] = []
        for rule in self._rules.values():
            if predicate is not None and not predicate(rule):
                continue
            if not should_evaluate_rule(rule, context):
                continue
            detection = rule.detect(window, context)
            outputs.append(detection)
        return outputs


registry = RuleRegistry()


def register_rule(rule_cls: Type[PatternRule]) -> Type[PatternRule]:
    """Decorator for registering a rule at definition time."""

    return registry.register(rule_cls)


def clear_registry() -> None:
    """Remove all rule registrations."""

    registry.clear()
