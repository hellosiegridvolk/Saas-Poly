"""Static enforcement of the multi-tenancy invariant (spec §3.6).

Every concrete UserScopedRepository subclass must take ``user_id`` as the
first non-self argument of every public async query method. Mutating
methods like ``add(entity)`` are exempt because the entity carries
``user_id`` on itself.
"""

from __future__ import annotations

import inspect

from shared.db.repositories import (
    SignalRepository,
    StrategyInstanceRepository,
    UserScopedRepository,
)

EXEMPT_METHODS = {"add"}


def _public_async_methods(cls: type) -> list[str]:
    methods: list[str] = []
    for name, _value in inspect.getmembers(cls, predicate=inspect.iscoroutinefunction):
        if name.startswith("_") or name in EXEMPT_METHODS:
            continue
        methods.append(name)
    return methods


def test_base_class_methods_take_user_id_first() -> None:
    for method_name in _public_async_methods(UserScopedRepository):
        sig = inspect.signature(getattr(UserScopedRepository, method_name))
        params = [p for p in sig.parameters.values() if p.name != "self"]
        assert params, f"{method_name} has no parameters"
        assert params[0].name == "user_id", (
            f"UserScopedRepository.{method_name} must take user_id first; "
            f"got {params[0].name!r}"
        )


def test_concrete_subclasses_enforce_user_id_scoping() -> None:
    subclasses: list[type[UserScopedRepository[object]]] = [
        SignalRepository,
        StrategyInstanceRepository,
    ]
    for cls in subclasses:
        for method_name in _public_async_methods(cls):
            sig = inspect.signature(getattr(cls, method_name))
            params = [p for p in sig.parameters.values() if p.name != "self"]
            assert params, f"{cls.__name__}.{method_name} has no parameters"
            assert params[0].name == "user_id", (
                f"{cls.__name__}.{method_name} must take user_id first; "
                f"got {params[0].name!r}"
            )


def test_concrete_subclasses_declare_model() -> None:
    for cls in (SignalRepository, StrategyInstanceRepository):
        assert hasattr(cls, "model"), f"{cls.__name__} must declare a `model` attribute"
