"""Shared agent primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd


@dataclass(slots=True)
class AgentContext:
    """Container for tabular inputs consumed by agents."""

    scores: pd.DataFrame
    candidates: pd.DataFrame = field(default_factory=pd.DataFrame)
    holdings: pd.DataFrame = field(default_factory=pd.DataFrame)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AgentResult:
    """Standardized agent output."""

    agent_name: str
    summary: str
    actions: list[dict[str, Any]]
    metrics: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseAgent(Protocol):
    """Protocol for all agents in the platform."""

    name: str

    def run(self, context: AgentContext) -> AgentResult:
        """Execute the agent against the supplied context."""

