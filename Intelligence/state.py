"""
intelligence/state.py
---------------------
Shared state object that flows through every LangGraph node.
Each node reads from state, adds its output, passes it forward.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineState:
    """
    Single state object that flows through the entire LangGraph pipeline.
    Each node populates its section and passes the full state forward.
    """

    # ── Input ─────────────────────────────────────────────────────────
    db_url: str = ""                        # postgresql://user:pass@host/db
    db_config: dict = field(default_factory=dict)

    # ── Phase 1: Schema ───────────────────────────────────────────────
    table_meta: dict = field(default_factory=dict)          # raw metadata
    schema_summary: str = ""                                # text summary for LLM

    # ── Phase 2: Domain detection ────────────────────────────────────
    detected_domain: str = ""                               # "core_banking", "real_estate"...
    domain_confidence: float = 0.0
    domain_reasoning: str = ""

    # ── Phase 3: Column intelligence ─────────────────────────────────
    column_values: dict = field(default_factory=dict)       # table→col→[values]
    domains_yaml_content: dict = field(default_factory=dict)

    # ── Phase 4: Volume inference ────────────────────────────────────
    volume_ratios: dict = field(default_factory=dict)       # table→{parent,ratio}
    anchor_entities: dict = field(default_factory=dict)     # table→count

    # ── Phase 5: Scenario generation ─────────────────────────────────
    scenarios: dict = field(default_factory=dict)           # name→scenario_dict

    # ── Phase 6: Config writing ───────────────────────────────────────
    domains_yaml_path: str = ""
    config_yaml_path: str = ""
    scenarios_yaml_path: str = ""

    # ── Phase 7: Pipeline execution ──────────────────────────────────
    generation_complete: bool = False
    total_rows_generated: int = 0
    errors: list[str] = field(default_factory=list)

    # ── Runtime ───────────────────────────────────────────────────────
    llm_calls: int = 0
    log_messages: list[str] = field(default_factory=list)

    def log(self, msg: str) -> None:
        self.log_messages.append(msg)
        print(f"  [Pipeline] {msg}")