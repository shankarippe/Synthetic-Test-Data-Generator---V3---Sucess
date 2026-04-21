"""
intelligence/nodes.py  [v2 — Multi-DB + T24 Precision]
-------------------------------------------------------
Every LangGraph node lives here.
Each node receives PipelineState, does one job, returns updated state.

Nodes:
  1. schema_reader_node      → reads DB metadata (Postgres/Oracle/MSSQL/MySQL)
  2. domain_detector_node    → SLM detects domain from schema
  3. column_inference_node   → T24-precise column values + LLM augmentation
  4. volume_inference_node   → SLM infers parent:child ratios
  5. scenario_generator_node → SLM generates business scenarios
  6. config_writer_node      → writes all YAML files automatically
  7. pipeline_executor_node  → runs the generation + load pipeline
"""

from __future__ import annotations

import json
import sys
import time                          # ← ADDED: needed for sleep between LLM batches
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from Intelligence.state import PipelineState


# ===========================================================================
# NODE 1 — Schema Reader  [UPDATED: uses adapter factory]
# ===========================================================================

def schema_reader_node(state: PipelineState, llm_client) -> PipelineState:
    """
    Connects to ANY supported database using the adapter factory,
    reads full schema metadata, builds a compact text summary for the LLM.
    """
    engine = state.db_config.get("database", {}).get("engine", "postgres")
    print(f"\n[Node 1/7] Reading schema from {engine.upper()}...")

    loggers = _silent_loggers()

    from adapters import get_adapter
    adapter = get_adapter(state.db_config, loggers)
    table_meta = adapter.read_all()
    state.table_meta = table_meta
    # Store adapter engine for use in pipeline_executor
    state.db_config["_adapter_engine"] = engine

    # Build compact schema summary for LLM
    lines = [f"Database schema ({engine}) with {len(table_meta)} tables:\n"]
    for tbl_name, tm in table_meta.items():
        pk_cols = ", ".join(tm.primary_keys) if tm.primary_keys else "none"
        fk_info = []
        for fk in tm.foreign_keys[:3]:
            fk_info.append(f"{fk.column}→{fk.ref_table}.{fk.ref_column}")
        fk_str = ", ".join(fk_info) if fk_info else "none"

        col_summary = []
        for col in tm.columns[:15]:
            col_summary.append(f"{col.name}({col.data_type})")
        if len(tm.columns) > 15:
            col_summary.append(f"... +{len(tm.columns)-15} more")

        lines.append(
            f"TABLE: {tbl_name}\n"
            f"  PKs: {pk_cols}\n"
            f"  FKs: {fk_str}\n"
            f"  Columns: {', '.join(col_summary)}\n"
        )

    state.schema_summary = "\n".join(lines)
    state.log(f"Schema read via {engine}: {len(table_meta)} tables discovered")
    return state


# ===========================================================================
# NODE 2 — Domain Detector
# ===========================================================================

def domain_detector_node(state: PipelineState, llm_client) -> PipelineState:
    """
    LLM reads the schema summary and detects what business domain this is.
    Now also tries to detect if this is a Temenos T24 schema.
    """
    print("\n[Node 2/7] Detecting business domain from schema...")

    # Heuristic pre-check: T24 schemas have characteristic table/app names.
    # Tables can be prefixed with tstg_, stg_, dm_, w1_, etc. — strip that
    # and look at the real T24 application name underneath.
    table_names = list(state.table_meta.keys())

    _STAGING_PREFIXES = ("tstg_", "stg_", "dm_", "w1_", "w2_", "dw_", "edw_", "raw_", "land_")
    _T24_APP_PREFIXES = (
        "aa_", "ac_", "de_", "ft_", "fx_", "lc_", "li_", "sc_", "sw_",
        "eb_", "st_", "mm_", "md_", "re_", "cr_", "am_", "pf_",
    )

    def _strip_pfx(name: str) -> str:
        n = name.lower()
        for p in _STAGING_PREFIXES:
            if n.startswith(p):
                return n[len(p):]
        return n

    stripped_names = [_strip_pfx(t) for t in table_names]
    t24_indicators = sum(
        1 for s in stripped_names
        if any(s.startswith(app) for app in _T24_APP_PREFIXES)
        or any(k in s for k in ("customer", "account", "arrangement", "company", "stmt_entry"))
    )
    likely_t24 = t24_indicators >= 3

    system_prompt = """You are a database expert specializing in banking systems, 
particularly Temenos T24/Transact core banking systems.
Analyze table names, column names, and relationships to identify the business domain."""

    user_prompt = f"""Analyze this database schema and identify the business domain.
{'HINT: Many tables have T24/Temenos naming patterns. Consider core_banking or t24 as domain.' if likely_t24 else ''}

{state.schema_summary}

Respond with a JSON object:
{{
  "domain": "short_domain_name",
  "display_name": "Human Readable Domain Name",
  "confidence": 0.95,
  "reasoning": "Brief explanation",
  "is_t24": {str(likely_t24).lower()},
  "sub_domains": ["sub_area_1", "sub_area_2"],
  "key_entities": ["main entity 1", "main entity 2", "main entity 3"]
}}

Examples of domain values: core_banking, t24_banking, retail_banking, insurance, 
real_estate, healthcare, manufacturing, retail, fertilizers, telecom, logistics, hr_payroll"""

    result = llm_client.ask_json(system_prompt, user_prompt)
    state.llm_calls += 1

    state.detected_domain = result.get("domain", "unknown")
    state.domain_confidence = result.get("confidence", 0.0)
    state.domain_reasoning = result.get("reasoning", "")
    # Store T24 flag in config for data generator use
    state.db_config["_is_t24"] = result.get("is_t24", likely_t24)

    state.log(
        f"Domain detected: '{state.detected_domain}' "
        f"(confidence: {state.domain_confidence:.0%}) "
        f"T24={state.db_config.get('_is_t24', False)} — {state.domain_reasoning}"
    )
    return state


# ===========================================================================
# NODE 3 — Column Intelligence  [UPDATED: T24-precise values + rate limit fix]
# ===========================================================================

def column_inference_node(state: PipelineState, llm_client) -> PipelineState:
    """
    For each table, uses T24DataLibrary for banking-precise values,
    then augments with LLM for any gaps.

    Rate limit fix: sleeps 15 seconds between LLM batches so the
    tokens-per-minute window resets and we never hit a 429 error.
    With batch_size=8 and ~7 batches for 52 tables, this adds roughly
    90 seconds to the pipeline run — a worthwhile trade for reliability.
    """
    print(f"\n[Node 3/7] Inferring column values (T24-precise)...")

    from core.t24_data_library import get_t24_library

    table_meta = state.table_meta
    domain = state.detected_domain
    is_t24 = state.db_config.get("_is_t24", False)
    t24_lib = get_t24_library()

    all_column_patterns: dict = {}
    table_overrides: dict = {}
    suffix_patterns: dict = {}

    # ── Step 1: T24 Library — resolve known fields with precise values ────
    if is_t24 or "banking" in domain.lower() or "t24" in domain.lower():
        print("  Using T24 precision data library...")
        for tbl_name, tm in table_meta.items():
            tbl_overrides_for_table = {}
            for col in tm.columns:
                if col.name.lower() in (pk.lower() for pk in tm.primary_keys):
                    continue  # skip PKs
                if any(fk.column.lower() == col.name.lower() for fk in tm.foreign_keys):
                    continue  # skip FKs

                vals = t24_lib.resolve(tbl_name, col.name)
                if vals:
                    tbl_overrides_for_table[col.name] = vals

            if tbl_overrides_for_table:
                table_overrides[tbl_name] = tbl_overrides_for_table

    # ── Step 2: LLM augmentation for non-T24 columns ─────────────────────
    print("  LLM augmenting remaining columns...")
    system_prompt = f"""You are a data expert for {domain} banking systems.
Generate realistic domain-appropriate values for database columns.
Think about what real T24/Temenos or banking data looks like."""

    # Find columns NOT already covered by T24 library
    uncovered_tables = {}
    for tbl_name, tm in table_meta.items():
        covered = set((table_overrides.get(tbl_name) or {}).keys())
        uncovered_cols = []
        for col in tm.columns:
            if col.name in covered:
                continue
            if col.name.lower() in (pk.lower() for pk in tm.primary_keys):
                continue
            if any(fk.column.lower() == col.name.lower() for fk in tm.foreign_keys):
                continue
            if col.data_type.lower() in ("numeric", "integer", "bigint", "date", "timestamp", "boolean"):
                continue
            max_len = col.character_maximum_length
            if max_len and max_len <= 3:
                continue  # too short for interesting values
            uncovered_cols.append(f"{col.name}:{col.data_type}")
        if uncovered_cols:
            uncovered_tables[tbl_name] = uncovered_cols

    # Batch process uncovered tables
    table_names = list(uncovered_tables.keys())
    batch_size = 8
    total_batches = (len(table_names) + batch_size - 1) // batch_size

    for batch_start in range(0, len(table_names), batch_size):
        batch = table_names[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        print(f"  LLM batch {batch_num}/{total_batches}: {batch[:4]}{'...' if len(batch)>4 else ''}")

        batch_desc = []
        for tbl in batch:
            cols = uncovered_tables[tbl][:15]
            if cols:
                batch_desc.append(f"{tbl}: {', '.join(cols)}")

        if not batch_desc:
            # ── Rate limit sleep (even for empty batches to keep spacing) ──
            if batch_num < total_batches:
                print(f"  Waiting 15s before next batch (rate limit protection)...")
                time.sleep(15)
            continue

        user_prompt = f"""Domain: {domain}

For these columns generate realistic banking/T24 values:
{chr(10).join(batch_desc)}

Respond with JSON:
{{
  "table_overrides": {{
    "table_name": {{
      "column_name": ["value1", "value2", "value3", "value4"]
    }}
  }},
  "common_patterns": {{
    "column_name": ["value1", "value2", "value3"]
  }}
}}

Rules:
- Only VARCHAR/CHAR columns with clear categorical meaning
- 4-8 realistic banking/T24 values per column
- Skip free-text columns (description, narrative, remarks, notes)"""

        try:
            result = llm_client.ask_json(system_prompt, user_prompt)
            state.llm_calls += 1

            for tbl, cols in result.get("table_overrides", {}).items():
                if tbl in table_meta:
                    table_overrides.setdefault(tbl, {}).update(cols)

            for col, vals in result.get("common_patterns", {}).items():
                if col not in all_column_patterns:
                    all_column_patterns[col] = vals

        except Exception as e:
            state.log(f"Warning: LLM batch {batch_num} failed: {e}")

        # ── Rate limit protection ─────────────────────────────────────────
        # Sleep 15 seconds between every batch (except after the last one).
        # This keeps token usage well under the 12,000 TPM free tier limit.
        # Each batch uses ~1,500–2,000 tokens. After 15s the window has
        # partially reset, so the next batch starts with headroom.
        # Remove this sleep only if you upgrade to the Groq Developer tier.
        if batch_num < total_batches:
            print(f"  Waiting 15s before next batch (rate limit protection)...")
            time.sleep(15)                               # ← THE FIX

    # ── Step 3: Suffix patterns (T24-specific) ────────────────────────────
    suffix_patterns = _t24_suffix_patterns()

    # ── Assemble domains.yaml content ─────────────────────────────────────
    state.column_values = {
        "column_patterns": all_column_patterns,
        "suffix_patterns": suffix_patterns,
        "substring_patterns": {},
        "table_prefix_overrides": table_overrides,
    }
    state.domains_yaml_content = {
        "domain_profile": domain,
        "domains": {domain: state.column_values}
    }

    t24_cols = sum(len(v) for v in table_overrides.values())
    state.log(
        f"Column inference complete: "
        f"{t24_cols} T24-precise column overrides, "
        f"{len(all_column_patterns)} common patterns"
    )
    return state


def _t24_suffix_patterns() -> dict:
    """Return T24-standard suffix → value mappings."""
    return {
        "_status":    ["LIVE", "INACT", "PEND", "HISTORY"],
        "_type":      ["STANDARD", "PREMIUM", "CORPORATE", "RETAIL"],
        "_code":      None,   # too generic
        "_flag":      ["Y", "N"],
        "_ind":       ["Y", "N"],
        "_indicator": ["Y", "N"],
        "_method":    ["STANDARD", "MANUAL", "AUTO", "BATCH"],
        "_channel":   ["INTERNET", "MOBILE", "BRANCH", "ATM", "API"],
        "_currency":  ["USD", "EUR", "GBP", "JPY", "CHF", "SGD", "AED"],
        "_country":   ["GB", "US", "DE", "FR", "SG", "AE", "IN", "CN"],
        "_ccy":       ["USD", "EUR", "GBP", "JPY", "CHF", "SGD", "AED"],
        "_basis":     ["A", "M", "D"],
        "_frequency": ["DAILY", "MONTHLY", "QUARTERLY", "ANNUAL"],
    }


# ===========================================================================
# NODE 4 — Volume Inference
# ===========================================================================

def volume_inference_node(state: PipelineState, llm_client) -> PipelineState:
    """LLM infers realistic volume ratios from table semantics."""
    print(f"\n[Node 4/7] Inferring volume ratios for {len(state.table_meta)} tables...")

    table_meta = state.table_meta
    domain = state.detected_domain

    table_info = []
    for tbl, tm in table_meta.items():
        parents = [fk.ref_table for fk in tm.foreign_keys]
        table_info.append({
            "table": tbl,
            "parents": parents[:3],
            "column_count": len(tm.columns),
        })

    system_prompt = f"""You are a database architect for {domain} systems.
You understand data volumes in banking/T24 databases."""

    user_prompt = f"""For this {domain} database, determine volume ratios.

Tables:
{json.dumps(table_info[:30], indent=2)}

Respond with JSON:
{{
  "anchor_entities": {{
    "root_table_name": 100
  }},
  "ratios": {{
    "child_table_name": {{
      "parent": "parent_table_name",
      "ratio": 5
    }}
  }},
  "reasoning": "Brief explanation"
}}

T24 volume guidelines:
- Company/Institution tables: 1-5 rows
- Customer tables: 100-500 rows  
- Account tables: 2-5 per customer
- Statement/Entry tables: 20-50 per account
- Transaction tables: 10-30 per account
- Reference/Master data: 10-50 rows
- History tables: same ratio as parent (1:1)
- Only pick ONE parent per child table"""

    result = llm_client.ask_json(system_prompt, user_prompt)
    state.llm_calls += 1

    state.anchor_entities = result.get("anchor_entities", {})
    state.volume_ratios = result.get("ratios", {})

    state.log(
        f"Volume inference: {len(state.anchor_entities)} anchors, "
        f"{len(state.volume_ratios)} ratios"
    )
    return state


# ===========================================================================
# NODE 5 — Scenario Generator
# ===========================================================================

def scenario_generator_node(state: PipelineState, llm_client) -> PipelineState:
    """LLM generates 3-5 business scenarios relevant to the detected domain."""
    print(f"\n[Node 5/7] Generating business scenarios for {state.detected_domain}...")

    domain = state.detected_domain
    table_names = list(state.table_meta.keys())

    system_prompt = f"""You are a business analyst for {domain} banking systems.
Design test scenarios for validating data pipelines, ML models, and reporting."""

    user_prompt = f"""Create 3 realistic business test scenarios for a {domain} database.

Available tables: {', '.join(table_names[:20])}

Respond with JSON:
{{
  "scenarios": {{
    "scenario_key_name": {{
      "description": "What this scenario tests",
      "domain": "{domain}",
      "anchor_overrides": {{"table_name": 50}},
      "volume_skews": {{"table_name": 2.0}},
      "column_overrides": {{
        "table_name": {{"column_name": ["value1", "value2"]}}
      }},
      "date_context": {{"reference_date": "today", "date_range_years": 2}}
    }}
  }}
}}

Suggest T24-relevant scenarios such as:
- Month-end close with high statement volumes
- New customer onboarding batch
- Loan default stress test
- Regulatory reporting period
- Cross-currency settlement run"""

    result = llm_client.ask_json(system_prompt, user_prompt)
    state.llm_calls += 1

    state.scenarios = result.get("scenarios", {})
    state.log(f"Scenarios generated: {list(state.scenarios.keys())}")
    return state


# ===========================================================================
# NODE 6 — Config Writer
# ===========================================================================

def config_writer_node(state: PipelineState, llm_client) -> PipelineState:
    """Writes domains.yaml, config.yaml, scenarios.yaml automatically."""
    print("\n[Node 6/7] Writing configuration files automatically...")

    import yaml
    from pathlib import Path

    output_dir = Path(state.db_config.get("generation", {}).get("output_dir", "./output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── domains.yaml ──────────────────────────────────────────────────
    domains_path = Path("domains.yaml")
    with open(domains_path, "w", encoding="utf-8") as fh:
        yaml.dump(
            state.domains_yaml_content,
            fh, default_flow_style=False, allow_unicode=True, sort_keys=False,
        )
    state.domains_yaml_path = str(domains_path)
    print(f"  ✓ domains.yaml written ({domains_path})")

    # ── scenarios.yaml ────────────────────────────────────────────────
    scenarios_path = Path("scenarios.yaml")
    with open(scenarios_path, "w", encoding="utf-8") as fh:
        yaml.dump(
            {"scenarios": state.scenarios},
            fh, default_flow_style=False, allow_unicode=True, sort_keys=False,
        )
    state.scenarios_yaml_path = str(scenarios_path)
    print(f"  ✓ scenarios.yaml written ({scenarios_path})")

    # ── config.yaml ───────────────────────────────────────────────────
    config_path = Path(state.db_config.get("_config_path", "config.yaml"))
    if config_path.exists():
        with open(config_path) as fh:
            config = yaml.safe_load(fh) or {}
    else:
        config = {"database": state.db_config.get("database", {}), "generation": {}}

    config["anchor_entities"] = state.anchor_entities
    config["ratios"] = state.volume_ratios
    config.setdefault("database", {})["engine"] = state.db_config.get("database", {}).get("engine", "postgres")

    with open(config_path, "w") as fh:
        yaml.dump(config, fh, default_flow_style=False, sort_keys=False)
    state.config_yaml_path = str(config_path)
    print(f"  ✓ config.yaml updated ({config_path})")

    state.log(
        f"Config files written — domain={state.detected_domain}, "
        f"engine={state.db_config.get('database', {}).get('engine', 'postgres')}, "
        f"anchors={len(state.anchor_entities)}, "
        f"ratios={len(state.volume_ratios)}, "
        f"scenarios={len(state.scenarios)}"
    )
    return state


# ===========================================================================
# NODE 7 — Pipeline Executor  [UPDATED: uses adapter for loading]
# ===========================================================================

def pipeline_executor_node(state: PipelineState, llm_client) -> PipelineState:
    """
    Runs the full synthetic data generation pipeline using all
    auto-generated configs. Now supports multi-DB loading.
    """
    print("\n[Node 7/7] Running generation pipeline...")

    import time
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))

    from dependency_graph import DependencyGraph
    from auto_ratio_inferrer import AutoRatioInferrer
    from entity_registry import EntityRegistry
    from data_generator import DataGenerator, DomainConfig
    from file_writer import FileWriter
    from parallel_writer import ParallelWriter
    from seed_manager import SeedManager
    from adapters import get_adapter

    loggers = _silent_loggers()
    config = state.db_config

    # Build dependency graph using adapter-provided metadata
    graph = DependencyGraph(state.table_meta, loggers)

    # Load domain config (T24-precise values already baked in)
    domain = DomainConfig("domains.yaml")

    # Volume plan
    inferrer = AutoRatioInferrer(graph, state.table_meta, config, loggers)
    volume_plan = inferrer.infer_volume_plan()

    # Seed manager
    seed_mgr = SeedManager(config, loggers)
    generation_order = graph.generation_order()
    parent_map = {}
    for tbl in generation_order:
        parents = graph.parents_of(tbl)
        parent_map[tbl] = parents[0] if parents else None
    table_seeds = seed_mgr.derive_seeds_for_all(generation_order, parent_map)
    seed_mgr.set_volume_plan(volume_plan)
    config = seed_mgr.apply_to_config(config, table_seeds)

    print(f"  Seed profile : {seed_mgr.profile_name}")
    print(f"  Global seed  : {seed_mgr.global_seed}")
    print(f"  Engine       : {config.get('database', {}).get('engine', 'postgres').upper()}")

    registry = EntityRegistry(loggers)
    parallel = ParallelWriter(config, loggers)
    csv_paths: dict = {}
    total_rows = 0

    for table_name in generation_order:
        tm = state.table_meta[table_name]
        rows = volume_plan.get(table_name, 0)
        if rows == 0:
            continue

        t1 = time.perf_counter()

        if parallel.should_parallelize(rows):
            print(f"  Generating: {table_name:<45} {rows:>8,} rows  [PARALLEL]")
            csv_path = parallel.write_parallel(table_name, tm, graph, registry, domain, rows)
        else:
            print(f"  Generating: {table_name:<45} {rows:>8,} rows  [single]")
            gen = DataGenerator(tm, graph, registry, domain, config, loggers)
            writer = FileWriter(table_name, gen.column_names, config, loggers)
            csv_path = writer.write_all(gen.generate(rows), rows)

        csv_paths[table_name] = csv_path
        total_rows += rows

        elapsed = time.perf_counter() - t1
        rps = rows / elapsed if elapsed > 0 else 0
        print(f"    ✓ {rows:,} rows in {elapsed:.1f}s  ({rps:,.0f} rows/s)")

    state.total_rows_generated = total_rows

    # ── Load using the appropriate DB adapter ─────────────────────────
    # dry_run can arrive from CLI (database._dry_run) or API (top-level _dry_run)
    # We check all locations so neither path can silently skip the flag.
    dry_run = bool(
        config.get("_dry_run")                          # set by API layer
        or config.get("database", {}).get("_dry_run")   # set by CLI layer
        or state.db_config.get("_dry_run")              # set directly on state
        or state.db_config.get("database", {}).get("_dry_run")
    )
    print(f"\n  dry_run={dry_run}  (DB load will {'be SKIPPED' if dry_run else 'proceed'})")
    if not dry_run:
        engine = config.get("database", {}).get("engine", "postgres")
        print(f"\n  Loading into {engine.upper()}...")

        adapter = get_adapter(config, loggers)
        for tbl_name in generation_order:
            if tbl_name not in csv_paths:
                continue
            col_names = [c.name for c in state.table_meta[tbl_name].columns]
            adapter.bulk_load(tbl_name, str(csv_paths[tbl_name]), col_names)

        print(f"  ✓ All tables loaded into {engine.upper()}.")
    else:
        print("\n  [Dry run] Skipping database load.")

    run_id = seed_mgr.register_run()
    seed_mgr.print_registry_summary()

    state.generation_complete = True
    state.log(
        f"Pipeline complete: {total_rows:,} rows across {len(csv_paths)} tables "
        f"| engine={config.get('database', {}).get('engine', 'postgres')} "
        f"| seed_profile={seed_mgr.profile_name} run_id={run_id}"
    )
    return state


# ===========================================================================
# Helper
# ===========================================================================

def _silent_loggers() -> dict:
    """Return loggers that write to app.log but don't clutter the console."""
    import logging
    from pathlib import Path
    app = logging.getLogger("app")
    err = logging.getLogger("error")
    audit = logging.getLogger("audit")
    if not app.handlers:
        Path("logs").mkdir(exist_ok=True)
        fh = logging.FileHandler("logs/app.log")
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        app.addHandler(fh)
        app.setLevel(logging.DEBUG)
        err.addHandler(fh)
        audit.addHandler(fh)
    return {"app": app, "error": err, "audit": audit}