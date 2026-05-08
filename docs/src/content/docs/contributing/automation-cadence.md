---
title: Automation cadence
description: How OmicIDX uses Dagster declarative automation, and the cron+deps_updated pattern that prevents runaway cascades.
---

OmicIDX uses Dagster's [declarative automation](https://docs.dagster.io/concepts/automation/declarative-automation) to drive the daily refresh. Most assets cascade automatically when their upstream lands. A few are deliberately gated to prevent high-frequency upstreams from triggering full-graph rebuilds.

This page documents the pattern. If you're adding a new asset, this is the rubric for choosing its automation condition.

## The default: `eager()`

Most consolidation and Postgres-load assets use:

```python
automation_condition=dg.AutomationCondition.eager()
```

Translation: "materialize me whenever any upstream updates." For an asset whose upstream is daily, that means once per day. The cascade is automatic; no schedule needed.

## The exception: cron-paced cascade

PubMed lands hourly via a file sensor. If `pubmed_parquet`, `pubmed_postgres`, and `omicidx_duckdb` were eager, every new PubMed file would trigger:

1. A full PubMed Parquet rebuild (gigabytes of data, every hour).
2. A full PubMed Postgres A/B reload (every hour).
3. A full DuckDB rebuild + R2 upload (every hour).

That's wasteful and expensive. The fix is to gate those three assets to once-daily, only firing when an upstream actually has new content:

```python
automation_condition=(
    dg.AutomationCondition.on_cron("0 3 * * *")
    & dg.AutomationCondition.any_deps_updated()
)
```

Translation: "fire at 3:00 UTC, but only if any upstream has been updated since the last cron tick."

## When to use which

| Use `eager()` | Use `on_cron(...) & any_deps_updated()` |
| ------------- | --------------------------------------- |
| Upstream is **daily or slower** | Upstream is **hourly or faster** |
| Asset is **cheap to rebuild** | Asset is **expensive to rebuild** |
| You want immediate propagation | You want a fixed daily wall-clock cadence |
| The asset has **few upstreams** | The asset has **many upstreams** that update at varying times |

## The staggered times

Three assets currently use the cron-paced pattern, with staggered times so each downstream finds its dep settled:

| Asset             | Cron       | Why this slot                                |
| ----------------- | ---------- | -------------------------------------------- |
| `pubmed_parquet`  | `0 3 * * *` | After PubMed sensor's overnight runs land.   |
| `pubmed_postgres` | `0 4 * * *` | One hour after `pubmed_parquet`.             |
| `omicidx_duckdb`  | `0 5 * * *` | After the entire consolidation cascade has had time to settle. |

If you add an asset that depends on one of these, give it a slot **after** its upstream's slot.

## The automation sensor

The cascade is driven by an `AutomationConditionSensorDefinition` registered with `default_status=DefaultSensorStatus.RUNNING` so it's active immediately on deployment without manual enable steps. See [`packages/omicidx-dagster/src/omicidx/dagster/definitions.py`](https://github.com/omicidx/omicidx/blob/main/packages/omicidx-dagster/src/omicidx/dagster/definitions.py).

## Background

The cron+deps_updated pattern was adopted in [PR #70](https://github.com/omicidx/omicidx/pull/70) after a Copilot review caught the eager-cascade-from-hourly-upstream issue. The follow-up issue [#73](https://github.com/omicidx/omicidx/issues/73) tracks concurrent-run safety for `omicidx_duckdb`'s fixed output path.
