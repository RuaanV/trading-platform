# AGENTS.md

This file defines repo-specific guidance for coding agents working in this repository.

It is intentionally lightweight. Treat it as a bootstrap document that can be refined as the project and workflows become more opinionated.

## Purpose

This repository is a personal trading platform with:

- Python-based ingestion, feature engineering, training, scoring, and recommendation workflows
- dbt models for warehouse transformations
- Airflow DAG scaffolding for orchestration
- Streamlit dashboard code for presentation
- notebooks for research and experimentation
- an `src/agents/` package for higher-level recommendation and analysis logic

Agents should optimize for correctness, reproducibility, and minimal-risk changes.

## Working Style

- Prefer understanding the existing pipeline before editing code.
- Preserve the current architecture unless the user asks for a structural change.
- Keep changes narrow and task-focused.
- Favor simple, testable implementations over clever abstractions.
- Do not introduce new dependencies unless they are necessary for the task.

## Repo Map

Key locations:

- `data_pipeline/`: ingestion and portfolio-loading scripts
- `dbt/`: dbt project, models, and profiles
- `src/agents/`: agent scaffolding and recommendation entrypoints
- `src/features/`: feature building
- `src/training/`: model training
- `src/scoring/`: scoring workflows
- `src/strategies/`: trade candidate generation
- `src/recommender/`: recommendation generation
- `src/backtesting/`: historical analysis and backtesting
- `src/analytics/`: portfolio analytics and reporting
- `dashboard/`: Streamlit app
- `tests/`: unit tests
- `models/trained_models/`: generated artifacts
- `notebooks/`: research notebooks
- `data/`: fixtures and portfolio data

## Preferred Commands

Default environment:

- Virtualenv path: `.venv`
- Python executable: `.venv/bin/python`

Use the `Makefile` where possible instead of re-creating command sequences manually.

Common commands:

```bash
make setup
make run-pipeline
make run-recommender
make run-agent
make run-dashboard
make dbt-run
make test-recommender
make test-analytics
```

When running Python scripts directly, prefer:

```bash
.venv/bin/python <script.py>
```

## Code Change Guidelines

- Prefer local consistency over introducing a new pattern.
- Keep functions and modules focused on one responsibility.
- Add concise comments only when the code would otherwise be hard to follow.
- Avoid broad refactors unless they are required to complete the task safely.
- If touching recommendation logic, preserve clear rationale fields and artifact compatibility where possible.

## Testing Expectations

Run the smallest relevant verification for the change.

Examples:

- recommender logic changes: `make test-recommender`
- analytics or backtesting changes: `make test-analytics`
- pipeline or integration changes: run the narrowest relevant `make` target first

If full verification is not possible, state what was not run and why.

## Data and Artifact Safety

- Treat files under `models/trained_models/` as generated artifacts unless the user explicitly asks to edit them.
- Avoid deleting artifacts as part of normal task work.
- Be careful with scripts that write to Postgres or refresh portfolio snapshots.
- Do not assume external services or credentials are available unless the task demonstrates that they are configured.

## Agent-Specific Notes

The repository already includes an application-level `src/agents/` package. Do not confuse that with this `AGENTS.md` file.

Use `src/agents/` when the task is about:

- recommendation or portfolio analysis agent behavior
- agent registration or execution
- agent input/output contracts

Use this `AGENTS.md` file as the behavioral guide for the coding agent working in the repo.

## Bootstrap Sections To Expand Later

You can extend this file over time with sections like:

- domain rules for portfolio recommendations
- approved data sources and providers
- coding standards by area
- test matrix by subsystem
- release or deployment workflow
- prompting rules for any future multi-agent orchestration

## Example Future Additions

Possible sections to add when the repo matures:

- "Recommendation Policy"
- "Portfolio Risk Constraints"
- "Data Freshness Requirements"
- "Model Retraining Rules"
- "Dashboard UX Guardrails"
- "Notebook Hygiene"

## Default Assumptions

Unless the user says otherwise:

- prefer minimal code changes
- prefer existing Makefile targets
- prefer unit tests over manual-only validation
- avoid changing database schemas casually
- avoid changing artifact formats without checking downstream usage
