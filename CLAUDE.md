# ADF Bill Ingestion — Codebase Instructions

## New carrier onboarding — strict 3-stage order

When onboarding a new carrier, run these stages **in order** and do not skip or reorder them:

1. **Setup agent** (`.cursor/agents/setup_agent.md`) — triggered first, on "I want to integrate [Carrier]".
   Creates the `{carrier}_transform/` folder with 6 empty files (`{carrier}_example_bill.csv`, `reference_stored_procedure.sql`, `additional_reference.md`, `Insert_ELT_&_CB.sql`, `Sync_Reference_Data.sql`, `Insert_Unified_tables.sql`), then **stops and waits** for the user to supply the CSV and stored procedure. Do not generate any scripts at this stage.

2. **Design agent** (`.cursor/agents/design_agent.md`) — triggered only after the user has provided the CSV + stored procedure (+ optional additional reference) and confirmed requirements.
   Reads the inputs, applies `.cursor/rules/design-constraints.mdc`, presents an implementation plan and clarifying questions, and **stops and waits for user approval** before generating the 3 transform scripts, a validation test query, and carrier docs under `docs/carriers/`.

3. **add-carrier** (Claude Code skill) — triggered last, only once the design agent's 3 SQL scripts exist and are approved.
   Wires the finished carrier into the ADF pipeline JSON layer (`adf/pipeline/<Carrier>_Transform.json`, `Parent_bill_Ingestor.json`). This lives in a separate ADF Git-integration repo, not in `ADF_bill_ingestion` — confirm/obtain that repo path before running this stage if it hasn't been established yet.

Never jump straight to add-carrier for a brand-new carrier — it assumes the design stage is already done.

## Repo structure

Each `{carrier}_transform/` folder contains two key scripts:
- `Insert_ELT_&_CB.sql` — parses raw Azure Data Lake files into `billing.{carrier}_bill`
- `Insert_Unified_tables.sql` — normalizes carrier bill rows into `billing.shipment_attributes` and `billing.shipment_charges`

`parent_pipeline/` runs after all transforms and handles WMS enrichment and cost ledger population.

## Service charges / invoice-level charges

Some direct carriers (UPS, DHL, FedEx, Bukuship, UniUni, Veho) include account-level charges on their invoices (payment fees, billing adjustments) that have no tracking number. These are legitimate charges that must flow through the pipeline.

**Sentinel row:** `billing.shipment_attributes` contains one permanent row with `tracking_number = 'Service_charges'` and `carrier_id = NULL`. All null-tracking rows in `billing.shipment_charges` point to this row's `id` to satisfy the FK constraint.

**Join pattern** used in `Insert_Unified_tables.sql` for the above carriers:
```sql
ON sa.tracking_number = ISNULL(ub.tracking_number, 'Service_charges')
```

**Aggregator carriers** (EasyPost, Eliteworks, FlavorCloud, Passport, Shippo, ShipX, Speedship, USPS Modern) invoice per-shipment only and never produce account-level charges. Their null-tracking filters in `Insert_ELT_&_CB.sql` are intentional — do not remove them.

**`Load_to_gold.sql` Part 4** handles inserting null-tracking charges into `dbo.carrier_cost_ledger`. These rows always get `status = 'unknown'` — no WMS match is possible. See `docs/service_charges_plan.md` for full implementation details.

## Migration / schema changes

- Always run schema changes through `schema.sql` or the migrations pattern
- `billing.shipment_attributes.carrier_id` is nullable (the sentinel row requires it)
- Do not add a NOT NULL constraint back to that column
