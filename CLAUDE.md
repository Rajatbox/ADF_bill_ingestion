# ADF Bill Ingestion — Codebase Instructions

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
