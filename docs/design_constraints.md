# Design Constraints

## Billing pipeline data flow

```
{carrier}_bill  →  shipment_attributes + shipment_charges  →  carrier_cost_ledger
     ↑                        ↑                                       ↑
Insert_ELT_&_CB.sql    Insert_Unified_tables.sql              Load_to_gold.sql
```

All replay and reprocessing logic drives from `billing.shipment_charges` as source of truth. Records that bypass this table are invisible to replays.

---

## shipment_attributes sentinel row

- One permanent row exists with `tracking_number = 'Service_charges'`, `carrier_id = NULL`
- Serves as the FK anchor for all invoice-level (null-tracking) charges in `shipment_charges`
- `carrier_id` on this table is nullable specifically to accommodate this row — do not re-add NOT NULL
- Never delete or update this row

---

## Carrier classification: service charges

| Type | Carriers | Null-tracking charges? |
|---|---|---|
| Direct / invoice-level | UPS, DHL, FedEx, Bukuship, UniUni, Veho | Yes — route to sentinel |
| Aggregator | EasyPost, Eliteworks, FlavorCloud, Passport, Shippo, ShipX, Speedship, USPS Modern | No — filters are intentional |

When adding a new carrier, determine which type it is before deciding whether to apply the `ISNULL(tracking_number, 'Service_charges')` join pattern.

---

## Load_to_gold.sql part responsibilities

| Part | Scope | Tracking filter |
|---|---|---|
| 1 | UPDATE `dbo.shipment` with zone/carrier | Skips null-tracking (explicit guard) |
| 2 | UPDATE `dbo.shipment_package` with dimensions/costs | Skips null-tracking (explicit guard) |
| 3 | INSERT tracked charges into `carrier_cost_ledger` | Skips null-tracking (explicit guard) |
| 4 | INSERT invoice-level charges into `carrier_cost_ledger` | Null-tracking only |

Part 4 always produces `status = 'unknown'`. These rows are permanently excluded from `Backfill_Carrier_Cost_Ledger.sql` because the backfill JOINs (not LEFT JOINs) on `tracking_number`, so null-tracking rows never match.

---

## shipment_charges unique constraint

`UQ_shipment_charges_bill_tracking_charge` on `(carrier_bill_id, tracking_number, charge_type_id)` is the idempotency guard for all ingestion. This covers null-tracking rows because each has a distinct `charge_type_id` per bill. No additional NOT EXISTS guard is needed in `Insert_Unified_tables.sql`.

---

## #FileShipments dedup in Load_to_gold.sql

`ROW_NUMBER() OVER (PARTITION BY sa.tracking_number ...)` is used to resolve WMS tracking number rotation (one tracking number appearing on multiple shipment packages). This partition collapses all NULL tracking_number rows into one, which is why Part 3 must exclude them and Part 4 handles them via a direct `shipment_charges` query.
