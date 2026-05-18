# Unknown Shipment Resolution Plan — Falcon

This document tracks the three phases required to resolve unknown carrier bill shipments for Falcon.

---

## Phase 1: Carrier Bill Ingestion

Promote fields from the bronze delta tables into the silver carrier-specific bill tables so they are available for resolution logic.

### billing.dhl_bill
Add the following columns (sourced from `billing.delta_dhl_bill`):

| New Column | Source Column | Purpose |
|---|---|---|
| `billing_ref1` | `delta_dhl_bill.billing_ref1` | Service type flag — identifies return programs (e.g. `QUALITY RETURNS`) |
| `bol_number` | `delta_dhl_bill.bol_number` | Return BOL number — identifies customer-specific return programs |

### billing.fedex_bill
Add the following column (sourced from `billing.delta_fedex_bill`):

| New Column | Source Column | Purpose |
|---|---|---|
| `original_customer_reference` | `delta_fedex_bill.[Original Customer Reference]` | Numeric reference stamped on FedEx label at ship time — used to match back to the originating order shipment |

### Files to change
- `dhl_transform/Insert_ELT_&_CB.sql` — add `billing_ref1`, `bol_number` to Step 2 INSERT
- `fedex_transform/Insert_ELT_&_CB.sql` — add `original_customer_reference` to Step 2 INSERT

### New pipeline step
Add `parent_pipeline/Resolve_Unknown_Shipments.sql` to run after `Load_to_gold.sql`:
- DHL: match `carrier_cost_ledger` unknowns back to `dhl_bill` via `carrier_bill_id`, resolve using `billing_ref1` and `bol_number`
- FedEx: match `carrier_cost_ledger` unknowns back to `fedex_bill` via `carrier_bill_id`, resolve using `original_customer_reference` → `IntegrationDatabase.dbo.OrderShipment` (see Phase 3)

---

## Phase 2: WMS Ingestion

Pull the following from the WMS (ShipStream) to resolve unknowns that cannot be matched via standard tracking number lookup.

### What is needed

| WMS Table | Column | Purpose |
|---|---|---|
| `sales_flat_shipment_package` | `alt_track_number` | Manually packed shipments store tracking here instead of the standard field — carrier bill has the real tracking, WMS has it in the alt field |
| `delivery_track` | `track_number` | Return label tracking for RMA deliveries — needed for suffix matching (`RIGHT(track_number, 12)` for general RMAs, `RIGHT(track_number, 18)` for UPS returns) where `delivery.shipment_id` is null and no corresponding `shipment_package` record exists |

### Open question
Confirm what percentage of return-type `delivery` records in Falcon have a null `shipment_id`. If the number is negligible, `delivery_track` ingestion may not be required and the match can be done via existing `shipment_package` data.

### Target
To be determined — new staging table(s) under `billing` schema or a dedicated `wms` schema.

---

## Phase 3: IntegrationDatabase Ingestion

Ingest `IntegrationDatabase.dbo.OrderShipment` to enable FedEx Smart Post resolution.

### Why
`billing.fedex_bill.original_customer_reference` contains the `OrderShipmentId` from the tenant's IntegrationDatabase. This is not currently ingested into Falcon. Without it, FedEx Smart Post unknowns cannot be resolved.

### Data Discovery First

Before scoping the ingestion, compare `IntegrationDatabase.dbo.OrderShipment` columns against Falcon's existing tables to determine:
- Whether `MerchantId` in `OrderShipment` maps to anything already in Falcon (e.g. `dbo.3pl_customer`) — if it does, we do not need to ingest `Merchant` separately
- Whether `OrderKey` maps to `dbo.order.order_number` or `dbo.order.external_id`
- Whether `WarehouseId` maps to `dbo.warehouse.warehouse_id`

This discovery determines how much of the resolution can be done using existing Falcon data vs what additional lookups are needed.

### What is needed (pending discovery)

| Source Table | Columns needed | Purpose |
|---|---|---|
| `IntegrationDatabase.dbo.OrderShipment` | `OrderShipmentId`, `OrderKey`, `MerchantId`, `WarehouseId` | Join `original_customer_reference` → `OrderShipmentId` to resolve the originating order shipment |

### Target
New ingestion pipeline and staging table(s) to be scoped after data discovery.
