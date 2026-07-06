# Shippo — Additional Reference

## Integration Type
Aggregator (carrier_id = 18, is_aggregator = true). Documentation appended to `docs/carriers/aggregators.md`.

## CSV Format
- **Source**: Shippo label/transaction export
- **Columns**: 40 (all VARCHAR in delta table)
- **Row per**: One label purchase (one tracking number)
- **Status values**: `SUCCESS` (label issued) | `ERROR` (label attempt failed, no tracking number)
- **Filter**: Only `status = 'SUCCESS'` rows are processed; ERROR rows have no tracking number and must be excluded

## No Invoice Number
Shippo exports do not contain an invoice number. A synthetic bill number is computed:
`'Shippo_' + CONVERT(VARCHAR(10), MAX(object_created AS DATE), 23)`  
All SUCCESS rows in a file aggregate to one `carrier_bill` record (same pattern as EasyPost).

## Account Number
Column: `rate_carrier_account` = UUID-style Shippo carrier account ID (e.g. `f669d329b627420b8ebb5644d01b2eae`)  
Position: column 18 (1-based) → `Prop_17` in ADF JSON  
Used in `ValidateCarrierInfo.sql`: `WHEN @InputCarrier = 'shippo' THEN JSON_VALUE(@RawJson, '$.Prop_17')`

## Integrated Carrier
Column: `rate_provider` (e.g. `USPS`, `FedEx`). Auto-discovered into `dbo.carrier` via Block 0 in Sync_Reference_Data.sql.

## Shipping Method
Column: `rate_servicelevel_name` (e.g. `Ground Advantage`, `First Class Package International Service`)  
Stored with `integrated_carrier_id` FK from `rate_provider` lookup (same as EasyPost).

## Charge Structure
Single charge per shipment: `rate_amount` → charge name `'Base Rate'` → category Transportation (15), freight=1.  
Seeded once in Sync_Reference_Data.sql Block 2. No dynamic charge types.

## Weight / Dimensions
- `parcel_weight`, `parcel_length/width/height` exist as columns but are empty in current export
- `parcel_mass_unit` and `parcel_distance_unit` also empty
- Dynamic unit conversion coded in Insert_Unified_tables.sql (LB→OZ×16, KG→OZ×35.274; CM→IN÷2.54, MM→IN÷25.4) for future files
- Values will be NULL in `shipment_attributes` for current file

## No Stored Procedure
This is a new integration without a prior stored procedure. Logic follows EasyPost aggregator pattern.
