# Bukuship Transform — Mapping Design Reference

## Overview

Bukuship is an **aggregator carrier** (`is_aggregator = true`).  
`@Carrier_id` is resolved at runtime by `ValidateCarrierInfo.sql` — it is the `carrier_id` for the Bukuship aggregator record in `dbo.carrier`.

A single billing file contains charges from two integrated (fulfillment) carriers:

| Integrated Carrier | CarrierName in CSV |
|--------------------|--------------------|
| DHL eCommerce      | `DHL eCommerce`    |
| Landmark Global    | `Landmark Global`  |

Integrated carrier IDs are resolved dynamically from `dbo.carrier` by case-insensitive name match (`LOWER(carrier_name)`), not hardcoded.

Each row in the CSV is a **single charge** for a single shipment (narrow format — no unpivoting needed).

---

## Pipeline Execution Order

```
1. ValidateCarrierInfo.sql      — validate carrier/account exists
2. Insert_ELT_&_CB.sql          — delta → carrier_bill + bukuship_bill
3. Sync_Reference_Data.sql      — bukuship_bill → carrier / shipping_method / charge_types
4. Insert_Unified_tables.sql    — bukuship_bill → shipment_attributes + shipment_charges
```

---

## File → Delta Table Mapping

**Source:** `landmark_example_bill.csv`  
**Target:** `billing.delta_bukuship_bill`  
All columns are loaded as `VARCHAR` (raw, no casting).

| CSV Column              | Delta Column              | Notes |
|-------------------------|---------------------------|-------|
| CompanyName             | CompanyName               | e.g. `Nexiuum` |
| CarrierName             | CarrierName               | `DHL eCommerce` or `Landmark Global` |
| AccountNumber           | AccountNumber             | Invoice grouping key |
| InvoiceDate             | InvoiceDate               | `yyyy-MM-dd` string |
| ShipDate                | ShipDate                  | May be empty |
| DeliveryDate            | DeliveryDate              | May be empty |
| TrackingNumber          | TrackingNumber            | Used as-is for Landmark Global rows |
| WaybillNumber           | WaybillNumber             | Used to construct DHL tracking key |
| ChargeType              | ChargeType                | `Freight Charge` or `Accessorial` |
| ServiceGroup            | ServiceGroup              | May be empty |
| ServiceName             | ServiceName               | Shipping method name |
| ChargeGroup             | ChargeGroup               | May be empty |
| ChargeName              | ChargeName                | Charge classification (see charge types) |
| Zone                    | Zone                      | Destination zone code |
| NetCost                 | NetCost                   | Charge amount (decimal string) |
| PackageWeight           | PackageWeight             | Physical scan weight |
| PackageWeightUnit       | PackageWeightUnit         | `OZ`, `LB` |
| BilledWeight            | BilledWeight              | Billed weight (may differ due to DIM) |
| BilledWeightUnits       | BilledWeightUnits         | `OZ`, `LB` |
| WeightBreak             | WeightBreak               | e.g. `1-5 Lbs`, `< 1 Lb` |
| Length                  | Length                    | Dim length; empty when not DIM billed |
| Width                   | Width                     | Dim width |
| Height                  | Height                    | Dim height |
| DimDivisor              | DimDivisor                | `139` = inches confirmed |
| IsDIM                   | IsDIM                     | `TRUE`/`FALSE` string |
| DIMIncrease             | DIMIncrease               | Weight increase from DIM billing |
| Pieces                  | Pieces                    | Package count |
| BundleNumber            | BundleNumber              |  |
| Payor                   | Payor                     | e.g. `Shipper` |
| SenderName              | SenderName                |  |
| SenderCompany           | SenderCompany             |  |
| SenderAddressLine1      | SenderAddressLine1        |  |
| SenderAddressLine2      | SenderAddressLine2        |  |
| SenderCity              | SenderCity                |  |
| SenderState             | SenderState               |  |
| SenderZipCode           | SenderZipCode             |  |
| SenderCountry           | SenderCountry             |  |
| ReceiverName            | ReceiverName              |  |
| ReceiverCompany         | ReceiverCompany           |  |
| ReceiverAddressLine1    | ReceiverAddressLine1      |  |
| ReceiverAddressLine2    | ReceiverAddressLine2      |  |
| ReceiverCity            | ReceiverCity              |  |
| ReceiverState           | ReceiverState             |  |
| ReceiverZipCode         | ReceiverZipCode           | Also used in DHL tracking key construction |
| ReceiverCountry         | ReceiverCountry           |  |
| Residential             | Residential               | `Y`/`N` or empty |
| ShipmentReference1      | ShipmentReference1        |  |
| ShipmentReference2      | ShipmentReference2        |  |
| Reference1              | Reference1                |  |
| Reference2              | Reference2                |  |
| Reference3              | Reference3                |  |
| CustomsValue            | CustomsValue              | `0` treated as NULL |
| CustomsValueCurrencyCode| CustomsValueCurrencyCode  |  |
| DeliveryConfirmation    | DeliveryConfirmation      |  |
| IntlStatus              | IntlStatus                | `Domestic` or `International` |
| Packaging               | Packaging                 | e.g. `CustomerPackaging` |
| EnteredLength           | EnteredLength             | `0` treated as NULL |
| EnteredWidth            | EnteredWidth              | `0` treated as NULL |
| EnteredHeight           | EnteredHeight             | `0` treated as NULL |

---

## Tracking Number Construction

The resolved tracking number written to `bukuship_bill.tracking_number` depends on `CarrierName`:

| CarrierName match        | tracking_number formula                         | Required source columns |
|--------------------------|-------------------------------------------------|-------------------------|
| LIKE `%dhl%`             | `'420' \|\| ReceiverZipCode \|\| WaybillNumber` | WaybillNumber + ReceiverZipCode (both must be non-empty) |
| All others (Landmark Global) | `TrackingNumber` as-is                      | TrackingNumber must be non-empty |

DHL rows missing WaybillNumber or ReceiverZipCode are **excluded** from bukuship_bill (filtered in Insert_ELT_&_CB.sql).

---

## Delta → carrier_bill Mapping (Step 1 of Insert_ELT_&_CB.sql)

Aggregates charge rows by `(AccountNumber, InvoiceDate)` — one row per invoice.

| carrier_bill column | Source / Formula |
|---------------------|-----------------|
| carrier_id          | `@Carrier_id` (Bukuship aggregator, resolved at runtime) |
| bill_number         | `AccountNumber` |
| bill_date           | `CAST(InvoiceDate AS DATE)` |
| total_amount        | `SUM(CAST(NetCost AS DECIMAL(18,2)))` |
| num_shipments       | `COUNT(DISTINCT resolved_tracking_number)` |
| account_number      | `MAX(AccountNumber)` |
| file_id             | `@File_id` |

**Idempotency:** `WHERE NOT EXISTS (SELECT 1 FROM carrier_bill WHERE file_id = @File_id)`

**Accounts in example file:**

| bill_number | bill_date  | total_amount | num_shipments |
|-------------|------------|-------------|---------------|
| 5125786     | 2026-04-05 | $40,256.06  | 6,278         |
| L2289A      | 2026-04-05 | $2,783.32   | 100           |
| L2297A      | 2026-04-05 | $129.18     | 12            |

---

## Delta → bukuship_bill Mapping (Step 2 of Insert_ELT_&_CB.sql)

One row per charge row from delta. Joins to `carrier_bill` on `(AccountNumber, InvoiceDate, carrier_id)`.

| bukuship_bill column       | Source / Formula |
|----------------------------|-----------------|
| carrier_bill_id            | FK from carrier_bill join |
| company_name               | `CompanyName` |
| carrier_name               | `CarrierName` (integrated carrier name) |
| account_number             | `AccountNumber` |
| invoice_date               | `CAST(InvoiceDate AS DATE)` |
| ship_date                  | `NULLIF(TRIM(ShipDate), '')` — kept as string |
| delivery_date              | `NULLIF(TRIM(DeliveryDate), '')` — kept as string |
| tracking_number            | Constructed (see tracking number logic above) |
| waybill_number             | `NULLIF(TRIM(WaybillNumber), '')` |
| charge_type                | `ChargeType` |
| service_group              | `NULLIF(TRIM(ServiceGroup), '')` |
| service_name               | `NULLIF(TRIM(ServiceName), '')` |
| charge_group               | `NULLIF(TRIM(ChargeGroup), '')` |
| charge_name                | `ChargeName` |
| zone                       | `NULLIF(TRIM(Zone), '')` |
| net_cost                   | `CAST(NetCost AS DECIMAL(18,2))` |
| package_weight             | `CAST(PackageWeight AS DECIMAL(18,6))` — NULL if 0 or empty |
| package_weight_unit        | `NULLIF(TRIM(PackageWeightUnit), '')` |
| billed_weight              | `CAST(BilledWeight AS DECIMAL(18,6))` — NULL if 0 or empty |
| billed_weight_units        | `NULLIF(TRIM(BilledWeightUnits), '')` |
| weight_break               | `NULLIF(TRIM(WeightBreak), '')` |
| length                     | `CAST(Length AS DECIMAL(18,2))` — NULL if 0 or empty |
| width                      | `CAST(Width AS DECIMAL(18,2))` — NULL if 0 or empty |
| height                     | `CAST(Height AS DECIMAL(18,2))` — NULL if 0 or empty |
| dim_divisor                | `NULLIF(TRIM(DimDivisor), '')` |
| is_dim                     | `NULLIF(TRIM(IsDIM), '')` |
| dim_increase               | `NULLIF(TRIM(DIMIncrease), '')` |
| pieces                     | `CAST(Pieces AS INT)` — NULL if empty |
| bundle_number              | `NULLIF(TRIM(BundleNumber), '')` |
| payor                      | `NULLIF(TRIM(Payor), '')` |
| sender_* (6 fields)        | `NULLIF(TRIM(...), '')` for each |
| receiver_* (8 fields)      | `NULLIF(TRIM(...), '')` for each |
| residential                | `NULLIF(TRIM(Residential), '')` |
| shipment_reference1/2      | `NULLIF(TRIM(...), '')` |
| reference1/2/3             | `NULLIF(TRIM(...), '')` |
| customs_value              | `CAST AS DECIMAL(18,2)` — NULL if 0 or empty |
| customs_value_currency_code| `NULLIF(TRIM(...), '')` |
| delivery_confirmation      | `NULLIF(TRIM(...), '')` |
| intl_status                | `NULLIF(TRIM(...), '')` |
| packaging                  | `NULLIF(TRIM(...), '')` |
| entered_length/width/height| `CAST AS DECIMAL(18,2)` — NULL if 0 or empty |

**Idempotency:** `WHERE NOT EXISTS (SELECT 1 FROM bukuship_bill WHERE carrier_bill_id = cb.carrier_bill_id)`

---

## Reference Data Sync (Sync_Reference_Data.sql)

### Block 0 — Auto-Discover Integrated Carriers → `dbo.carrier`

Inserts any new `CarrierName` values from `bukuship_bill` as non-aggregator carriers.  
Case-insensitive NOT EXISTS check prevents duplicates.

| carrier column   | Value |
|------------------|-------|
| carrier_name     | `bukuship_bill.carrier_name` (distinct) |
| is_active        | `1` |
| is_aggregator    | `0` |

Carriers discovered from example file: `DHL eCommerce`, `Landmark Global` — IDs assigned by the database on insert.

### Block 1 — Shipping Methods → `dbo.shipping_method`

Key: `(carrier_id, method_name, integrated_carrier_id)`

| shipping_method column  | Value |
|-------------------------|-------|
| carrier_id              | `@Carrier_id` (Bukuship aggregator, resolved at runtime) |
| method_name             | `bukuship_bill.service_name` |
| service_level           | `'Standard'` (hardcoded) |
| guaranteed_delivery     | `0` / `false` |
| is_active               | `1` / `true` |
| integrated_carrier_id   | Resolved from `carrier` by `LOWER(carrier_name) = LOWER(bukuship_bill.carrier_name)` |

Methods discovered from example file:

| method_name                          | integrated_carrier |
|--------------------------------------|--------------------|
| DHL SmartMail Parcel Expedited       | DHL eCommerce      |
| DHL SmartMail Parcel Expedited Max   | DHL eCommerce      |
| DHL SmartMail Parcel Plus Expedited  | DHL eCommerce      |
| Apple Express Ground                 | Landmark Global    |
| Canada Post Expedited                | Landmark Global    |
| Intelcom Standard                    | Landmark Global    |
| Landmark Intl Standard               | Landmark Global    |

### Block 2 — Charge Types → `dbo.charge_types`

Key: `(carrier_id, charge_name)`

| charge_types column | Value |
|---------------------|-------|
| carrier_id          | `@Carrier_id` (Bukuship aggregator, resolved at runtime) |
| charge_name         | `bukuship_bill.charge_name` (distinct) |
| freight             | `1`/`true` if `LOWER(charge_name) = 'freight charge'`, else `0`/`false` |
| charge_category_id  | Resolved by name lookup against `dbo.charge_category` for the "Other" category |

Charge types discovered from example file:

| charge_name     | is_freight |
|-----------------|-----------|
| Freight Charge  | true      |
| Broker Fee      | false     |
| Disbursement    | false     |
| Fuel            | false     |
| Fuel Surcharge  | false     |
| GST Tax         | false     |
| HST Tax         | false     |
| Minimum Pickup  | false     |

---

## bukuship_bill → shipment_attributes Mapping (Part 1 of Insert_Unified_tables.sql)

One row per distinct `tracking_number`. Canonical source row selected via:

```sql
ROW_NUMBER() OVER (
    PARTITION BY tracking_number
    ORDER BY
        CASE WHEN LOWER(charge_type) = 'freight charge' THEN 0 ELSE 1 END,
        carrier_bill_id
)
```

The `Freight Charge` row is preferred because it carries the canonical weight, zone, and service. Accessorial-only tracking numbers fall back to any available row.

| shipment_attributes column | Source / Formula |
|----------------------------|-----------------|
| carrier_id                 | `@Carrier_id` (Bukuship aggregator, resolved at runtime) |
| shipment_date              | `TRY_CAST(NULLIF(TRIM(ship_date),'') AS TIMESTAMP)` |
| shipping_method            | `service_name` from Freight Charge row |
| destination_zone           | `NULLIF(TRIM(zone), '')` |
| tracking_number            | `tracking_number` (already resolved in bukuship_bill) |
| billed_weight_oz           | `billed_weight` converted to OZ (see weight conversion below) |
| billed_length_in           | `length` (already NULL when 0; DimDivisor=139 confirms inches) |
| billed_width_in            | `width` |
| billed_height_in           | `height` |
| integrated_carrier_id      | Resolved from `carrier` by `LOWER(carrier_name)` |

### Weight Conversion to OZ

| billed_weight_units | formula              |
|---------------------|----------------------|
| `OZ`                | `billed_weight` as-is |
| `LB`                | `billed_weight × 16` |
| `KG`                | `billed_weight × 35.274` |
| other / NULL        | `billed_weight` as-is |

Weight units observed in example file: `OZ` (DHL, 6,278 shipments), `LB` (Landmark Global, 94 shipments, case-insensitive: `LB`, `lb`)

**Idempotency:** `WHERE NOT EXISTS (... WHERE carrier_id = @Carrier_id AND tracking_number = l.tracking_number)`

---

## bukuship_bill → shipment_charges Mapping (Part 2 of Insert_Unified_tables.sql)

One row per `bukuship_bill` charge row (narrow format — no unpivoting).  
Zero and NULL `net_cost` rows are excluded.

| shipment_charges column  | Source / Formula |
|--------------------------|-----------------|
| carrier_id               | `@Carrier_id` (Bukuship aggregator, resolved at runtime) |
| carrier_bill_id          | `bukuship_bill.carrier_bill_id` |
| tracking_number          | `bukuship_bill.tracking_number` |
| charge_type_id           | Looked up from `charge_types` on `(carrier_id, charge_name)` |
| amount                   | `bukuship_bill.net_cost` |
| shipment_attribute_id    | Looked up from `shipment_attributes` on `(carrier_id, tracking_number)` |

**Idempotency:** `WHERE NOT EXISTS (... WHERE carrier_bill_id = ... AND tracking_number = ... AND charge_type_id = ...)`

---

## Key Design Constraints Applied

| # | Constraint | Implementation |
|---|-----------|---------------|
| 2 | Transaction wraps carrier_bill + bukuship_bill inserts | `BEGIN TRANSACTION` in Insert_ELT_&_CB.sql |
| 3 | Fail-fast casting — no TRY_CAST in production scripts | Direct `CAST(x AS DECIMAL)` — invalid data raises error |
| 4 | Idempotency via NOT EXISTS | All four scripts use NOT EXISTS guards |
| 5 | Business key: `(carrier_id, tracking_number)` | UNIQUE INDEX on shipment_attributes |
| 7 | Weight stored in OZ in shipment_attributes | LB×16, KG×35.274 applied in Insert_Unified_tables.sql |
| 8 | Scripts return Status + row counts | SELECT at end of each TRY block |
| 9 | Line-item NOT EXISTS uses carrier_bill_id only | Prevents re-insert of all rows on re-run |
| 10 | Narrow format — one bukuship_bill row = one charge | No CROSS APPLY / unpivot needed |
| 11 | charge_category_id = "Other" for all charge types | Resolved by name lookup against `dbo.charge_category` in Sync_Reference_Data.sql Block 2 |
| 12 | file_id stored in carrier_bill; all joins filter by file_id | Enables file-based idempotency and selective retry |

---

## Sum Validation (example file)

| Layer | Total |
|-------|-------|
| delta_bukuship_bill (valid rows) | $43,168.56 |
| bukuship_bill.net_cost | $43,168.56 |
| carrier_bill.total_amount (sum) | $43,168.56 |
| shipment_charges.amount (sum) | $43,168.56 |

Per-account breakdown:

| Account | Total | Shipments |
|---------|-------|-----------|
| 5125786 (DHL eCommerce) | $40,256.06 | 6,278 |
| L2289A (Landmark Global) | $2,783.32 | 100 |
| L2297A (Landmark Global) | $129.18 | 12 |

Per-charge-type breakdown:

| Charge Name    | is_freight | Total      | Count |
|----------------|-----------|-----------|-------|
| Freight Charge | true       | $38,987.81 | 6,372 |
| Broker Fee     | false      | $1,587.50  | 2,177 |
| HST Tax        | false      | $1,138.23  | 37    |
| GST Tax        | false      | $606.40    | 61    |
| Fuel           | false      | $540.96    | 6,278 |
| Minimum Pickup | false      | $159.91    | 8     |
| Fuel Surcharge | false      | $78.84     | 94    |
| Disbursement   | false      | $68.91     | 1     |

---

## Script Implementation Logic

### Global Conventions (All Scripts)

| Convention | Rule |
|-----------|------|
| SQL reserved keywords | Wrapped in `[]` (e.g., `[zone]`, `[length]`, `[width]`, `[height]`, `[service_name]`, `[sender_state]`, `[receiver_state]`, `[Status]`) |
| String cleaning | `NULLIF(TRIM(col), '')` — converts whitespace-only strings to NULL |
| Zero-value numerics | `CAST(...) <> 0` check after NULLIF/TRIM — numeric columns storing `'0'` become NULL |
| Fail-fast casting | Direct `CAST(x AS DECIMAL)` — no `TRY_CAST`; invalid data raises an error immediately |
| Idempotency | `NOT EXISTS` guards on every INSERT; scripts are safe to re-run |
| Output format | Every script ends with a `SELECT 'SUCCESS' AS [Status], ...` or `SELECT 'ERROR' AS [Status], ...` result set |
| Error handling | `BEGIN TRY / BEGIN CATCH` with `THROW 50000, @DetailedError, 1` to propagate to ADF |
| ADF variables | `@Carrier_id` (INT — Bukuship aggregator ID, resolved by `ValidateCarrierInfo.sql`), `@File_id` (INT from parent pipeline) |
| `SET NOCOUNT ON` | Applied in all scripts to suppress row-count messages |
| `SET XACT_ABORT ON` | Applied only in transactional script (`Insert_ELT_&_CB.sql`) |

---

### Script 1: Insert_ELT_&_CB.sql

**Purpose:** Two-step transactional insert from `delta_bukuship_bill` into `carrier_bill` (invoice summaries) and `bukuship_bill` (charge-level line items).

**Transaction:** `BEGIN TRANSACTION` wraps both INSERTs; `ROLLBACK` on any error. Only script in the pipeline that uses a transaction (Design Constraint #2).

#### Step 1 — carrier_bill (invoice aggregation)

- Groups `delta_bukuship_bill` by `(AccountNumber, InvoiceDate)` — one row per invoice.
- `total_amount` = `SUM(CAST(NetCost AS DECIMAL(18,2)))`.
- `num_shipments` = `COUNT(DISTINCT resolved_tracking_number)` — DHL uses constructed key, others use `TrackingNumber`.
- **Idempotency key:** `HAVING NOT EXISTS (SELECT 1 FROM carrier_bill WHERE file_id = @File_id)` — entire group skipped if file already processed.
- **Row filter:** `AccountNumber`, `InvoiceDate`, `NetCost` must be non-NULL and non-empty.

#### Step 2 — bukuship_bill (charge-level)

- One row per charge row from `delta_bukuship_bill` (narrow format — no unpivoting).
- JOINs to `carrier_bill` on `(bill_number = AccountNumber, bill_date = CAST(InvoiceDate AS DATE), carrier_id = @Carrier_id)` to resolve `carrier_bill_id`.
- **Tracking number logic:**
  - `LOWER(CarrierName) LIKE '%dhl%'` → `'420' + ReceiverZipCode + WaybillNumber`
  - All others (Landmark Global) → `TrackingNumber` as-is
- **DHL row filter:** must have both non-empty `WaybillNumber` AND `ReceiverZipCode` to construct the key; rows missing either are excluded.
- **Idempotency key:** `NOT EXISTS WHERE carrier_bill_id = cb.carrier_bill_id` (Design Constraint #9 — carrier_bill_id only, not per-row tracking key).
- **Dimension columns** (`Length`, `Width`, `Height`, `EnteredLength/Width/Height`): stored as NULL when value is `''` or `'0'`.
- **Weight columns** (`PackageWeight`, `BilledWeight`): stored as NULL when value is `''` or `'0'`.
- **CustomsValue**: same NULL-on-zero logic.
- **Pieces**: `CAST AS INT`, NULL if empty.

---

### Script 2: Sync_Reference_Data.sql

**Purpose:** Auto-discover and insert new reference/lookup values from processed `bukuship_bill` data. Three independent idempotent blocks — no transaction.

**File-based filter:** All blocks JOIN `bukuship_bill → carrier_bill` and filter `WHERE cb.file_id = @File_id`.

#### Block 0 — Integrated Carriers (`dbo.carrier`)

- Finds distinct `carrier_name` values in `bukuship_bill` not already in `dbo.carrier`.
- Case-insensitive NOT EXISTS: `LOWER(c.carrier_name) = LOWER(l.carrier_name)` prevents "DHL eCommerce" vs "dhl ecommerce" duplicates.
- Inserts with `is_active = 1`, `is_aggregator = 0`.
- **Must run before Block 1** — Block 1 needs `carrier_id` FK from this table.

#### Block 1 — Shipping Methods (`dbo.shipping_method`)

- Finds distinct `(service_name, carrier_name)` combinations.
- `carrier_id` = `@Carrier_id` (Bukuship aggregator, resolved at runtime).
- `integrated_carrier_id` resolved via `LEFT JOIN dbo.carrier ON LOWER(carrier_name) = LOWER(l.carrier_name)`.
- Idempotency key: `(carrier_id, method_name, integrated_carrier_id)` — handles NULL integrated_carrier_id with an `OR IS NULL` check.
- Hardcoded: `service_level = 'Standard'`, `guaranteed_delivery = 0`, `is_active = 1`.

#### Block 2 — Charge Types (`dbo.charge_types`)

- Finds distinct `charge_name` values.
- `carrier_id` = `@Carrier_id`.
- `is_freight = 1` only when `LOWER(charge_name) = 'freight charge'`; all others `= 0`.
- `charge_category_id` resolved by name lookup for the "Other" category in `dbo.charge_category` (Design Constraint #11).
- Idempotency key: `(carrier_id, charge_name)`.

---

### Script 3: Insert_Unified_tables.sql

**Purpose:** Two-part idempotent population of unified billing tables from `bukuship_bill`. No transaction — each INSERT is independently idempotent.

**File-based filter:** Both parts JOIN `bukuship_bill → carrier_bill` and filter `WHERE cb.file_id = @File_id`.

#### Part 1 — shipment_attributes (one row per tracking number)

- **Canonical row selection:** `ROW_NUMBER() OVER (PARTITION BY tracking_number ORDER BY CASE WHEN LOWER(charge_type) = 'freight charge' THEN 0 ELSE 1 END, carrier_bill_id)` — selects the Freight Charge row first (it carries canonical weight, zone, service); falls back to any row for accessorial-only tracking numbers.
- Only rows where `rn = 1` are inserted.
- `integrated_carrier_id` resolved via `LEFT JOIN dbo.carrier ON LOWER(carrier_name) = LOWER(l.carrier_name)`.
- **Weight conversion to OZ** (Design Constraint #7):
  - `UPPER(billed_weight_units) = 'OZ'` → store as-is
  - `UPPER(billed_weight_units) = 'LB'` → `billed_weight * 16`
  - `UPPER(billed_weight_units) = 'KG'` → `billed_weight * 35.274`
  - Other/NULL → store as-is
- **Dimensions** (`length`, `width`, `height`): already NULL when 0 (set in Script 1); stored as-is (inches confirmed by `DimDivisor = 139`).
- `shipment_date` = `NULLIF(TRIM(ship_date), '')` — kept as string, no date cast.
- **Idempotency key:** `NOT EXISTS WHERE carrier_id = @Carrier_id AND tracking_number = l.tracking_number`.

#### Part 2 — shipment_charges (one row per charge)

- Narrow format — each `bukuship_bill` row = one charge, no unpivoting.
- `charge_type_id` looked up via `INNER JOIN dbo.charge_types ON (charge_name, carrier_id)`.
- `shipment_attribute_id` looked up via `INNER JOIN billing.shipment_attributes ON (carrier_id, tracking_number)`.
- **Exclusion rule:** rows where `net_cost IS NULL` or `net_cost = 0` are skipped.
- **Idempotency key:** `NOT EXISTS WHERE (carrier_bill_id, tracking_number, charge_type_id)`.

---

### Reserved Keywords Wrapped in `[]`

The following column names are T-SQL reserved keywords and must be bracketed wherever they appear in DDL or DML:

| Column | Appears In |
|--------|-----------|
| `[zone]` | `bukuship_bill` INSERT list, SELECT list |
| `[length]` | `bukuship_bill` INSERT list, SELECT list |
| `[width]` | `bukuship_bill` INSERT list, SELECT list |
| `[height]` | `bukuship_bill` INSERT list, SELECT list |
| `[service_name]` | `bukuship_bill` INSERT list |
| `[sender_state]` | `bukuship_bill` INSERT list |
| `[receiver_state]` | `bukuship_bill` INSERT list |
| `[Status]` | Output SELECT in all scripts |
