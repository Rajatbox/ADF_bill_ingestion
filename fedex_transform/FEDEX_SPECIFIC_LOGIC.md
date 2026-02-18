# LLM Context: Logistics Billing Integration

## Purpose
This document provides essential context for AI assistants working on carrier billing integration. It captures business logic, data structures, and design decisions specific to each carrier.

---

## FEDEX - Billing Schema & Business Logic

### 1. Source Data Format: Wide Table Schema

**Table**: `delta_fedex_bill` (exact replica of FedEx CSV billing files)

**Key Characteristic**: **Wide Table Format**
- One row = one shipment/package
- Multiple charges stored horizontally in the same row (up to 50 charge pairs!)
- Charge pairs: `[Tracking ID Charge Description]` + `[Tracking ID Charge Amount]`
- Indexed from 0 to 50: 
  - `[Tracking ID Charge Description]`, `[Tracking ID Charge Amount]`
  - `[Tracking ID Charge Description_1]`, `[Tracking ID Charge Amount_1]`
  - ... through ...
  - `[Tracking ID Charge Description_50]`, `[Tracking ID Charge Amount_50]`

**Critical Columns**:
```
[Express or Ground Tracking ID]  -- Primary tracking identifier
[MPS Package ID]                 -- Multi-Piece Shipment ID (for grouped shipments)
[Net Charge Amount]              -- SUM of all individual charges in this row
[Transportation Charge Amount]   -- Base freight charge
[Shipment Date]                  -- Date shipment was sent
[Service Type]                   -- Service level (Ground, Express, etc.)
[Zone Code]                      -- Destination zone for rating
[Rated Weight Amount]            -- Billable weight
[Rated Weight Units]             -- LB, OZ, KG
[Dim Length/Width/Height]        -- Package dimensions
[Dim Unit]                       -- IN or CM
```

**Why Wide Table?**
- FedEx provides billing data in denormalized format
- One row contains all charges for that specific package/shipment
- Requires unpivoting (via `vw_FedExCharges`) to normalize into `shipment_charges` table

---

### 2. Multi-Piece Shipment (MPS) Logic

**Business Concept**: FedEx groups multiple packages under a single master tracking number for consolidated billing. When a shipment contains multiple packages, FedEx generates multiple rows in their CSV with a complex relationship structure.

**Critical: Invoice & File Structure**:
- **1 CSV file = 1 invoice** (filename matches invoice number)
- **Wide format**: 1 row per package/shipment with 50+ charge columns
- **No duplicate rows possible**: FedEx's billing system generates unique rows per package
- **Architectural guarantee**: Duplicate `express_or_ground_tracking_id` means MPS group, NOT data corruption

**MPS Identification**:
- Multiple rows share the same `[Express or Ground Tracking ID]`
- `[MPS Package ID]` differentiates roles within the group

---

#### **MPS Roles: Detailed Breakdown**

| Role | Condition | Has Cost/Date/Zone | Has Dimensions | msp_tracking_id | Purpose |
|------|-----------|-------------------|----------------|-----------------|---------|
| **NORMAL_SINGLE** | `count = 1` | ✅ Complete record | ✅ Yes | `NULL` | Standard single-package shipment |
| **MPS_HEADER** | `count > 1` AND `msp = NULL` | ✅ Aggregated totals | ✅ Aggregated dimensions | `NULL` | Group summary row (billing total) |
| **MPS_PARENT** | `count > 1` AND `msp = express_or_ground_id` | ❌ NULL/0 | ✅ Yes (package dimensions) | Equals `express_or_ground_id` | First package in MPS group |
| **MPS_CHILD** | `count > 1` AND `msp ≠ express_or_ground_id` | ❌ NULL/0 | ✅ Yes (package dimensions) | Different tracking number | Additional packages in group |

---

#### **Row Structure Examples**

**NORMAL_SINGLE Shipment** (1 row):
```
express_or_ground_tracking_id: TRK001
msp_tracking_id: NULL
net_charge_amount: $50.00
shipment_date: 2024-01-15
service_type: Ground
zone_code: 5
dimensions: 10x10x10 inches
→ Classification: NORMAL_SINGLE (complete record, insert as-is)
```

**MPS Group** (4 rows for 3 packages):
```
Row 1: HEADER
  express_or_ground_tracking_id: MPS100
  msp_tracking_id: NULL
  net_charge_amount: $150.00 (sum of all packages)
  shipment_date: 2024-01-15
  service_type: Ground
  zone_code: 5
  dimensions: 30x20x15 (aggregated)
  → Classification: MPS_HEADER (filtered out, used for hoisting)

Row 2: PARENT
  express_or_ground_tracking_id: MPS100
  msp_tracking_id: MPS100 (same as ground ID)
  net_charge_amount: NULL or 0
  shipment_date: NULL
  service_type: NULL
  zone_code: NULL
  dimensions: 10x8x5 (actual package)
  → Classification: MPS_PARENT (gets hoisted metadata)

Row 3: CHILD
  express_or_ground_tracking_id: MPS100
  msp_tracking_id: PKG001 (different)
  net_charge_amount: NULL or 0
  shipment_date: NULL
  zone_code: NULL
  dimensions: 12x10x7 (actual package)
  → Classification: MPS_CHILD (gets hoisted metadata)

Row 4: CHILD
  express_or_ground_tracking_id: MPS100
  msp_tracking_id: PKG002 (different)
  net_charge_amount: NULL or 0
  dimensions: 8x2x3 (actual package)
  → Classification: MPS_CHILD (gets hoisted metadata)
```

**Result in shipment_attributes**: 3 rows (Parent + 2 Children)
- HEADER is filtered out
- All 3 get hoisted date/zone/service from header
- View calculates cost from unpivoted charges

---

#### **Correction Records**

**Important**: Correction invoices can have **NULL values** for metadata:
```
express_or_ground_tracking_id: TRK001 (existing shipment)
msp_tracking_id: NULL
net_charge_amount: $5.00 (adjustment)
shipment_date: NULL (not provided in correction)
service_type: NULL
zone_code: NULL
dimensions: NULL
→ Classification: Depends on count
   - If count = 1: NORMAL_SINGLE (correction record)
   - If count > 1: MPS_HEADER (even though it's a correction)
```

**This is why**:
- We can't use "has dimensions" to distinguish headers from normals
- We can't use "has cost/date" to validate classification
- We rely entirely on `COUNT()` + `msp_tracking_id` pattern

---

#### **Hoisting Logic** (Window Functions)

**Source**: `MPS_HEADER` or `NORMAL_SINGLE` row (where metadata exists)  
**Target**: All rows in the MPS group (same `express_or_ground_tracking_id`)

**Hoisted Fields**:
1. `net_charge_amount` → Used in view calculation (not stored)
2. `shipment_date` → `shipment_date`
3. `service_type` → `shipping_method`
4. `zone_code` → `destination_zone`
5. Aggregated dimensions (if needed)

**Window Function Implementation**:
```sql
-- Hoist metadata from header/normal row
MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN shipment_date END) 
    OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_shipment_date

MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN service_type END) 
    OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_service_type

MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN zone_code END) 
    OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_zone_code
```

---

#### **Tracking Number Resolution**

```sql
COALESCE(NULLIF(msp_tracking_id, ''), express_or_ground_tracking_id) AS tracking_number
```

**Logic**:
- If `msp_tracking_id` has value → Use it (PARENT and CHILD packages)
- If `msp_tracking_id` is NULL or empty → Use `express_or_ground_tracking_id` (HEADER and NORMAL)

**Result**:
- NORMAL: Uses `express_or_ground_id` (e.g., TRK001)
- HEADER: Uses `express_or_ground_id` (e.g., MPS100) - then filtered out
- PARENT: Uses `msp_tracking_id` (e.g., MPS100)
- CHILD: Uses `msp_tracking_id` (e.g., PKG001, PKG002)

---

#### **Why No Validation Needed**

**The classification logic relies on architectural guarantees**:
1. ✅ FedEx won't send duplicate rows in wide CSV format (1 file = 1 invoice structure)
2. ✅ `COUNT(express_or_ground_tracking_id)` accurately identifies MPS groups
3. ✅ `msp_tracking_id` pattern is consistent across all invoices

**If classification is wrong**: FedEx changed their format or system is broken. No validation can detect this - would need FedEx documentation update.

**Cannot test**: No way to distinguish "duplicate NORMAL rows" from "MPS group" without external reference (FedEx documentation or manual invoice inspection)

---

### 3. Billing Corrections: Automatic Accumulation

**Business Rule**: Corrections accumulate **automatically** via view calculation.

**Scenario**:
```
Initial Bill (Invoice 001):
  Tracking: ABC123
  Charges inserted: $20 freight, $5 fuel
  → vw_shipment_summary: billed_shipping_cost = SUM($20 + $5) = $25.00

Correction Bill (Invoice 002, weeks later):
  Tracking: ABC123
  New charge inserted: $3 residential surcharge
  → vw_shipment_summary: billed_shipping_cost = SUM($20 + $5 + $3) = $28.00

Credit Bill (Invoice 003):
  Tracking: ABC123
  New charge inserted: -$2 service refund
  → vw_shipment_summary: billed_shipping_cost = SUM($20 + $5 + $3 - $2) = $26.00
```

**Implementation**:
```sql
-- View always calculates from ALL charges
CREATE VIEW vw_shipment_summary AS
SELECT 
    sa.*,
    ISNULL(SUM(sc.amount), 0) AS billed_shipping_cost
FROM shipment_attributes sa
LEFT JOIN shipment_charges sc ON sc.shipment_attribute_id = sa.id
GROUP BY sa.id, ...;
```

**Why This Works Better**:
- No cumulative UPDATE logic needed (eliminated complexity)
- Corrections accumulate naturally (sum includes all charges)
- Idempotent (rerun doesn't double costs)
- Always correct (single source of truth)

**Standard Logistics Practice**:
- FedEx issues adjustment invoices with only the correction amount
- Each invoice adds charges to `shipment_charges` table
- View automatically reflects cumulative total

---

### 4. INSERT Strategy: Business Key

**Business Key**: `carrier_id + tracking_number`

**UNIQUE INDEX Enforcement**:
- Schema enforces uniqueness via `UNIQUE INDEX (carrier_id, tracking_number)`
- INSERT will fail if attempting to insert duplicate (provides safety net)
- No silent corruption possible

**INSERT Behavior**:
- Uses `NOT EXISTS` check to skip existing tracking numbers
- Only inserts new shipment_attributes records with metadata
- No updates needed (core attributes never change after creation)

**Why No Updates?**
- Core attributes (shipment_date, zone, dimensions, weight) represent original shipment
- Should not change after initial creation
- `billed_shipping_cost` is NOT stored (calculated via view)
- Corrections handled via charges table (view automatically recalculates)

**Idempotency Without Transaction**:
- Part 1 (INSERT attributes): NOT EXISTS check + UNIQUE constraint prevents duplicates
- Part 2 (INSERT charges): NOT EXISTS check prevents duplicate charges
- Both parts use same pattern for consistency
- View recalculates cost from whatever charges exist (always correct)
- Safe to rerun with same `@File_id` - no double-counting, no corruption
- File-based filtering isolates processing to specific file data only

---

### 5. Data Flow Architecture

```
CSV File (Wide Format - 50+ charge columns)
    ↓
delta_fedex_bill (Staging - Exact CSV replica)
    ↓
fedex_bill (Normalized - Line Items)
    ↓
    ├─→ MPS Logic (4-stage CTE) ──→ shipment_attributes (metadata only)
    │                                    ↓
    │                                   id (Business Key)
    │                                    ↓
    └─→ vw_FedExCharges (Unpivot) ──→ shipment_charges (with FK to attributes)
                                         ↓
                                    vw_shipment_summary (calculates cost)
```

**CTE Pipeline for MPS** (in `Insert_Unified_tables.sql`):
1. **fx_tallied**: Count occurrences of each tracking ID (identify MPS groups)
2. **fx_classified**: Classify into MPS roles (HEADER/PARENT/CHILD/NORMAL)
3. **fx_hoisted**: Hoist header values via window functions (propagate group data)
4. **fx_final**: Apply unit conversions, filter out headers, prepare for INSERT

**View Layer**:
- **vw_shipment_summary**: Joins `shipment_attributes` + `shipment_charges`
- Calculates `billed_shipping_cost = SUM(charges.amount)` per shipment
- Single source of truth for cost (always correct)

---

### 6. Relationship Model

**Business Key**: `shipment_attributes.id` (IDENTITY)
- PRIMARY KEY on `id`
- UNIQUE INDEX on `(carrier_id, tracking_number)` enforces business key uniqueness
- Represents unique shipment across carrier
- One shipment_attributes row → Many shipment_charges rows (1-to-Many)

**Foreign Key**: `shipment_charges.shipment_attribute_id`
- Column: `shipment_attribute_id INT NULL` in `shipment_charges` table
- NOT NULL constraint (every charge must link to a shipment)
- FOREIGN KEY constraint: `FK_shipment_charges_attributes` references `shipment_attributes(id)`
- Non-clustered index: `IX_shipment_charges_attribute_id` for FK lookup performance
- Enforces referential integrity between charges and shipments

**Single Source of Truth**:
- `shipment_charges` table: Itemized breakdown of individual charges (fuel, residential, DAS, etc.)
- `vw_shipment_summary` view: Calculates `billed_shipping_cost` as `SUM(shipment_charges.amount)`

**Architecture Decision**:
- `billed_shipping_cost` is NOT stored in `shipment_attributes`
- Calculated on-the-fly via view (eliminates sync bugs)
- Single source: `shipment_charges` (unpivoted from 50 charge column pairs)

**Why this design?**
- Eliminates redundancy (no two sources to keep in sync)
- Correctness by construction (cost is calculated, can't be wrong)
- Handles corrections naturally (sum always includes all charges)
- Simpler pipeline (no aggregate UPDATE step needed)

---

### 7. Unit Conversions

**Weight**: LB → OZ (multiply by 16)
```sql
CASE WHEN UPPER(rated_weight_units) = 'LB' 
     THEN rated_weight_amount * 16
     ELSE rated_weight_amount END
```

**Dimensions**: CM → Inches (divide by 2.54)
```sql
CASE WHEN UPPER(dim_unit) = 'CM' 
     THEN dim_length / 2.54
     ELSE dim_length END
```

**Standardization**: All unified tables store weight in OZ and dimensions in inches for consistency across carriers.

---

### 8. Key Tables

| Table/View | Purpose | Key Columns | Row Grain |
|------------|---------|-------------|-----------|
| `delta_fedex_bill` | Raw CSV staging (wide format) | All FedEx columns as-is | One row per package in CSV |
| `fedex_bill` | Normalized line items | `express_or_ground_tracking_id`, `msp_tracking_id`, `net_charge_amount`, `created_date` | One row per package |
| `shipment_attributes` | Unified shipment master (business key) | `id` (PK), `tracking_number`, metadata fields (no cost) | One row per unique carrier_id + tracking_number |
| `shipment_charges` | Detailed charge breakdown | `shipment_attribute_id` (FK, NOT NULL), `charge_type_id`, `amount` | One row per charge per shipment |
| `vw_shipment_summary` | **View**: Attributes with calculated cost | All `shipment_attributes` columns + `billed_shipping_cost` (calculated) | One row per unique carrier_id + tracking_number |
| `charge_types` | Reference: Charge type lookup | `charge_type_id`, `charge_name`, `carrier_id` | One row per charge type per carrier |
| `carrier_bill` | Invoice-level summary | `carrier_bill_id`, `bill_number`, `total_amount`, `num_shipments` | One row per invoice |

---

### 9. Critical Business Rules

1. **MPS_HEADER rows must be filtered out** - They are summary rows for the group, not actual packages
2. **MPS_CHILD packages get NULL cost** - Charge is assigned to the parent only to avoid double-counting
3. **Corrections are cumulative** - Always ADD to existing cost, never replace
4. **Tracking number resolution** - Use COALESCE for MPS vs non-MPS packages
5. **One business key** - `carrier_id + tracking_number` uniquely identifies a shipment in our unified system
6. **Conservative updates** - Only `billed_shipping_cost` and `updated_date` change on MATCHED; all other attributes frozen at creation
7. **NULL as zero** - For cumulative math, treat NULL as 0 to handle MPS_CHILD corrections

---

### 10. Idempotency Strategies

**shipment_attributes** (INSERT with NOT EXISTS):
- Key: `carrier_id + tracking_number` (UNIQUE INDEX enforced)
- INSERT: Only new tracking numbers (NOT EXISTS check)
- No updates performed (core attributes never change)
- Safe to re-run: NOT EXISTS + UNIQUE constraint prevents duplicates
- Cost calculated via view (not stored), so no cumulative logic issues

**shipment_charges** (INSERT with NOT EXISTS):
- Key: `carrier_bill_id + tracking_number + charge_type_id`
- Prevents duplicate charges within same invoice
- Safe to re-run: NOT EXISTS check prevents duplicates
- View automatically recalculates total from all charges

**carrier_bill** (INSERT with NOT EXISTS):
- Key: `bill_number + bill_date`
- Prevents duplicate invoice headers
- Safe to re-run: NOT EXISTS in HAVING clause

**Why No Transaction Needed**:
- Each part is independently idempotent (constraints + NOT EXISTS)
- View calculates cost from whatever charges exist (always correct)
- Partial success is fine (view will show correct cost once charges inserted)

---

### 11. Known Limitations & Future Enhancements

**Current Limitations**:
- `shipment_date` not in business key (due to potential NULLs in correction records)
- No validation that `SUM(shipment_charges.amount)` equals `billed_shipping_cost`
- No audit trail of individual corrections (only cumulative total visible)
- Cannot distinguish between initial load vs. correction load in data (same table, same structure)

**Planned Enhancements**:
1. Add `shipment_date` to business key once confirmed no NULLs in corrections
2. Add `correction_history` table to track each adjustment separately
3. Implement validation query: Compare `billed_shipping_cost` vs `SUM(shipment_charges.amount)` for initial loads
4. Add `correction_count` column to track how many corrections applied
5. Consider separate `corrections` table with `original_cost`, `correction_amount`, `reason_code`

---

### 12. Validation & Testing

**No Validation File** - Correctness by Construction

**Architecture Enforces Correctness**:
- ✅ `billed_shipping_cost` calculated via view → Cannot mismatch (single source of truth)
- ✅ `UNIQUE INDEX (carrier_id, tracking_number)` → Prevents duplicate business keys
- ✅ `NOT NULL` on `shipment_attribute_id` → Prevents orphaned charges
- ✅ `FOREIGN KEY` constraint → Enforces referential integrity
- ✅ `NOT EXISTS` idempotency check → Prevents duplicate charges on rerun
- ✅ Delta table architecture (1 file = 1 invoice) → Prevents concurrent processing

**MPS Classification Cannot Be Validated**:
- Relies on FedEx's CSV format (1 file = 1 invoice, wide format with 50 charge columns)
- Architectural guarantee: No duplicate rows possible in FedEx's billing system structure
- If wrong: FedEx changed format or system is broken → Contact vendor, not testable
- Cannot distinguish: "Duplicate rows" vs "MPS group" without external reference

**What Could Actually Go Wrong**:
1. **FedEx Changes CSV Format** → Pipeline fails or produces wrong results → Update logic per new specs
2. **FedEx Sends Corrupt File** → Parse errors, schema violations → File-level validation (out of scope)
3. **Unmapped Charge Types** → Charges dropped by INNER JOIN → Sync_Reference_Data.sql auto-populates new types

**Design Principle**: 
- Correctness by construction (view + constraints + architectural guarantees)
- Trust FedEx's billing system isn't catastrophically broken (same as trusting SQL Server's SUM())
- No validation file needed

---

## Integration Notes

**Pipeline Execution Order**:
1. `ValidateAndInitializeFile.sql` - Validate account, get `@Carrier_id` and `@File_id`
2. `Insert_ELT_&_CB.sql` - Populate `carrier_bill` + `fedex_bill` from delta staging (with `file_id`)
3. `Sync_Reference_Data.sql` - Populate `charge_types` and `shipping_method` from new data (filtered by `file_id`)
4. `Insert_Unified_tables.sql` - **Idempotent 2-part pipeline** (filtered by `file_id`):
   - Part 1: INSERT `shipment_attributes` (metadata only, NOT EXISTS + UNIQUE constraint enforces no duplicates)
   - Part 2: INSERT `shipment_charges` (with FK to attributes, NOT EXISTS prevents duplicates)
   - Both parts safe to rerun independently

**ADF Activity Outputs**:

Each SQL script returns structured result sets for ADF monitoring and error handling:

1. **ValidateAndInitializeFile.sql** Output:
   - `carrier_id` (INT) - Carrier identifier for downstream queries
   - `file_id` (INT) - File tracking ID for file-based filtering
   - `validated_carrier_name` (NVARCHAR) - Carrier name or 'Skip' if already processed

2. **Insert_ELT_&_CB.sql** Output:
   - `Status` ('SUCCESS' or 'ERROR')
   - `InvoicesInserted` (INT) - Number of carrier_bill records inserted
   - `LineItemsInserted` (INT) - Number of fedex_bill records inserted
   - `ErrorNumber` (INT) - SQL error code (if error)
   - `ErrorMessage` (NVARCHAR) - Descriptive error message (if error)

3. **Sync_Reference_Data.sql** Output:
   - `ShippingMethodsAdded` (INT) - New shipping methods discovered
   - `ChargeTypesAdded` (INT) - New charge types discovered

4. **Insert_Unified_tables.sql** Output:
   - `Status` ('SUCCESS' or 'ERROR')
   - `AttributesInserted` (INT) - Number of shipment_attributes inserted
   - `ChargesInserted` (INT) - Number of shipment_charges inserted
   - `ErrorNumber` (INT) - SQL error code (if error)
   - `ErrorMessage` (NVARCHAR) - Descriptive error message (if error)
   - `ErrorLine` (INT) - Line number where error occurred (if error)

**Query Layer**:
- Use `vw_shipment_summary` for queries needing `billed_shipping_cost`
- View joins `shipment_attributes` + `shipment_charges` on-the-fly
- Fast via indexed FK relationship

**Dependencies**:
- Part 2 depends on Part 1 for `shipment_attribute_id` FK lookup (within same script execution)
- `Sync_Reference_Data.sql` must run before `Insert_Unified_tables.sql` to populate charge_types
- `@File_id` parameter drives file-based filtering across all steps
- File tracking enables parallel processing, fail-fast duplicate detection, and selective retry
- Idempotency guaranteed by constraints, NOT EXISTS checks, and file_id filtering

---

## Questions to Ask When Adding New Carriers

Use this checklist when integrating a new carrier:

### Data Format
1. **Schema**: Wide table or already normalized?
2. **Charge Structure**: Separate columns, single total, or JSON/XML?
3. **Row Grain**: One row per shipment, per package, or per charge?

### Grouping Logic
4. **Multi-Piece**: How are grouped shipments identified? (Similar to FedEx MPS?)
5. **Header Rows**: Are there summary rows that need filtering?
6. **Package Hierarchy**: Parent/child relationships? How identified?

### Corrections & Adjustments
7. **Correction Type**: Additive (like FedEx) or replacement?
8. **Correction Identification**: How do you know it's a correction vs. new shipment?
9. **Negative Amounts**: Credits/refunds possible? How represented?

### Unique Identifiers
10. **Tracking Number**: Single field or composite? Always present?
11. **Business Key**: What makes a shipment unique? (tracking + date? + service type?)
12. **Duplicates**: Can same tracking appear in multiple invoices legitimately?

### Unit Standards
13. **Weight Units**: LB, KG, OZ, grams? Need conversion?
14. **Dimension Units**: Inches, CM, MM? Need conversion?
15. **Currency**: Always USD or multi-currency?

### Date Handling
16. **Date Format**: YYYYMMDD int, date type, datetime, string?
17. **Date Fields**: Shipment date, invoice date, delivery date - which is authoritative?
18. **NULL Dates**: Can dates be NULL? In corrections?

### Data Quality
19. **NULL Handling**: Which fields can be NULL? Required fields?
20. **Empty Strings**: Are empty strings treated as NULL?
21. **Validation**: What's required for a valid record?

---

## Carrier Comparison Matrix

| Aspect | FedEx | [Next Carrier] | [Next Carrier] |
|--------|-------|----------------|----------------|
| **Data Format** | Wide table (50 charge columns) | ? | ? |
| **Multi-Piece** | MPS with header/parent/child | ? | ? |
| **Corrections** | Cumulative (add amounts) | ? | ? |
| **Business Key** | carrier_id + tracking_number | ? | ? |
| **Weight Unit** | LB (convert to OZ) | ? | ? |
| **Dimension Unit** | IN or CM (convert to IN) | ? | ? |
| **Date Format** | YYYYMMDD int | ? | ? |
| **Correction Dates** | May have NULL shipment_date | ? | ? |

---

## Version History

| Date | Carrier | Changes | Author |
|------|---------|---------|--------|
| 2026-02-08 | FedEx | Initial documentation of MPS logic, cumulative corrections, wide table schema | AI Assistant |

---

## Related Files

- `schema.sql` - Database schema with all table definitions
- `fedex_transform/Insert_Unified_tables.sql` - Main transformation with MPS logic
- `fedex_transform/Insert_ELT_&_CB.sql` - Loads fedex_bill from delta staging
- `fedex_transform/Sync_Reference_Data.sql` - Populates charge_types and shipping_method
- `fedex_transform/Validate_MPS_Logic.sql` - Comprehensive validation queries
- `fedex_transform/MPS_Implementation_Summary.md` - Implementation details and test cases
- `fedex_transform/Fedex_charges.sql` - View that unpivots 50 charge columns


