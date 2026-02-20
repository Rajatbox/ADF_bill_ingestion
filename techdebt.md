# Technical Debt

This document tracks known technical debt items that should be addressed in future iterations.

## fedex_transform/Insert_ELT_&_CB.sql

### 1. Add carrier_id to NOT EXISTS clause in Step 1 (carrier_bill insert)

**Current State:**
The NOT EXISTS check in the carrier_bill insert (Step 1) only compares `bill_number` and `bill_date`:

```sql
HAVING
    NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill AS cb
        WHERE cb.bill_number = CAST(d.[Invoice Number] AS nvarchar(50))
            AND cb.bill_date = CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date)
    );
```

**Issue:**
If the same bill_number and bill_date exist for different carriers, this check would incorrectly prevent insertion.

**Recommended Fix:**
Include `carrier_id` in the NOT EXISTS comparison for more precise duplicate detection:

```sql
HAVING
    NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill AS cb
        WHERE cb.bill_number = CAST(d.[Invoice Number] AS nvarchar(50))
            AND cb.bill_date = CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date)
            AND cb.carrier_id = @Carrier_id
    );
```

**Priority:** Medium  
**Impact:** Low (assuming bill_number/bill_date combinations are unique across carriers in practice)

---

## schema.sql

### 2. Add index on created_date column in fedex_bill table

**Current State:**
The `fedex_bill` table has a `created_date` column (defaults to `sysdatetime()`) but no index on it.

**Issue:**
The `vw_FedExCharges` view is now filtered by `created_date > @lastrun` for incremental processing. Without an index, this filter will result in full table scans as the table grows, significantly impacting performance.

**Recommended Fix:**
Add a non-clustered index on the `created_date` column:

```sql
CREATE NONCLUSTERED INDEX IX_fedex_bill_created_date 
ON billing.fedex_bill (created_date);
```

**Alternative (Composite Index):**
If queries often filter by both `created_date` and `carrier_bill_id`, consider a composite index:

```sql
CREATE NONCLUSTERED INDEX IX_fedex_bill_created_date_carrier_bill_id 
ON billing.fedex_bill (created_date, carrier_bill_id);
```

**Priority:** High  
**Impact:** High (performance degradation as table grows without this index)

---

### 3. Add source_filename and created_date columns to carrier_bill table

**Current State:**
The `carrier_bill` table does not track the source CSV filename or the timestamp when the record was created.

**Issue:**
Without these audit columns, it's difficult to:
- Trace back which source file a particular bill record came from
- Determine when records were ingested for troubleshooting and audit purposes
- Implement incremental processing or time-based filtering on carrier_bill
- Debug data quality issues by correlating with source files

**Recommended Fix:**
Add two new columns to the `carrier_bill` table:

```sql
ALTER TABLE billing.carrier_bill
ADD source_filename varchar(255) NULL,
    created_date datetime2 NOT NULL DEFAULT sysdatetime();

-- Add index on created_date for potential filtering/reporting
CREATE NONCLUSTERED INDEX IX_carrier_bill_created_date 
ON billing.carrier_bill (created_date);
```

**Implementation Notes:**
- `source_filename`: Store the CSV filename from ADF pipeline variable (e.g., 'FedEx_Invoice_20260207.csv')
- `created_date`: Auto-populated with current timestamp, similar to `fedex_bill.created_date`
- Update `Insert_ELT_&_CB.sql` to populate `source_filename` from ADF parameter

**Priority:** Medium  
**Impact:** Medium (improves auditability and troubleshooting capabilities)

---

### 4. Add created_date column to charge_types and shipping_method tables

**Current State:**
The `charge_types` and `shipping_method` reference tables do not have a `created_date` column to track when records were created.

**Issue:**
Without timestamp tracking on these reference tables, it's difficult to audit when new charge types or shipping methods were added to the system.

**Recommended Fix:**
Add `created_date` column to both tables:

```sql
ALTER TABLE billing.charge_types
ADD created_date datetime2 NOT NULL DEFAULT sysdatetime();

ALTER TABLE billing.shipping_method
ADD created_date datetime2 NOT NULL DEFAULT sysdatetime();
```

**Priority:** Low  
**Impact:** Low (minor improvement to auditability for reference data)

---

### 5. Calculate hash on carrier_id, tracking_number, and ship_date to optimize joins

**Current State:**
Join operations between delta tables and target tables rely on VARCHAR comparisons of `tracking_number` columns, which is inefficient for large datasets.

**Issue:**
- VARCHAR joins are slower than integer comparisons, especially on large tables
- Downstream queries that join on tracking numbers suffer from poor performance
- Index seeks on VARCHAR columns are less efficient than on integer hash values
- Both upstream (delta → target) and downstream (analytics) queries are impacted

**Recommended Fix:**
Add a computed hash column to both delta and target tables:

```sql
-- Add hash column to delta_fedex_bill
ALTER TABLE billing.delta_fedex_bill
ADD shipment_hash AS CHECKSUM(
    CONCAT(
        CAST(@Carrier_id AS varchar(10)),
        '|',
        [Express or Ground Tracking ID],
        '|',
        CAST([Shipment Date] AS varchar(8))
    )
) PERSISTED;

-- Add hash column to fedex_bill and shipment_package_wip
ALTER TABLE billing.fedex_bill
ADD shipment_hash int NULL;

ALTER TABLE dbo.shipment_package_wip
ADD shipment_hash int NULL;

-- Create indexes on hash columns
CREATE NONCLUSTERED INDEX IX_fedex_bill_shipment_hash 
ON billing.fedex_bill (shipment_hash);

CREATE NONCLUSTERED INDEX IX_shipment_package_wip_shipment_hash 
ON dbo.shipment_package_wip (shipment_hash);
```

**Benefits:**
- Faster join performance (integer vs VARCHAR comparison)
- Reduced index size and improved cache hit rates
- Improved query performance for both ETL and analytics workloads
- Composite key (carrier_id + tracking_number + ship_date) ensures uniqueness

**Considerations:**
- Hash collisions possible with CHECKSUM function
- Alternative: Proper composite index on `(carrier_id, tracking_number, ship_date)` may suffice
- **Recommendation:** Measure current performance first, only implement if proven bottleneck exists

**Priority:** Low  
**Impact:** Medium (optimize after identifying actual performance bottleneck)

---

### 6. Add shipment_attribute_id to shipment_package_wip for on-the-fly variance calculation

**Current State:**
The `shipment_package_wip` table requires UPDATE operations to populate variance columns after initial insert, leading to a two-step process.

**Issue:**
- UPDATE operations are slower than single INSERT operations
- Two-step process (INSERT → UPDATE) increases transaction complexity
- Cannot calculate variances on-the-fly in queries without stored values
- Additional I/O overhead from updating existing rows

**Business Context:**
In real-world scenarios, shipments must occur before carrier bills are generated. The `shipment_attributes` data from WMS will always exist before carrier billing data arrives. The only failure scenario is broken ETL on the WMS side.

**Recommended Fix:**
Add `shipment_attribute_id` foreign key to `shipment_package_wip` to enable direct JOIN with `shipment_attributes`:

```sql
-- Add shipment_attribute_id column
ALTER TABLE dbo.shipment_package_wip
ADD shipment_attribute_id int NULL;

-- Add foreign key constraint
ALTER TABLE dbo.shipment_package_wip
ADD CONSTRAINT FK_shipment_package_wip_shipment_attributes
    FOREIGN KEY (shipment_attribute_id)
    REFERENCES billing.shipment_attributes(shipment_attribute_id);

-- Add index for join performance
CREATE NONCLUSTERED INDEX IX_shipment_package_wip_shipment_attribute_id 
ON dbo.shipment_package_wip (shipment_attribute_id);
```

**Query Example (Calculate Variance On-the-Fly):**
```sql
SELECT 
    wp.tracking_number,
    wp.carrier_billed_weight,
    sa.actual_weight,
    (wp.carrier_billed_weight - sa.actual_weight) AS weight_variance,
    wp.carrier_billed_amount,
    sa.expected_amount,
    (wp.carrier_billed_amount - sa.expected_amount) AS amount_variance
FROM shipment_package_wip wp
INNER JOIN shipment_attributes sa 
    ON wp.shipment_attribute_id = sa.shipment_attribute_id;
```

**Implementation Requirement:**
⚠️ **Parent pipeline must add WMS ETL validation check** - Add a lookup activity in the parent pipeline to verify WMS pipeline `last_run_date` is recent before processing carrier bills. This ensures `shipment_attributes` data is loaded and up-to-date before attempting carrier bill reconciliation.

**Parent Pipeline Check Example:**
```sql
-- Validate WMS data is fresh (within last 24 hours)
SELECT last_run_time
FROM carrier_ingestion_tracker
WHERE pipeline_name = 'pl_WMS_ShipmentAttributes'
  AND last_run_time > DATEADD(hour, -24, GETDATE());
```

**Benefits:**
- Eliminates UPDATE operations entirely (single INSERT workflow)
- Calculate variances on-the-fly in queries when needed
- Simpler ETL logic with single-pass processing
- Reduced transaction duration and lock contention
- Enforces proper data sequencing through parent pipeline validation

**Priority:** High  
**Impact:** High (eliminates UPDATE operations, simplifies ETL, improves performance)

---

### 7. Replace non-clustered index with columnstore index for shipment_package_wip reconciliation queries

**Current State:**
Standard non-clustered B-tree indexes are used on `carrier_id` and `tracking_number` columns in `shipment_package_wip` table.

**Issue:**
- B-tree indexes are optimized for OLTP (row-by-row lookups), not set-based analytical queries
- Reconciliation queries are ALWAYS set-based operations (bulk matching carrier bills with WMS shipment data)
- Higher storage overhead for traditional indexes on large tables
- Sub-optimal performance for set-based ETL joins and reconciliation processing

**Business Context:**
The business model is to **map physical shipment data (WMS) with financial data (carrier bills)** and then reconcile discrepancies. This involves two distinct phases with separate tables:

1. **Mapping Phase (Set-Based)**: `shipment_package_wip` table
   - Bulk/batch operations joining carrier bills with WMS shipment data
   - Set-based queries using `(carrier_id, tracking_number)` join key
   - Write-once, read-many workload (INSERT + SELECT, no updates)

2. **Reconciliation Phase (CRUD)**: `carrier_cost_ledger` table
   - Row-level reconciliation operations (Create, Update, Read, Delete)
   - User-driven variance resolution and dispute tracking
   - Traditional OLTP workload with rowstore indexes

**Key Insight**: CRUD operations are **isolated to `carrier_cost_ledger`**, NOT `shipment_package_wip`. This makes columnstore ideal for the mapping table with zero trade-offs.

**Recommended Fix:**
Create a non-clustered columnstore index optimized for set-based reconciliation:

```sql
-- Drop existing non-clustered indexes if they exist
DROP INDEX IF EXISTS IX_shipment_package_wip_carrier_id 
ON dbo.shipment_package_wip;

DROP INDEX IF EXISTS IX_shipment_package_wip_tracking_number 
ON dbo.shipment_package_wip;

-- Create non-clustered columnstore index
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_shipment_package_wip_reconciliation
ON dbo.shipment_package_wip (
    carrier_id,
    tracking_number,
    carrier_billed_weight,
    carrier_billed_amount,
    shipment_date,
    shipment_attribute_id,
    created_date
);
```

**Use Cases (All Set-Based):**
- Bulk mapping `shipment_package_wip` with `shipment_attributes` for variance analysis
- Batch reconciliation queries joining carrier bills with WMS shipment data
- Aggregation reports by carrier, date ranges, or variance thresholds
- ETL performance optimization for large-scale reconciliation operations

**Benefits:**
- **10x compression ratio** - significant storage cost reduction
- **Faster ETL performance** - columnstore optimized for batch scanning and set-based joins
- **Better set-based join performance** - ideal for bulk mapping operations
- **Reduced I/O** - columnar compression means less data read from disk
- **Perfect fit for workload** - write-once (INSERT), read-many (SELECT) with set-based queries
- **Zero trade-offs** - CRUD operations isolated to separate `carrier_cost_ledger` table

**Implementation Notes:**
- Keep clustered rowstore index on primary key for table maintenance
- Columnstore handles all mapping queries (100% of business queries on this table)
- No performance penalty - `shipment_package_wip` has no CRUD operations
- CRUD reconciliation happens in `carrier_cost_ledger` (separate table with rowstore indexes)

**Priority:** High  
**Impact:** High (significant ETL performance improvement for reconciliation, reduced storage costs, perfect match for set-based workload)

---

## eliteworks_transform & flavorcloud_transform

### 8. Rename Delta Table Columns to Match CSV Column Names Exactly

**Status:** Deferred  
**Created:** 2026-02-20  
**Priority:** Medium  
**Complexity:** Medium

**Current State:**
The `delta_eliteworks_bill` and `delta_flavorcloud_bill` tables have column names that were renamed from their original CSV headers for developer convenience. For example, CSV columns like `user_account`, `tracking_number`, `Origin Location`, etc. were transformed into more readable/standardized names.

**Issue:**
The original design strategy was to keep delta table column names **identical** to CSV column headers to enable future hash comparison for automated schema change detection. When CSV schema changes (new columns, renamed columns, reordered columns), a hash comparison between:
- CSV header row hash
- Delta table column name hash

...would immediately flag the schema drift without requiring manual inspection.

**Examples of Renamed Columns:**
- **Eliteworks:** CSV columns were normalized (e.g., `user_account` → standardized naming)
- **FlavorCloud:** CSV columns were normalized (e.g., `Origin Location` → standardized naming)

**Impact:**
- ❌ **Cannot implement automated schema change detection** via hash comparison
- ❌ **Manual inspection required** when carriers change CSV formats
- ⚠️ **Inconsistent with other carriers** (FedEx, UPS, etc. maintain exact CSV column names)
- ⚠️ **Higher maintenance burden** when troubleshooting CSV ingestion issues

**Recommended Fix:**
1. Rename delta table columns to match CSV headers exactly (case-sensitive, spaces preserved)
2. Update ADF Copy Data activity column mappings to reflect new names
3. Update all 3 transform scripts (`Insert_ELT_&_CB.sql`, `Sync_Reference_Data.sql`, `Insert_Unified_tables.sql`) to reference exact CSV column names with brackets/quotes
4. Update example CSV files if needed

**Example (Eliteworks):**
```sql
-- Current (normalized names)
SELECT 
    user_account,
    tracking_number,
    ship_date
FROM billing.delta_eliteworks_bill;

-- Desired (exact CSV names)
SELECT 
    [user_account],        -- Exact CSV header
    [tracking_number],     -- Exact CSV header  
    [ship_date]           -- Exact CSV header
FROM billing.delta_eliteworks_bill;
```

**Implementation Steps:**
1. Create `ALTER TABLE` statements to rename columns in both delta tables
2. Update ADF pipeline's Copy Data activity "Named Column Mapping" in `architecture.md`
3. Modify all SQL scripts in `eliteworks_transform/` and `flavorcloud_transform/` folders
4. Test end-to-end ingestion with example CSV files
5. Document exact CSV → Delta table column mappings

**Future Benefit:**
Once completed, implement automated schema validation in parent pipeline:
```sql
-- Hash comparison to detect CSV schema changes
DECLARE @CsvHeaderHash int = CHECKSUM(CONCAT(/* CSV headers */));
DECLARE @DeltaTableHash int = CHECKSUM(CONCAT(/* delta column names */));

IF @CsvHeaderHash <> @DeltaTableHash
    THROW 50001, 'CSV schema has changed - manual review required', 1;
```

**Priority:** Medium (blocks future automation, but no immediate functional impact)  
**Impact:** Medium (enables automated schema change detection, improves consistency across carriers)

---

## ups_transform/Insert_Unified_tables.sql

### 9. Charges with NULL Tracking Numbers Are Excluded

**Status:** Documented  
**Created:** 2026-02-10  
**Priority:** Low  
**Complexity:** Medium

**Current State:**
The `Insert_Unified_tables.sql` script excludes charges that don't have a tracking number:

**Part 1 - MERGE shipment_attributes:**
```sql
WHERE ub.created_date > @lastrun
    AND ub.tracking_number IS NOT NULL  -- Filters out NULL tracking numbers
    GROUP BY ub.tracking_number
```

**Part 2 - INSERT shipment_charges:**
```sql
FROM billing.ups_bill AS ub
INNER JOIN billing.shipment_attributes AS sa  -- Can't join if no tracking number
    ON sa.carrier_id = @carrier_id
    AND sa.tracking_number = ub.tracking_number
```

**Issue:**
Some UPS charges don't have tracking numbers (e.g., invoice-level fees, adjustments, service charges). These are excluded from `shipment_charges` because:
1. They can't be inserted into `shipment_attributes` (tracking_number is the business key)
2. The INNER JOIN in Part 2 requires matching on tracking_number
3. **Result:** Total charges in `shipment_charges` < Total in `ups_bill` (validation fails)

**Example Charges Without Tracking Numbers:**
- Invoice-level service charges
- Account-level adjustments
- Billing fees not tied to specific shipments

**Impact:**
- ✅ **Current:** Acceptable - shipment-level reconciliation is the primary use case
- ⚠️ **Future:** If full invoice reconciliation is needed, these charges must be included

**Recommended Fix (Future):**
If invoice-level charges need to be tracked, options include:

**Option 1: Separate Table for Invoice-Level Charges**
```sql
CREATE TABLE billing.invoice_level_charges (
    id int IDENTITY(1,1) PRIMARY KEY,
    carrier_id int NOT NULL,
    carrier_bill_id int NOT NULL,
    charge_type_id int NOT NULL,
    amount decimal(18,2) NOT NULL,
    created_date datetime2 DEFAULT SYSDATETIME(),
    FOREIGN KEY (carrier_id) REFERENCES billing.carrier(carrier_id),
    FOREIGN KEY (carrier_bill_id) REFERENCES billing.carrier_bill(carrier_bill_id),
    FOREIGN KEY (charge_type_id) REFERENCES dbo.charge_types(charge_type_id)
);
```

**Option 2: Allow NULL tracking_number in shipment_charges**
- Remove INNER JOIN requirement with shipment_attributes
- Change to LEFT JOIN or separate INSERT for NULL tracking numbers
- Update validation queries to sum both shipment_charges and invoice-level charges

**Option 3: Use Sentinel Value**
- Create a placeholder shipment_attribute with tracking_number = 'INVOICE_LEVEL' or 'NO_TRACKING'
- Link all non-shipment charges to this sentinel record
- Requires business logic to handle this special case

**Validation Query Impact:**
Current validation compares:
- **Expected:** SUM(delta_ups_bill.[Net Amount])
- **Actual:** SUM(shipment_charges.amount) WHERE tracking_number IS NOT NULL
- **Mismatch:** Charges with NULL tracking_number are excluded from actual

**Workaround (Current):**
Filter validation query to only compare charges with tracking numbers:
```sql
-- Modified validation: Only compare shipment-level charges
WITH file_total AS (
    SELECT SUM(CAST([Net Amount] AS decimal(18,2))) AS expected
    FROM billing.delta_ups_bill
    WHERE [Tracking Number] IS NOT NULL  -- Match filter logic
),
charges_total AS (
    SELECT SUM(amount) AS actual
    FROM billing.shipment_charges sc
    JOIN billing.shipment_attributes sa ON sa.id = sc.shipment_attribute_id
    WHERE sa.carrier_id = @carrier_id
)
SELECT expected, actual, ABS(expected - actual) AS difference;
```

**Priority:** Low (shipment-level reconciliation is sufficient for current business needs)  
**Impact:** Low (invoice-level charges represent small percentage of total, typically < 1%)

---

