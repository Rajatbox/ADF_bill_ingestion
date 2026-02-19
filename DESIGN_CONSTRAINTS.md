# Design Constraints

**Note:** Database names are parameterized per environment (DEV/UAT/PROD). Scripts reference only `schema.table` format.

---

## Quick Reference Card

### ✅ ALWAYS Include:
- `@Carrier_id` (INT) parameter
- `@File_id` (INT) parameter
- `JOIN carrier_bill cb` + `WHERE cb.file_id = @File_id` in Sync and Unified scripts
- `file_id` column in carrier_bill INSERT
- Unit conversions (weight → OZ, dimensions → IN)
- `carrier_id` in all NOT EXISTS checks
- Standard error handling (TRY...CATCH with descriptive messages)

### ❌ NEVER Use:
- Temp tables (#temp) or table variables (@table)
- Dynamic SQL (EXEC sp_executesql)
- TRY_CAST (use CAST for fail-fast)
- Computed columns stored in tables
- New helper tables, procedures, or functions
- Modified parent pipeline scripts

### 📋 Script Count: 3 per Carrier
1. Insert_ELT_&_CB.sql (WITH transaction)
2. Sync_Reference_Data.sql (NO transaction)
3. Insert_Unified_tables.sql (NO transaction)

### 📂 Table Locations:
- Staging: `billing.delta_[carrier]_bill`
- Carrier-Specific: `billing.[carrier]_bill`
- Shared: `billing.carrier_bill`, `billing.shipment_attributes`, `billing.shipment_charges`
- Reference: `dbo.carrier`, `dbo.shipping_method`, `dbo.charge_types`

---


## 1. Parent Pipeline Immutability
- **NEVER modify** `parent_pipeline/Load_to_gold.sql`
- **NEVER modify** `parent_pipeline/ValidateCarrierInfo.sql` (formerly LookupCarrierInfo.sql)
- **NEVER modify** `parent_pipeline/CompleteFileProcessing.sql`
- Parent pipeline is carrier-agnostic and shared across all carriers

---

## 1.5. Table and Schema Conventions

### Staging (Delta) Tables
- **Schema:** `billing`
- **Pattern:** `billing.delta_[carrier]_bill`
- **Purpose:** Raw CSV data (1:1 with file structure)
- **Example:** `billing.delta_fedex_bill`, `billing.delta_dhl_bill`

### Carrier-Specific Tables
- **Schema:** `billing`
- **Pattern:** `billing.[carrier]_bill`
- **Purpose:** Typed/cleaned carrier data with carrier_bill_id FK
- **Example:** `billing.fedex_bill`, `billing.dhl_bill`, `billing.ups_bill`

### Shared Invoice Table
- **Schema:** `billing`
- **Table:** `billing.carrier_bill`
- **Purpose:** Invoice summaries across all carriers (with file_id)
- **Grain:** One row per invoice per carrier

### Unified Analytical Tables
- **Schema:** `billing`
- **Tables:** 
  - `billing.shipment_attributes` (physical shipment data)
  - `billing.shipment_charges` (itemized charges)
- **Purpose:** Carrier-agnostic analytical layer

### Reference Tables
- **Schema:** `dbo`
- **Tables:** 
  - `dbo.carrier` (carrier master)
  - `dbo.shipping_method` (service types per carrier)
  - `dbo.charge_types` (charge types per carrier)
- **Purpose:** Lookup tables for IDs and descriptions

### File Tracking Table
- **Schema:** `billing`
- **Table:** `billing.file_ingestion_tracker`
- **Purpose:** Track file processing status (created_at, completed_at)
- **Grain:** One row per file per carrier

## 2. Transaction Boundaries

### Transactional (BEGIN TRANSACTION / COMMIT)
```sql
BEGIN TRANSACTION;
    INSERT INTO carrier_bill ...;
    INSERT INTO [carrier]_bill ...;
COMMIT TRANSACTION;
```
**Why**: Invoice header + line items must succeed together

### Non-Transactional (Idempotent with NOT EXISTS)
```sql
-- No transaction needed
INSERT INTO shipping_method ... WHERE NOT EXISTS ...;
INSERT INTO charge_types ... WHERE NOT EXISTS ...;
INSERT INTO shipment_attributes ... WHERE NOT EXISTS ...;
INSERT INTO shipment_charges ... WHERE NOT EXISTS ...;
```
**Why**: Each independently idempotent via constraints

## 3. Type Conversion - Fail Fast
```sql
-- ✅ CORRECT: Direct CAST
CAST([Invoice Date] AS DATE)

-- ❌ WRONG: TRY_CAST silently converts bad data to NULL
TRY_CAST([Invoice Date] AS DATE)
```

## 4. Idempotency Pattern
```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM target_table t
    WHERE t.key1 = source.key1
        AND t.carrier_id = @Carrier_id  -- ALWAYS include carrier_id
)
```

## 5. Business Key
- Unified layer: `(carrier_id, tracking_number)`
- Enforced by UNIQUE INDEX on shipment_attributes
- No updates after initial insert

## 6. Cost Calculation
```sql
-- ❌ WRONG: Don't store cost in shipment_attributes
ALTER TABLE shipment_attributes ADD billed_shipping_cost ...;

-- ✅ CORRECT: View calculates on-the-fly
CREATE VIEW vw_shipment_summary AS
SELECT sa.*, SUM(sc.amount) AS billed_shipping_cost
FROM shipment_attributes sa
LEFT JOIN shipment_charges sc ON sc.shipment_attribute_id = sa.id
GROUP BY ...;
```
**Why**: Corrections accumulate automatically, no sync issues

## 7. Unit Conversions (REQUIRED)

### Weight → Ounces (OZ)
```sql
CASE 
    WHEN UPPER(weight_unit) = 'LB' THEN weight * 16
    WHEN UPPER(weight_unit) = 'KG' THEN weight * 35.274
    ELSE weight  -- Already OZ
END
```

### Dimensions → Inches (IN)
```sql
CASE 
    WHEN UPPER(dim_unit) = 'CM' THEN dimension / 2.54
    WHEN UPPER(dim_unit) = 'MM' THEN dimension / 25.4
    ELSE dimension  -- Already IN
END
```

## 8. Script Return Values
```sql
-- Success
SELECT 
    'SUCCESS' AS Status,
    @Count AS RecordsInserted;

-- Error (with descriptive THROW)
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        '[Carrier] [ScriptName].sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH
```

## 9. Carrier Bill Line Items - Use carrier_bill_id Join Only
```sql
-- ✅ CORRECT: Use carrier_bill_id only in NOT EXISTS
INSERT INTO [carrier]_bill (...)
SELECT ...
FROM delta_[carrier]_bill d
INNER JOIN carrier_bill cb
    ON cb.bill_number = d.[Invoice Number]
    AND cb.bill_date = d.[Invoice Date]
    AND cb.carrier_id = @Carrier_id  -- Always include carrier_id in join
WHERE NOT EXISTS (
    SELECT 1
    FROM [carrier]_bill t
    WHERE t.carrier_bill_id = cb.carrier_bill_id  -- Single field check
);

-- ❌ WRONG: Don't redundantly check invoice_number AND invoice_date in NOT EXISTS
WHERE NOT EXISTS (
    SELECT 1
    FROM [carrier]_bill t
    WHERE t.invoice_number = d.[Invoice Number]
      AND t.invoice_date = d.[Invoice Date]
      AND t.carrier_bill_id = cb.carrier_bill_id
);
```
**Why**: carrier_bill_id uniquely identifies the invoice. Once an invoice is processed (carrier_bill_id exists in line items table), skip all lines. This is cleaner and prevents duplicate inserts more reliably.

## 10. Narrow vs Wide Format

### Narrow Format (one row per charge)
```sql
-- Read charges directly from [carrier]_bill
SELECT charge_description, charge_amount
FROM [carrier]_bill;
```

### Wide Format (50+ charge columns like FedEx)
```sql
-- Create unpivot view first
CREATE VIEW vw_[Carrier]Charges AS
SELECT tracking_number, charge_type, charge_amount
FROM [carrier]_bill
UNPIVOT (...) AS unpvt;
```

## 11. Charge Category Mapping (CRITICAL)

**NEVER** invent charge category names or IDs. Only use the explicit mappings below:

### Known Categories (with IDs):
- **Adjustment** → `charge_category_id = 16`
- **Other** → `charge_category_id = 11`

### Pattern (Carrier-Specific Field-Based):

**FedEx** (uses charge_description text matching):
```sql
CASE 
    WHEN LOWER(charge_description) LIKE '%adjustment%' THEN 'Adjustment'
    ELSE 'Other'
END AS category,

CASE 
    WHEN LOWER(charge_description) LIKE '%adjustment%' THEN 16
    ELSE 11
END AS charge_category_id
```

**UPS** (uses charge_category_code field):
```sql
CASE 
    WHEN charge_category_code = 'ADJ' THEN 'Adjustment'
    ELSE 'Other'
END AS category,

CASE 
    WHEN charge_category_code = 'ADJ' THEN 16
    ELSE 11
END AS charge_category_id
```

```sql
-- ❌ WRONG: Don't invent categories
CASE 
    WHEN classification_code = 'FSC' THEN 'Fuel'       -- Unknown category_id!
    WHEN classification_code = 'ACC' THEN 'Accessorial'  -- Unknown category_id!
    ELSE 'Other'
END
```

**Why**: We don't maintain a full category mapping table. Only Adjustment (16) and Other (11) are defined.

**Freight Flag**: Use carrier-specific field (e.g., UPS: `charge_category_code = 'SHP'`, FedEx: service type)

## 12. File-Based Processing (MANDATORY)

### All Scripts Use @File_id

**Input Parameters (ALL 3 SCRIPTS):**
- `@Carrier_id` (INT) - Carrier identifier from parent pipeline
- `@File_id` (INT) - File tracking ID from ValidateCarrierInfo.sql

### ❌ NEVER Do This:
```sql
-- WRONG: No filtering at all
INSERT INTO shipment_attributes (...)
SELECT ... FROM [carrier]_bill  -- Missing file_id filter!

-- WRONG: Filtering without join to carrier_bill
WHERE [carrier]_bill.created_date > '2024-01-01'
```

### ✅ ALWAYS Do This:

**Script 1: Insert_ELT_&_CB.sql**
```sql
-- STEP 1: carrier_bill INSERT
INSERT INTO billing.carrier_bill (
    carrier_id, bill_number, bill_date, 
    total_amount, num_shipments, account_number,
    file_id  -- MUST include
)
SELECT 
    @Carrier_id, [invoice], [date], 
    SUM([charges]), COUNT(*), MAX([account]),
    @File_id AS file_id  -- MUST include
FROM billing.delta_[carrier]_bill
GROUP BY ...
HAVING NOT EXISTS (
    SELECT 1 FROM billing.carrier_bill cb
    WHERE cb.file_id = @File_id  -- MUST check file_id only
);

-- STEP 2: [carrier]_bill INSERT
INSERT INTO billing.[carrier]_bill (...)
SELECT ... 
FROM billing.delta_[carrier]_bill d
JOIN billing.carrier_bill cb 
    ON cb.bill_number = d.[invoice]
    AND cb.carrier_id = @Carrier_id
WHERE NOT EXISTS (...);
```

**Script 2 & 3: Sync_Reference_Data.sql and Insert_Unified_tables.sql**
```sql
-- MANDATORY PATTERN: ALWAYS join carrier_bill and filter by file_id
INSERT INTO [target_table] (...)
SELECT ...
FROM billing.[carrier]_bill [carrier]
JOIN billing.carrier_bill cb 
    ON cb.carrier_bill_id = [carrier].carrier_bill_id  -- MUST join
WHERE cb.file_id = @File_id  -- MUST filter
  AND NOT EXISTS (...);
```

**CRITICAL:** EVERY data access in Sync_Reference_Data.sql and Insert_Unified_tables.sql MUST:
1. JOIN to `billing.carrier_bill`
2. Filter by `WHERE cb.file_id = @File_id`

**Why File-Based:**
- **Idempotency:** Same file = same data (fail-fast duplicate detection)
- **Parallelism:** Different carriers/files can process simultaneously
- **Retry:** Rerun specific failed files without reprocessing all data
- **Atomicity:** Each file is a processing unit
- **Audit Trail:** Complete file processing history in file_ingestion_tracker

**Execution Flow:**
1. ValidateCarrierInfo.sql creates/checks file record → returns `file_id` or 'Skip'
2. Insert_ELT_&_CB.sql inserts carrier_bill with `file_id`
3. Sync_Reference_Data.sql filters by `file_id` (via carrier_bill join)
4. Insert_Unified_tables.sql filters by `file_id` (via carrier_bill join)
5. CompleteFileProcessing.sql marks file as completed

## 13. Prohibited Patterns (Common AI Mistakes)

### ❌ Don't Create Helper Objects
```sql
-- WRONG: Don't create temp tables
CREATE TABLE #temp_data ...
SELECT ... INTO #temp FROM ...

-- WRONG: Don't create table variables
DECLARE @temp TABLE (...)

-- WRONG: Don't create procedures
CREATE PROCEDURE sp_helper ...

-- WRONG: Don't create functions
CREATE FUNCTION fn_helper ...
```

**WHY:** Keep scripts self-contained and simple. All logic should be inline SQL.

### ❌ Don't Invent New Tables
```sql
-- WRONG: Don't create audit tables
CREATE TABLE billing.carrier_bill_audit ...

-- WRONG: Don't create staging beyond delta tables
CREATE TABLE billing.stage_[carrier]_bill ...

-- WRONG: Don't create summary tables
CREATE TABLE billing.carrier_bill_summary ...
```

**WHY:** Schema is fixed. Use existing tables and views only.

### ❌ Don't Add Computed Columns to Tables
```sql
-- WRONG: Don't store computed values in tables
ALTER TABLE billing.shipment_attributes 
ADD total_cost AS (SELECT SUM(amount) FROM shipment_charges ...)

-- WRONG: Don't add columns to existing tables
ALTER TABLE billing.carrier_bill ADD processing_date DATETIME2

-- CORRECT: Use views for computed values
CREATE VIEW billing.vw_shipment_summary AS
SELECT sa.*, SUM(sc.amount) AS total_cost
FROM billing.shipment_attributes sa
LEFT JOIN billing.shipment_charges sc ON sc.shipment_attribute_id = sa.id
GROUP BY ...;
```

**WHY:** Schema changes require coordination. Use views for calculations.

### ❌ Don't Use Dynamic SQL
```sql
-- WRONG: Don't build dynamic queries
DECLARE @sql NVARCHAR(MAX) = 'INSERT INTO ' + @tableName + ' VALUES (...)';
EXEC sp_executesql @sql;

-- WRONG: Don't use dynamic table names
DECLARE @tableName NVARCHAR(100) = 'billing.' + @carrier + '_bill';
```

**WHY:** Dynamic SQL is hard to debug and can introduce SQL injection risks.

### ❌ Don't Create Loops or Cursors
```sql
-- WRONG: Don't use cursors
DECLARE cursor_name CURSOR FOR SELECT ... 
OPEN cursor_name
FETCH NEXT FROM cursor_name

-- WRONG: Don't use WHILE loops to process rows
WHILE (SELECT COUNT(*) FROM ...) > 0
BEGIN
    -- Process one row at a time
END
```

**WHY:** Set-based operations are faster and simpler. Use JOINs and WHERE clauses.

### ❌ Don't Modify Return Structure
```sql
-- WRONG: Don't change standard return columns
SELECT 'SUCCESS' AS Result, ... -- Should be 'Status'

-- WRONG: Don't add unnecessary columns
SELECT 
    'SUCCESS' AS Status,
    @Count AS RecordsInserted,
    GETDATE() AS ProcessedAt,  -- Not needed
    @UserName AS ProcessedBy   -- Not needed
```

**WHY:** ADF pipelines expect specific column names. Stick to the template.

---

## 14. Required Script Structure (All 3 Scripts)

### Header (MANDATORY)
```sql
/*
================================================================================
[Script Name]: [Purpose]
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - [ScriptSpecificCount1]: INT - Number of records inserted
    - [ScriptSpecificCount2]: INT - Number of records inserted (if applicable)
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: [Brief description]

Source:   [Source tables]
Targets:  [Target tables]

Execution Order: [SECOND/THIRD/FOURTH] in pipeline (after [previous script])
================================================================================
*/

SET NOCOUNT ON;
```

### Declarations (if needed)
```sql
DECLARE @Count1 INT, @Count2 INT;
```

### Main Logic with Error Handling (MANDATORY)
```sql
BEGIN TRY
    -- Transaction if needed (only Insert_ELT_&_CB.sql)
    -- BEGIN TRANSACTION; (only for Insert_ELT_&_CB.sql)
    
    -- Your INSERT/UPDATE logic here
    INSERT INTO ... 
    SET @Count1 = @@ROWCOUNT;
    
    -- COMMIT TRANSACTION; (only for Insert_ELT_&_CB.sql)
    
    -- Return success
    SELECT 
        'SUCCESS' AS Status,
        @Count1 AS RecordsInserted;

END TRY
BEGIN CATCH
    -- Rollback if transaction was started
    -- IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; (only for Insert_ELT_&_CB.sql)
    
    -- Build error details
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        '[Carrier] [ScriptName].sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    -- Return error details
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
```

**NO EXCEPTIONS:** Every script MUST follow this structure exactly.

---

## 15. Execution Order
1. ValidateCarrierInfo.sql (parent pipeline - get carrier_id, file_id)
2. Insert_ELT_&_CB.sql (transactional, with file_id)
3. Sync_Reference_Data.sql (idempotent, filtered by file_id)
4. Insert_Unified_tables.sql (idempotent, filtered by file_id)
5. CompleteFileProcessing.sql (parent pipeline - marks file as completed)
6. Load_to_gold.sql (parent pipeline - DO NOT MODIFY)

