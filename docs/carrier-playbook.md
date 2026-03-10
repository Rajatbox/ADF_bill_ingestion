# Carrier Implementation Playbook: File-Based Ingestion Tracking

## Purpose

Step-by-step guide for implementing file-based ingestion tracking for new carriers. Use this playbook for the 2+ incoming carriers and all future integrations.

## Prerequisites

- Schema migration completed (`file_ingestion_tracker` table, `carrier_bill.file_id` column)
- Parent pipeline updated (`ValidateCarrierInfo.sql`, `CompleteFileProcessing.sql`)
- FedEx reference implementation available

---

## Implementation Checklist

### Step 1: Gather Carrier Information

- [ ] Carrier name (for folder: `[carrier]_transform/`)
- [ ] Sample CSV file
- [ ] Existing stored procedure (if migrating from legacy system)
- [ ] Tracking number format/logic
- [ ] Charge structure (wide vs narrow format)
- [ ] Unit of measure for weight/dimensions
- [ ] Account number location in CSV

### Step 2: Create Delta Table Schema

**File**: `schema.sql`

```sql
CREATE TABLE billing.delta_[carrier]_bill (
    -- 1:1 replica of CSV columns
    -- Use NVARCHAR(MAX) for all columns initially
    ...
);
```

**Considerations**:
- FedEx/DHL/UPS: No headers (ordinal: Prop_0, Prop_1, etc.)
- USPS EasyPost: Has headers (named columns)
- UniUni: Has headers (named columns with spaces)

### Step 3: Update Parent Pipeline Scripts

**No changes needed** - `ValidateCarrierInfo.sql` already handles multi-carrier validation.

**Action required**: Add carrier to CASE statement if not already present:

```sql
WHEN @InputCarrier = '[new_carrier]' THEN JSON_VALUE(@RawJson, '$.Prop_X')
```

### Step 4: Create Transform Scripts (3 files)

---

#### Script 1: Insert_ELT_&_CB.sql

**Template**:

```sql
/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT
    - LineItemsInserted: INT
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion with file tracking:
         1. Insert carrier_bill with file_id
         2. Insert [carrier]_bill line items

File Tracking: Enables idempotency, parallelism, selective retry

Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @InvoicesInserted INT, @LineItemsInserted INT;

BEGIN TRANSACTION;
BEGIN TRY

    -- STEP 1: Insert carrier_bill (with file_id)
    INSERT INTO billing.carrier_bill (
        carrier_id, bill_number, bill_date, 
        total_amount, num_shipments, account_number,
        file_id  -- NEW
    )
    SELECT
        @Carrier_id,
        [invoice_column],
        CAST([date_column] AS DATE),
        SUM([charge_columns]),
        COUNT(*),
        MAX([account_column]),
        @File_id AS file_id  -- NEW
    FROM billing.delta_[carrier]_bill
    WHERE [invoice_column] IS NOT NULL
    GROUP BY [invoice_column], CAST([date_column] AS DATE)
    HAVING NOT EXISTS (
        SELECT 1 FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id  -- FILE-BASED IDEMPOTENCY
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    -- STEP 2: Insert [carrier]_bill line items
    INSERT INTO billing.[carrier]_bill (
        carrier_bill_id, ...
    )
    SELECT
        cb.carrier_bill_id, ...
    FROM billing.delta_[carrier]_bill d
    JOIN billing.carrier_bill cb 
        ON cb.bill_number = d.[invoice_column] 
        AND cb.bill_date = CAST(d.[date_column] AS DATE)
        AND cb.carrier_id = @Carrier_id
    WHERE NOT EXISTS (
        SELECT 1 FROM billing.[carrier]_bill t
        WHERE t.carrier_bill_id = cb.carrier_bill_id
    );

    SET @LineItemsInserted = @@ROWCOUNT;

    COMMIT;
    
    SELECT 
        'SUCCESS' AS Status, 
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @ErrorMessage AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW;
END CATCH;
```

**Key points**:
- Transaction wraps both inserts (atomicity)
- `file_id` stored in carrier_bill
- Idempotency via `file_id` (not bill_number/date/carrier_id)

---

#### Script 2: Sync_Reference_Data.sql

**Template**:

```sql
/*
================================================================================
Reference Data Synchronization Script
================================================================================
ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT
    - ChargeTypesAdded: INT
    - ErrorNumber: INT (if error)

Purpose: Discover and insert new shipping methods and charge types from 
         carrier-specific bills.

File-Based Filtering: Joins carrier_bill to filter by file_id

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

    -- PART 1: Discover shipping methods
    INSERT INTO dbo.shipping_method (
        carrier_id, method_name, service_level, 
        guaranteed_delivery, is_active
    )
    SELECT DISTINCT
        @Carrier_id,
        [service_column] AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active
    FROM billing.[carrier]_bill [carrier]
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
    WHERE cb.file_id = @File_id  -- FILE-BASED FILTERING
      AND [service_column] IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM dbo.shipping_method sm
        WHERE sm.method_name = [service_column] 
          AND sm.carrier_id = @Carrier_id
      );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    -- PART 2: Discover charge types
    -- Option A: Wide format (CROSS APPLY)
    INSERT INTO dbo.charge_types (
        carrier_id, charge_name, freight, charge_category_id
    )
    SELECT DISTINCT charge_name, @Carrier_id, freight, charge_category_id
    FROM billing.[carrier]_bill [carrier]
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
    CROSS APPLY (VALUES
        ('Charge Name 1', [charge_column_1], 1, 11),  -- freight=1 for base rate
        ('Charge Name 2', [charge_column_2], 0, 11)
    ) AS x(charge_name, amount, freight, charge_category_id)
    WHERE cb.file_id = @File_id  -- FILE-BASED FILTERING
      AND x.amount IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM dbo.charge_types ct
        WHERE ct.charge_name = x.charge_name AND ct.carrier_id = @Carrier_id
      );

    -- Option B: Narrow format (direct read)
    INSERT INTO dbo.charge_types (
        carrier_id, charge_name, freight, charge_category_id
    )
    SELECT DISTINCT
        @Carrier_id,
        [charge_desc_column],
        CASE WHEN [charge_type_field] = 'FRT' THEN 1 ELSE 0 END,
        11  -- Other category
    FROM billing.[carrier]_bill [carrier]
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
    WHERE cb.file_id = @File_id  -- FILE-BASED FILTERING
      AND NOT EXISTS (
        SELECT 1 FROM dbo.charge_types ct
        WHERE ct.charge_name = [charge_desc_column] AND ct.carrier_id = @Carrier_id
      );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    SELECT 
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded,
        @ChargeTypesAdded AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @ErrorMessage AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW;
END CATCH;
```

**Key points**:
- No transaction needed (independently idempotent)
- Always join carrier_bill for file_id filtering
- Charge discovery varies by format (wide vs narrow)

---

#### Script 3: Insert_Unified_tables.sql

**Template**:

```sql
/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges
================================================================================
ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - AttributesInserted: INT
    - ChargesInserted: INT
    - ErrorNumber: INT (if error)

Purpose: Transform carrier-specific data into unified analytical schema:
         1. Insert shipment_attributes with unit conversions
         2. Insert shipment_charges (unpivoted charges)

File-Based Filtering: Joins carrier_bill to filter by file_id

Unit Conversions: Weight → OZ, Dimensions → IN (REQUIRED)

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    -- PART 1: Insert shipment_attributes
    INSERT INTO billing.shipment_attributes (
        carrier_id, tracking_number, shipment_date, zone,
        billed_weight_oz, dim_length_in, dim_width_in, dim_height_in,
        shipping_method_id, recipient_zip, shipper_zip
    )
    SELECT
        @Carrier_id,
        [tracking_column],
        CAST([date_column] AS DATE),
        [zone_column],
        -- UNIT CONVERSIONS REQUIRED
        CASE 
            WHEN UPPER([weight_unit]) = 'LB' THEN [weight] * 16
            WHEN UPPER([weight_unit]) = 'KG' THEN [weight] * 35.274
            ELSE [weight]
        END AS billed_weight_oz,
        CASE 
            WHEN UPPER([dim_unit]) = 'CM' THEN [length] / 2.54
            WHEN UPPER([dim_unit]) = 'MM' THEN [length] / 25.4
            ELSE [length]
        END AS dim_length_in,
        CASE 
            WHEN UPPER([dim_unit]) = 'CM' THEN [width] / 2.54
            WHEN UPPER([dim_unit]) = 'MM' THEN [width] / 25.4
            ELSE [width]
        END AS dim_width_in,
        CASE 
            WHEN UPPER([dim_unit]) = 'CM' THEN [height] / 2.54
            WHEN UPPER([dim_unit]) = 'MM' THEN [height] / 25.4
            ELSE [height]
        END AS dim_height_in,
        sm.shipping_method_id,
        [recipient_zip_column],
        [shipper_zip_column]
    FROM billing.[carrier]_bill [carrier]
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
    LEFT JOIN dbo.shipping_method sm 
        ON sm.method_name = [service_column] AND sm.carrier_id = @Carrier_id
    WHERE cb.file_id = @File_id  -- FILE-BASED FILTERING
      AND [tracking_column] IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM billing.shipment_attributes sa
        WHERE sa.carrier_id = @Carrier_id 
          AND sa.tracking_number = [tracking_column]
      );

    SET @AttributesInserted = @@ROWCOUNT;

    -- PART 2: Insert shipment_charges
    ;WITH charge_source AS (
        SELECT
            @Carrier_id AS carrier_id,
            [carrier].carrier_bill_id,
            [tracking_column],
            ct.charge_type_id,
            CAST(x.amount AS DECIMAL(18,2)) AS amount,
            sa.id AS shipment_attribute_id
        FROM billing.[carrier]_bill [carrier]
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
        CROSS APPLY (VALUES
            ('Charge Name 1', [charge_column_1]),
            ('Charge Name 2', [charge_column_2])
        ) AS x(charge_name, amount)
        INNER JOIN dbo.charge_types ct 
            ON ct.charge_name = x.charge_name AND ct.carrier_id = @Carrier_id
        INNER JOIN billing.shipment_attributes sa 
            ON sa.carrier_id = @Carrier_id AND sa.tracking_number = [tracking_column]
        WHERE cb.file_id = @File_id  -- FILE-BASED FILTERING
          AND x.amount IS NOT NULL AND x.amount <> 0
    )
    INSERT INTO billing.shipment_charges (
        carrier_id, carrier_bill_id, tracking_number,
        charge_type_id, amount, shipment_attribute_id
    )
    SELECT 
        carrier_id, carrier_bill_id, tracking_number,
        charge_type_id, amount, shipment_attribute_id
    FROM charge_source
    WHERE NOT EXISTS (
        SELECT 1 FROM billing.shipment_charges sc
        WHERE sc.shipment_attribute_id = charge_source.shipment_attribute_id
          AND sc.carrier_bill_id = charge_source.carrier_bill_id
          AND sc.charge_type_id = charge_source.charge_type_id
    );

    SET @ChargesInserted = @@ROWCOUNT;

    SELECT 
        'SUCCESS' AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted AS ChargesInserted;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @ErrorMessage AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW;
END CATCH;
```

**Key points**:
- No transaction (both parts independently idempotent)
- Part 1: UNIQUE constraint on (carrier_id, tracking_number) prevents duplicates
- Part 2: NOT EXISTS prevents duplicate charges
- Unit conversions REQUIRED (weight → OZ, dimensions → IN)
- Always join carrier_bill for file_id filtering

---

### Step 5: Create Validation Test

**File**: `[carrier]_transform/validation_test.sql`

```sql
-- Test: File total vs charges total
DECLARE @File_id INT = /* test file_id */;
DECLARE @Carrier_id INT = /* carrier_id */;

WITH file_total AS (
    SELECT SUM(cb.total_amount) AS expected
    FROM billing.carrier_bill cb
    WHERE cb.file_id = @File_id
),
charges_total AS (
    SELECT SUM(sc.amount) AS actual
    FROM billing.shipment_charges sc
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
    WHERE cb.file_id = @File_id
)
SELECT 
    expected, actual, ABS(expected - actual) AS difference,
    CASE WHEN ABS(expected - actual) < 0.01 THEN '✅ PASS' ELSE '❌ FAIL' END AS result
FROM file_total, charges_total;
```

---

### Step 6: Test Execution

1. **Load test file** to delta table (Copy Data activity)
2. **Run Insert_ELT_&_CB.sql** - Verify InvoicesInserted, LineItemsInserted counts
3. **Run Sync_Reference_Data.sql** - Verify new shipping methods and charge types discovered
4. **Run Insert_Unified_tables.sql** - Verify AttributesInserted, ChargesInserted counts
5. **Run validation test** - Should return 'PASS' (difference < $0.01)

---

### Step 7: ADF Pipeline Configuration

**Child Pipeline**: `pl_[Carrier]_transform`

**Parameters**:
- `p_carrier_id` (INT) - From parent pipeline
- `p_file_id` (INT) - From parent pipeline (NEW - replaces p_last_run_time)

**Activities**:
1. Copy Data → `billing.delta_[carrier]_bill`
2. Script → `Insert_ELT_&_CB.sql` (with @Carrier_id, @File_id)
3. Script → `Sync_Reference_Data.sql` (with @Carrier_id, @File_id)
4. Script → `Insert_Unified_tables.sql` (with @Carrier_id, @File_id)

**Parent Pipeline Switch**: Add case for `[carrier]` → Execute `pl_[Carrier]_transform`

---

## Common Pitfalls

### 1. Forgot to Join carrier_bill for file_id
**Symptom**: Processing all data instead of just current file
**Fix**: Add `JOIN billing.carrier_bill cb ON ... WHERE cb.file_id = @File_id`

### 2. Missing Unit Conversions
**Symptom**: Mixed units in shipment_attributes (LB, KG, OZ)
**Fix**: Apply CASE statement for weight (→ OZ), dimensions (→ IN)

### 3. Wrong Idempotency Check in carrier_bill
**Symptom**: Using bill_number + bill_date + carrier_id in NOT EXISTS
**Fix**: Use `WHERE cb.file_id = @File_id` only

### 4. Missing file_id Column in carrier_bill INSERT
**Symptom**: NULL file_id in carrier_bill table
**Fix**: Add `file_id` to column list, `@File_id AS file_id` to SELECT

### 5. Missing @File_id Parameter Declaration
**Symptom**: SQL error: "Must declare the scalar variable @File_id"
**Fix**: Ensure ADF passes `p_file_id` parameter to script

---

## Reference Implementations

Use these as templates:

**Complete implementation**: `fedex_transform/`
- Insert_ELT_&_CB.sql (with MPS logic)
- Sync_Reference_Data.sql (with view-based charges)
- Insert_Unified_tables.sql (with 4-stage MPS CTE)
- Fedex_charges.sql (unpivot view with file_id)

**Simpler implementations**:
- **DHL**: `dhl_transform/` - 4 charge columns, domestic/international tracking
- **UPS**: `ups_transform/` - Narrow format
- **USPS**: `usps_easypost_transform/` - Clean CSV structure
- **UniUni**: `uniuni_transform/` - Wide format with 17 charge types

---

## Success Criteria

- [ ] All 3 scripts accept `@File_id` parameter
- [ ] carrier_bill INSERT includes `file_id` column
- [ ] All filtering in Sync and Unified scripts uses `cb.file_id = @File_id`
- [ ] Unit conversions applied (weight → OZ, dimensions → IN)
- [ ] Validation test passes (< $0.01 difference)
- [ ] Idempotent: Rerunning same file_id doesn't create duplicates
- [ ] Parallel processing: Different files process simultaneously without conflicts
- [ ] Fail-fast: Duplicate file detection before processing begins

---

## File-Based Processing Benefits

1. **Idempotency**: Same file won't create duplicates (file_id check in carrier_bill)
2. **Parallelism**: Different carriers/files process simultaneously without conflicts
3. **Selective Retry**: Rerun specific failed files without reprocessing all data
4. **Atomicity**: Each file is an independent processing unit
5. **Fail-Fast**: Duplicate file detection before any processing begins
6. **Audit Trail**: Complete file processing history in `file_ingestion_tracker`

---

## Next Steps After Implementation

1. Test with sample file
2. Validate against reference stored procedure output
3. Run parallel test (multiple files simultaneously)
4. Test retry scenario (rerun failed file)
5. Test duplicate detection (rerun completed file - should fail fast)
6. Document any carrier-specific business logic in `additional_reference.md`

