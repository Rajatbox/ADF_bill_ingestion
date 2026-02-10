# Design Constraints

**Note:** Database names are parameterized per environment (DEV/UAT/PROD). Scripts reference only `schema.table` format.

## 1. Parent Pipeline Immutability
- **NEVER modify** `parent_pipeline/Load_to_gold.sql`
- **NEVER modify** `parent_pipeline/LookupCarrierInfo.sql`
- Parent pipeline is carrier-agnostic

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

## 12. Execution Order
1. LookupCarrierInfo.sql (get carrier_id, lastrun)
2. Insert_ELT_&_CB.sql (transactional)
3. Sync_Reference_Data.sql (idempotent)
4. Insert_Unified_tables.sql (idempotent)
5. Load_to_gold.sql (parent pipeline - DO NOT MODIFY)

