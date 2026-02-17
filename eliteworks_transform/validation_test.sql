/*
================================================================================
Eliteworks Integration - Validation Test Query
================================================================================
Purpose: Validates that the 'Platform Charged' amounts in shipment_charges table 
         match the expected total from the source CSV file.

Test: File Total vs Platform Charged Total
Expected Result: Difference < $0.01 (accounting for rounding)

Note: Eliteworks stores 3 separate charge types:
      - Base Rate: Base carrier charge
      - Store Markup: Platform markup charge  
      - Platform Charged: Final billed amount (validation target)

Usage: Run after Insert_Unified_tables.sql completes successfully.
       Replace @Carrier_id with actual Eliteworks carrier_id value.
================================================================================
*/

-- Set carrier_id parameter (replace with actual value from dbo.carrier table)
DECLARE @Carrier_id INT = (SELECT carrier_id FROM dbo.carrier WHERE carrier_name = 'Eliteworks');

/*
================================================================================
Validation Query: File Total vs Charges Total
================================================================================
Compares:
  - Expected: Sum of platform_charged_with_corrections from CSV (delta table)
  - Actual: Sum of 'Platform Charged' charge type from shipment_charges
  
Note: The 'Platform Charged' charge type stores the final billed amount directly.
      Base Rate and Store Markup are also stored separately for analytics.

Pass Criteria: ABS(expected - actual) < 0.01
================================================================================
*/

WITH file_total AS (
    SELECT 
        SUM(CAST(platform_charged_with_corrections AS DECIMAL(18,2))) AS expected,
        COUNT(*) AS file_shipment_count
    FROM billing.delta_eliteworks_bill
    WHERE tracking_number IS NOT NULL
        AND NULLIF(TRIM(tracking_number), '') IS NOT NULL
),
charges_total AS (
    SELECT 
        SUM(sc.amount) AS actual,
        COUNT(DISTINCT sa.tracking_number) AS charged_shipment_count,
        COUNT(sc.id) AS total_charge_records
    FROM billing.shipment_charges sc
    JOIN billing.shipment_attributes sa ON sa.id = sc.shipment_attribute_id
    WHERE sa.carrier_id = @Carrier_id
)
SELECT 
    'Eliteworks Validation' AS test_name,
    f.expected AS expected_total,
    c.actual AS actual_total,
    ABS(f.expected - c.actual) AS difference,
    CASE 
        WHEN ABS(f.expected - c.actual) < 0.01 THEN '✅ PASS' 
        ELSE '❌ FAIL' 
    END AS result,
    f.file_shipment_count AS file_shipment_count,
    c.charged_shipment_count AS charged_shipment_count,
    c.total_charge_records AS total_charge_records
FROM file_total f, charges_total c;

/*
================================================================================
Additional Validation: Charge Breakdown by Type
================================================================================
Shows distribution of charges by type to verify CROSS APPLY unpivot logic
================================================================================
*/

SELECT 
    ct.charge_name,
    ct.freight AS is_freight_charge,
    ct.charge_category_id,
    COUNT(*) AS charge_count,
    SUM(sc.amount) AS total_amount,
    AVG(sc.amount) AS avg_amount,
    MIN(sc.amount) AS min_amount,
    MAX(sc.amount) AS max_amount
FROM billing.shipment_charges sc
JOIN billing.shipment_attributes sa ON sa.id = sc.shipment_attribute_id
JOIN dbo.charge_types ct ON ct.charge_type_id = sc.charge_type_id
WHERE sa.carrier_id = @Carrier_id
GROUP BY ct.charge_name, ct.freight, ct.charge_category_id
ORDER BY total_amount DESC;

/*
================================================================================
Additional Validation: Shipment Count Reconciliation
================================================================================
Verifies that all shipments from eliteworks_bill made it to shipment_attributes
================================================================================
*/

WITH eliteworks_shipments AS (
    SELECT COUNT(DISTINCT tracking_number) AS count
    FROM billing.eliteworks_bill eb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = eb.carrier_bill_id
    WHERE cb.carrier_id = @Carrier_id
),
unified_shipments AS (
    SELECT COUNT(*) AS count
    FROM billing.shipment_attributes sa
    WHERE sa.carrier_id = @Carrier_id
)
SELECT 
    'Shipment Count Reconciliation' AS test_name,
    es.count AS eliteworks_bill_count,
    us.count AS shipment_attributes_count,
    es.count - us.count AS difference,
    CASE 
        WHEN es.count = us.count THEN '✅ PASS' 
        ELSE '❌ FAIL' 
    END AS result
FROM eliteworks_shipments es, unified_shipments us;

/*
================================================================================
Additional Validation: Charge Type Seeding
================================================================================
Verifies that all 3 expected charge types exist for Eliteworks
================================================================================
*/

SELECT 
    'Charge Type Seeding' AS test_name,
    COUNT(*) AS actual_charge_types,
    CASE 
        WHEN COUNT(*) = 3 THEN '✅ PASS (Expected 3: Base Rate, Store Markup, Platform Charged)' 
        ELSE '❌ FAIL (Expected 3 charge types)' 
    END AS result
FROM dbo.charge_types
WHERE carrier_id = @Carrier_id;

-- Show the actual charge types
SELECT 
    ct.charge_type_id,
    ct.charge_name,
    ct.freight,
    ct.charge_category_id,
    ctc.category_name
FROM dbo.charge_types ct
LEFT JOIN dbo.charge_type_category ctc ON ctc.id = ct.charge_category_id
WHERE ct.carrier_id = @Carrier_id
ORDER BY ct.freight DESC, ct.charge_name;

/*
================================================================================
Expected Results Summary
================================================================================
Test 1: File Total vs Charges Total
  - Expected: PASS (difference < $0.01)
  - Validates: 'Platform Charged' charge type correctly stores the final billed amount

Test 2: Charge Breakdown
  - Expected: 3 charge types (Base Rate, Store Markup, Platform Charged)
  - Base Rate should have freight=1, others freight=0
  - All charge types should have charge_category_id=11 (Other)

Test 3: Shipment Count
  - Expected: PASS (equal counts)
  - Validates: All shipments from normalized table reached unified table

Test 4: Charge Type Seeding
  - Expected: PASS (exactly 3 charge types)
  - Validates: Sync_Reference_Data.sql seeded correctly

If any test fails:
  1. Check Insert_ELT_&_CB.sql for transaction rollback
  2. Verify Sync_Reference_Data.sql ran successfully
  3. Review Insert_Unified_tables.sql CROSS APPLY logic
  4. Check for NULL tracking numbers filtered correctly
================================================================================
*/
