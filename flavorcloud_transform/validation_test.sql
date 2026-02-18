/*
================================================================================
FlavorCloud Integration - Validation Test Query
================================================================================
Purpose: Validates that the charge amounts in shipment_charges table match 
         the expected totals from the source CSV file.

Test 1: File Total vs Charges Total
Expected Result: Difference < $0.01 (accounting for rounding)

Note: FlavorCloud has 6 charge types per shipment. Zero-amount charges are excluded.
      SUM(shipment_charges.amount) should equal SUM(Shipment Total Charges) from CSV
      because Shipment Total = Commissions + Duties + Taxes + Fees + Insurance + Shipping Charges.

Usage: Run after Insert_Unified_tables.sql completes successfully.
       Replace @Carrier_id with actual FlavorCloud carrier_id value.
================================================================================
*/

DECLARE @Carrier_id INT = (SELECT carrier_id FROM dbo.carrier WHERE carrier_name = 'FlavorCloud');

/*
================================================================================
Validation Query 1: File Total vs Charges Total
================================================================================
Compares:
  - Expected: Sum of [Shipment Total Charges (USD)] from CSV (delta table, line items only)
  - Actual: Sum of all charges from shipment_charges

Pass Criteria: ABS(expected - actual) < 0.01
================================================================================
*/

WITH file_total AS (
    SELECT 
        SUM(CAST([Shipment Total Charges (USD)] AS DECIMAL(18,2))) AS expected,
        COUNT(*) AS file_shipment_count
    FROM billing.delta_flavorcloud_bill
    WHERE [Invoice Number] IS NOT NULL
        AND NULLIF(TRIM([Invoice Number]), '') IS NOT NULL
        AND [Invoice Number] != 'Total'
        AND [Shipment Number] IS NOT NULL
        AND NULLIF(TRIM([Shipment Number]), '') IS NOT NULL
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
    'FlavorCloud Validation' AS test_name,
    f.expected AS expected_total,
    c.actual AS actual_total,
    ABS(f.expected - c.actual) AS difference,
    CASE 
        WHEN ABS(f.expected - c.actual) < 0.01 THEN 'PASS' 
        ELSE 'FAIL' 
    END AS result,
    f.file_shipment_count AS file_shipment_count,
    c.charged_shipment_count AS charged_shipment_count,
    c.total_charge_records AS total_charge_records
FROM file_total f, charges_total c;

/*
================================================================================
Validation Query 2: Charge Breakdown by Type
================================================================================
Shows distribution of charges by type.
Expected: Up to 6 charge types (Shipping Charges, Commissions, Duties, Taxes, Fees, Insurance)
Zero-amount charges are excluded so not all 6 may appear.
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
Validation Query 3: Shipment Count Reconciliation
================================================================================
Verifies that all shipments from flavorcloud_bill made it to shipment_attributes
================================================================================
*/

WITH flavorcloud_shipments AS (
    SELECT COUNT(DISTINCT tracking_number) AS count
    FROM billing.flavorcloud_bill fb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = fb.carrier_bill_id
    WHERE cb.carrier_id = @Carrier_id
),
unified_shipments AS (
    SELECT COUNT(*) AS count
    FROM billing.shipment_attributes sa
    WHERE sa.carrier_id = @Carrier_id
)
SELECT 
    'Shipment Count Reconciliation' AS test_name,
    fs.count AS flavorcloud_bill_count,
    us.count AS shipment_attributes_count,
    fs.count - us.count AS difference,
    CASE 
        WHEN fs.count = us.count THEN 'PASS' 
        ELSE 'FAIL' 
    END AS result
FROM flavorcloud_shipments fs, unified_shipments us;

/*
================================================================================
Validation Query 4: Charge Type Seeding
================================================================================
Verifies that the expected charge types exist for FlavorCloud
================================================================================
*/

SELECT 
    'Charge Type Seeding' AS test_name,
    COUNT(*) AS actual_charge_types,
    CASE 
        WHEN COUNT(*) = 6 THEN 'PASS (Expected 6 charge types)' 
        ELSE 'FAIL (Expected 6 charge types)' 
    END AS result
FROM dbo.charge_types
WHERE carrier_id = @Carrier_id;

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
  - Note: Zero-amount charges are excluded from shipment_charges but since they
    are $0, the sum should still match.

Test 2: Charge Breakdown
  - Expected: Up to 6 charge types
  - Shipping Charges: freight=1, charge_category_id=11
  - Commissions, Duties, Taxes, Fees, Insurance: freight=0, charge_category_id=11

Test 3: Shipment Count
  - Expected: PASS (equal counts)
  - Validates: All shipments from normalized table reached unified table

Test 4: Charge Type Seeding
  - Expected: PASS (exactly 6 charge types)
  - Validates: Insert_Charge_Types.sql seeded correctly

If any test fails:
  1. Check Insert_ELT_&_CB.sql for transaction rollback
  2. Verify Insert_Charge_Types.sql ran before pipeline
  3. Review Insert_Unified_tables.sql charge unpivot logic
  4. Check for NULL/empty Shipment Numbers filtered correctly
  5. Verify Total/summary rows were excluded from delta ingestion
================================================================================
*/
