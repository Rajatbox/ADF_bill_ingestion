/*
================================================================================
Passport Integration - Validation Test Query
================================================================================
Purpose: Validates that the charge amounts in shipment_charges table match
         the expected totals from the source CSV file.

Test 1: File Total vs Charges Total
  - Expected: PASS (difference < $0.01)
  - Note: TOTAL column in CSV = Rate + Fuel Surcharge + Tax + Duty + Insurance
            + Clearance Fee + FEE 1–8 amounts. Zero-amount charges are excluded
            from shipment_charges but the SUM should still reconcile.

Test 2: Charge Breakdown by Type
  - Expected: Up to 7 charge types per carrier_id
  - Rate: freight=1, all others: freight=0, charge_category_id=11

Test 3: Shipment Count Reconciliation
  - Expected: PASS (passport_bill count = shipment_attributes count)

Test 4: Charge Type Seeding
  - Expected: PASS (exactly 7 charge types seeded)

Usage: Run after Insert_Unified_tables.sql completes successfully.
       Replace @Carrier_id with the actual Passport carrier_id value.
================================================================================
*/

DECLARE @Carrier_id INT = (SELECT carrier_id FROM dbo.carrier WHERE carrier_name = 'Passport');

/*
================================================================================
Validation Query 1: File Total vs Charges Total
================================================================================
Compares:
  - Expected: Sum of [TOTAL] from delta_passport_bill
  - Actual:   Sum of all charges from shipment_charges for this carrier
Pass Criteria: ABS(expected - actual) < 0.01
================================================================================
*/

WITH file_total AS (
    SELECT
        SUM(CAST([TOTAL] AS DECIMAL(18,2))) AS expected,
        COUNT(*)                            AS file_shipment_count
    FROM billing.delta_passport_bill
    WHERE [INVOICE NUMBER] IS NOT NULL
        AND NULLIF(TRIM([INVOICE NUMBER]), '') IS NOT NULL
        AND [TRACKING ID] IS NOT NULL
        AND NULLIF(TRIM([TRACKING ID]), '') IS NOT NULL
),
charges_total AS (
    SELECT
        SUM(sc.amount)                          AS actual,
        COUNT(DISTINCT sa.tracking_number)      AS charged_shipment_count,
        COUNT(sc.id)                            AS total_charge_records
    FROM billing.shipment_charges sc
    JOIN billing.shipment_attributes sa ON sa.id = sc.shipment_attribute_id
    WHERE sa.carrier_id = @Carrier_id
)
SELECT
    'Passport Validation'   AS test_name,
    f.expected              AS expected_total,
    c.actual                AS actual_total,
    ABS(f.expected - c.actual) AS difference,
    CASE
        WHEN ABS(f.expected - c.actual) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END                                 AS result,
    f.file_shipment_count               AS file_shipment_count,
    c.charged_shipment_count            AS charged_shipment_count,
    c.total_charge_records              AS total_charge_records
FROM file_total f, charges_total c;

/*
================================================================================
Validation Query 2: Charge Breakdown by Type
================================================================================
Shows distribution of charges by type.
Expected: Up to 7 charge types (Rate, Fuel Surcharge, Tax, Duty, Insurance,
          Clearance Fee, Additional Fees). Zero-amount charges are excluded.
================================================================================
*/

SELECT
    ct.charge_name,
    ct.freight              AS is_freight_charge,
    ct.charge_category_id,
    COUNT(*)                AS charge_count,
    SUM(sc.amount)          AS total_amount,
    AVG(sc.amount)          AS avg_amount,
    MIN(sc.amount)          AS min_amount,
    MAX(sc.amount)          AS max_amount
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
Verifies that all shipments from passport_bill made it to shipment_attributes.
================================================================================
*/

WITH passport_shipments AS (
    SELECT COUNT(DISTINCT p.tracking_number) AS count
    FROM billing.passport_bill p
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = p.carrier_bill_id
    WHERE cb.carrier_id = @Carrier_id
),
unified_shipments AS (
    SELECT COUNT(*) AS count
    FROM billing.shipment_attributes sa
    WHERE sa.carrier_id = @Carrier_id
)
SELECT
    'Shipment Count Reconciliation' AS test_name,
    ps.count                        AS passport_bill_count,
    us.count                        AS shipment_attributes_count,
    ps.count - us.count             AS difference,
    CASE
        WHEN ps.count = us.count THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM passport_shipments ps, unified_shipments us;

/*
================================================================================
Validation Query 4: Charge Type Seeding
================================================================================
Verifies that exactly 7 charge types are seeded for Passport.
================================================================================
*/

SELECT
    'Charge Type Seeding'                   AS test_name,
    COUNT(*)                                AS actual_charge_types,
    CASE
        WHEN COUNT(*) = 7 THEN 'PASS (Expected 7 charge types)'
        ELSE 'FAIL (Expected 7 charge types)'
    END AS result
FROM dbo.charge_types
WHERE carrier_id = @Carrier_id;

SELECT
    ct.charge_type_id,
    ct.charge_name,
    ct.freight,
    ct.charge_category_id
FROM dbo.charge_types ct
WHERE ct.carrier_id = @Carrier_id
ORDER BY ct.freight DESC, ct.charge_name;

/*
================================================================================
Expected Results Summary
================================================================================
Test 1: File Total vs Charges Total
  - Expected: PASS (difference < $0.01)
  - TOTAL = Rate + Fuel Surcharge + Tax + Duty + Insurance + Clearance Fee + FEE 1–8

Test 2: Charge Breakdown
  - Expected: Up to 7 charge types
  - Rate: freight=1, charge_category_id=11
  - All others: freight=0, charge_category_id=11
  - Additional Fees may be $0 for most shipments (FEE 1–8 commonly zero)

Test 3: Shipment Count
  - Expected: PASS (equal counts)
  - Validates: All shipments from passport_bill reached shipment_attributes

Test 4: Charge Type Seeding
  - Expected: PASS (exactly 7 charge types)
  - Validates: Insert_Charge_Types.sql seeded correctly before pipeline run

If any test fails:
  1. Check Insert_ELT_&_CB.sql — verify transaction committed successfully
  2. Verify Insert_Charge_Types.sql ran before the pipeline
  3. Review Insert_Unified_tables.sql CROSS APPLY charge unpivot logic
  4. Check that [TRACKING ID] and [INVOICE NUMBER] are not NULL/empty in delta
================================================================================
*/
