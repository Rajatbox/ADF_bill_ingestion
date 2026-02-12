/*
================================================================================
Validation Test Query: USPS EasyPost Bill Reconciliation
================================================================================
Purpose: Verify that the total amount in carrier_bill matches the sum of all
         charges in shipment_charges for USPS EasyPost carrier.

Test Logic:
1. Calculate expected total from delta_usps_easypost_bill (source of truth)
   - Sum of: rate + label_fee + postage_fee + carbon_offset_fee + insurance_fee
   
2. Calculate actual total from shipment_charges (unified layer)
   - Sum of all charge amounts for USPS EasyPost shipments
   
3. Compare expected vs actual
   - Pass: Difference < $0.01 (accounting for rounding)
   - Fail: Difference >= $0.01

Usage:
- Run after Insert_Unified_tables.sql completes
- Set @Carrier_id to USPS EasyPost carrier_id (from dbo.carrier table)
- Expected result: '✅ PASS'

Note: This test validates the entire data pipeline:
      delta → carrier_bill → usps_easypost_bill → shipment_charges
================================================================================
*/

DECLARE @Carrier_id INT;

-- Get USPS EasyPost carrier_id
SELECT @Carrier_id = carrier_id
FROM dbo.carrier
WHERE carrier_name = 'USPS - Easy Post';

-- Validation Test: File total vs charges total
WITH file_total AS (
    SELECT 
        SUM(
            CAST(ISNULL(d.rate, '0') AS decimal(18,2)) + 
            CAST(ISNULL(d.label_fee, '0') AS decimal(18,2)) + 
            CAST(ISNULL(d.postage_fee, '0') AS decimal(18,2)) + 
            CAST(ISNULL(d.carbon_offset_fee, '0') AS decimal(18,2)) + 
            CAST(ISNULL(d.insurance_fee, '0') AS decimal(18,2))
        ) AS expected_total
    FROM test.delta_usps_easypost_bill d
),
charges_total AS (
    SELECT 
        SUM(sc.amount) AS actual_total
    FROM Test.shipment_charges sc
    INNER JOIN Test.shipment_attributes sa 
        ON sa.id = sc.shipment_attribute_id
    WHERE sa.carrier_id = @Carrier_id
),
carrier_bill_total AS (
    SELECT 
        SUM(cb.total_amount) AS carrier_bill_total
    FROM Test.carrier_bill cb
    WHERE cb.carrier_id = @Carrier_id
)
SELECT 
    ft.expected_total AS [Delta Table Total],
    cbt.carrier_bill_total AS [Carrier Bill Total],
    ct.actual_total AS [Shipment Charges Total],
    ABS(ft.expected_total - ct.actual_total) AS [Delta vs Charges Diff],
    ABS(cbt.carrier_bill_total - ct.actual_total) AS [Carrier Bill vs Charges Diff],
    CASE 
        WHEN ABS(ft.expected_total - ct.actual_total) < 0.01 
             AND ABS(cbt.carrier_bill_total - ct.actual_total) < 0.01
        THEN '✅ PASS - All totals match' 
        WHEN ABS(ft.expected_total - ct.actual_total) >= 0.01
        THEN '❌ FAIL - Delta vs Charges mismatch'
        WHEN ABS(cbt.carrier_bill_total - ct.actual_total) >= 0.01
        THEN '❌ FAIL - Carrier Bill vs Charges mismatch'
        ELSE '⚠️ UNKNOWN'
    END AS [Validation Result]
FROM file_total ft, charges_total ct, carrier_bill_total cbt;

/*
================================================================================
Additional Validation Queries (Optional)
================================================================================
*/

-- Check for missing tracking numbers in unified layer
SELECT 
    'Missing Tracking Numbers' AS [Check],
    COUNT(DISTINCT u.tracking_code) AS [Count in usps_easy_post_bill],
    COUNT(DISTINCT sa.tracking_number) AS [Count in shipment_attributes],
    COUNT(DISTINCT u.tracking_code) - COUNT(DISTINCT sa.tracking_number) AS [Missing Count]
FROM test.usps_easy_post_bill u
LEFT JOIN Test.shipment_attributes sa 
    ON sa.tracking_number = u.tracking_code
    AND sa.carrier_id = @Carrier_id;

-- Check for charge type coverage (should have 5 charge types)
SELECT 
    'Charge Type Coverage' AS [Check],
    COUNT(DISTINCT ct.charge_name) AS [Charge Types Synced],
    CASE 
        WHEN COUNT(DISTINCT ct.charge_name) = 5 
        THEN '✅ All 5 charge types present'
        ELSE '❌ Missing charge types'
    END AS [Status]
FROM test.charge_types ct
WHERE ct.carrier_id = @Carrier_id;

-- List all charge types for USPS EasyPost
SELECT 
    ct.charge_type_id,
    ct.charge_name,
    ct.freight,
    ct.category,
    ct.charge_category_id,
    COUNT(sc.id) AS [Times Used]
FROM test.charge_types ct
LEFT JOIN Test.shipment_charges sc 
    ON sc.charge_type_id = ct.charge_type_id
WHERE ct.carrier_id = @Carrier_id
GROUP BY 
    ct.charge_type_id,
    ct.charge_name,
    ct.freight,
    ct.category,
    ct.charge_category_id
ORDER BY ct.charge_name;

-- Sample shipment with charges breakdown
SELECT TOP 1
    sa.tracking_number,
    sa.shipping_method,
    sa.destination_zone,
    sa.billed_weight_oz,
    ct.charge_name,
    sc.amount,
    SUM(sc.amount) OVER (PARTITION BY sa.id) AS [Total Billed Cost]
FROM Test.shipment_attributes sa
INNER JOIN Test.shipment_charges sc 
    ON sc.shipment_attribute_id = sa.id
INNER JOIN test.charge_types ct 
    ON ct.charge_type_id = sc.charge_type_id
WHERE sa.carrier_id = @Carrier_id
ORDER BY sa.tracking_number, ct.charge_name;

/*
================================================================================
Expected Results
================================================================================
Query 1 (Main Validation):
- Validation Result: '✅ PASS - All totals match'
- All differences should be < $0.01

Query 2 (Missing Tracking Numbers):
- Missing Count: 0

Query 3 (Charge Type Coverage):
- Charge Types Synced: 5
- Status: '✅ All 5 charge types present'

Query 4 (Charge Types List):
Should show 5 rows:
1. Base Rate (freight=1, category='Other', charge_category_id=11)
2. Label Fee (freight=0, category='Other', charge_category_id=11)
3. Unknown Charges (freight=0, category='Other', charge_category_id=11)
4. Carbon Offset Fee (freight=0, category='Other', charge_category_id=11)
5. Insurance Fee (freight=0, category='Other', charge_category_id=11)

Query 5 (Sample Shipment):
- Shows one shipment with all its charges itemized
- Total Billed Cost should match sum of individual charges
================================================================================
*/

