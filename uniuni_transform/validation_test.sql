/*
================================================================================
Validation Test Query: UniUni Bill Reconciliation
================================================================================
Purpose: Verify that the total amount in carrier_bill matches the sum of all
         charges in shipment_charges for UniUni carrier.

Test Logic:
1. Calculate expected total from delta_uniuni_bill (source of truth)
   - Sum of: Total Billed Amount column from CSV
   
2. Calculate actual total from shipment_charges (unified layer)
   - Sum of all charge amounts for UniUni shipments
   
3. Compare expected vs actual
   - Pass: Difference < $0.01 (accounting for rounding)
   - Fail: Difference >= $0.01

Usage:
- Run after Insert_Unified_tables.sql completes
- Set @Carrier_id to UniUni carrier_id (from dbo.carrier table)
- Expected result: '✅ PASS'

Note: This test validates the entire data pipeline:
      delta → carrier_bill → uniuni_bill → shipment_charges
================================================================================
*/

DECLARE @Carrier_id INT;

-- Get UniUni carrier_id
SELECT @Carrier_id = carrier_id
FROM dbo.carrier
WHERE carrier_name = 'UniUni';

-- Validation Test: File total vs charges total
WITH file_total AS (
    SELECT 
        SUM(CAST(ISNULL(NULLIF(TRIM(d.[Total Billed Amount]), ''), '0') AS DECIMAL(18,2))) AS expected_total
    FROM billing.delta_uniuni_bill d
),
charges_total AS (
    SELECT 
        SUM(sc.amount) AS actual_total
    FROM billing.shipment_charges sc
    INNER JOIN billing.shipment_attributes sa 
        ON sa.id = sc.shipment_attribute_id
    WHERE sa.carrier_id = @Carrier_id
),
carrier_bill_total AS (
    SELECT 
        SUM(cb.total_amount) AS carrier_bill_total
    FROM billing.carrier_bill cb
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
    COUNT(DISTINCT u.tracking_number) AS [Count in uniuni_bill],
    COUNT(DISTINCT sa.tracking_number) AS [Count in shipment_attributes],
    COUNT(DISTINCT u.tracking_number) - COUNT(DISTINCT sa.tracking_number) AS [Missing Count]
FROM billing.uniuni_bill u
LEFT JOIN billing.shipment_attributes sa 
    ON sa.tracking_number = u.tracking_number
    AND sa.carrier_id = @Carrier_id;

-- Check for charge type coverage (should have 18 charge types)
SELECT 
    'Charge Type Coverage' AS [Check],
    COUNT(DISTINCT ct.charge_name) AS [Charge Types Synced],
    CASE 
        WHEN COUNT(DISTINCT ct.charge_name) = 18 
        THEN '✅ All 18 charge types present'
        ELSE '❌ Missing charge types'
    END AS [Status]
FROM dbo.charge_types ct
WHERE ct.carrier_id = @Carrier_id;

-- List all charge types for UniUni
SELECT 
    ct.charge_type_id,
    ct.charge_name,
    ct.freight,
    ct.category,
    ct.charge_category_id,
    COUNT(sc.id) AS [Times Used]
FROM dbo.charge_types ct
LEFT JOIN billing.shipment_charges sc 
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
FROM billing.shipment_attributes sa
INNER JOIN billing.shipment_charges sc 
    ON sc.shipment_attribute_id = sa.id
INNER JOIN dbo.charge_types ct 
    ON ct.charge_type_id = sc.charge_type_id
WHERE sa.carrier_id = @Carrier_id
ORDER BY sa.tracking_number, ct.charge_name;

-- Unit conversion validation: Check weight conversions
SELECT 
    'Weight Conversion Check' AS [Check],
    ub.tracking_number,
    ub.dim_weight AS [Original Weight],
    ub.dim_weight_uom AS [Original UOM],
    sa.billed_weight_oz AS [Converted Weight (OZ)],
    CASE 
        WHEN UPPER(TRIM(ub.dim_weight_uom)) = 'LBS' 
            THEN ABS((ub.dim_weight * 16.0) - sa.billed_weight_oz)
        WHEN UPPER(TRIM(ub.dim_weight_uom)) = 'OZS' 
            THEN ABS(ub.dim_weight - sa.billed_weight_oz)
        ELSE NULL
    END AS [Conversion Diff],
    CASE 
        WHEN UPPER(TRIM(ub.dim_weight_uom)) = 'LBS' AND ABS((ub.dim_weight * 16.0) - sa.billed_weight_oz) < 0.01
            THEN '✅ PASS'
        WHEN UPPER(TRIM(ub.dim_weight_uom)) = 'OZS' AND ABS(ub.dim_weight - sa.billed_weight_oz) < 0.01
            THEN '✅ PASS'
        WHEN ub.dim_weight IS NULL OR sa.billed_weight_oz IS NULL
            THEN '⚠️ NULL'
        ELSE '❌ FAIL'
    END AS [Status]
FROM billing.uniuni_bill ub
INNER JOIN billing.shipment_attributes sa 
    ON sa.tracking_number = ub.tracking_number
    AND sa.carrier_id = @Carrier_id
WHERE ub.dim_weight IS NOT NULL
ORDER BY ub.tracking_number;

-- Unit conversion validation: Check dimension conversions
SELECT 
    'Dimension Conversion Check' AS [Check],
    ub.tracking_number,
    ub.dim_length AS [Original Length],
    ub.dim_width AS [Original Width],
    ub.dim_height AS [Original Height],
    ub.package_dim_uom AS [Original UOM],
    sa.billed_length_in AS [Length (IN)],
    sa.billed_width_in AS [Width (IN)],
    sa.billed_height_in AS [Height (IN)],
    CASE 
        WHEN UPPER(TRIM(ub.package_dim_uom)) = 'CM' 
            THEN ABS((ub.dim_length * 0.393701) - sa.billed_length_in)
        WHEN UPPER(TRIM(ub.package_dim_uom)) = 'IN' 
            THEN ABS(ub.dim_length - sa.billed_length_in)
        ELSE NULL
    END AS [Length Conversion Diff],
    CASE 
        WHEN UPPER(TRIM(ub.package_dim_uom)) = 'CM' AND 
             ABS((ub.dim_length * 0.393701) - sa.billed_length_in) < 0.01 AND
             ABS((ub.dim_width * 0.393701) - sa.billed_width_in) < 0.01 AND
             ABS((ub.dim_height * 0.393701) - sa.billed_height_in) < 0.01
            THEN '✅ PASS'
        WHEN UPPER(TRIM(ub.package_dim_uom)) = 'IN' AND 
             ABS(ub.dim_length - sa.billed_length_in) < 0.01 AND
             ABS(ub.dim_width - sa.billed_width_in) < 0.01 AND
             ABS(ub.dim_height - sa.billed_height_in) < 0.01
            THEN '✅ PASS'
        WHEN ub.dim_length IS NULL OR sa.billed_length_in IS NULL
            THEN '⚠️ NULL'
        ELSE '❌ FAIL'
    END AS [Status]
FROM billing.uniuni_bill ub
INNER JOIN billing.shipment_attributes sa 
    ON sa.tracking_number = ub.tracking_number
    AND sa.carrier_id = @Carrier_id
WHERE ub.dim_length IS NOT NULL
ORDER BY ub.tracking_number;

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
- Charge Types Synced: 18
- Status: '✅ All 18 charge types present'

Query 4 (Charge Types List):
Should show 18 rows:
1. Approved Claim (freight=0, category='Adjustment')
2. Base Rate (freight=1, category='Base')
3. Billed Fee (freight=1, category='Base')
4. Credit (freight=0, category='Adjustment')
5. Credit Card Surcharge (freight=0, category='Surcharge')
6. Delivery Area Surcharge (freight=0, category='Surcharge')
7. Delivery Area Surcharge Extend (freight=0, category='Surcharge')
8. Discount Fee (freight=0, category='Discount')
9. Fuel Surcharge (freight=0, category='Surcharge')
10. Miscellaneous Fee (freight=0, category='Accessorial')
11. Over Dimension Fee (freight=0, category='Surcharge')
12. Over Max Size Fee (freight=0, category='Surcharge')
13. Over Weight Fee (freight=0, category='Surcharge')
14. Peak Season Surcharge (freight=0, category='Surcharge')
15. Pick Up Fee (freight=0, category='Accessorial')
16. Relabel Fee (freight=0, category='Accessorial')
17. Signature Fee (freight=0, category='Accessorial')
18. Truck Fee (freight=0, category='Accessorial')

Query 5 (Sample Shipment):
- Shows one shipment with all its charges itemized
- Total Billed Cost should match sum of individual charges

Query 6 (Weight Conversion Check):
- All conversions should show '✅ PASS'
- LBS → OZ: multiply by 16.0
- OZS → OZ: no conversion

Query 7 (Dimension Conversion Check):
- All conversions should show '✅ PASS'
- CM → IN: multiply by 0.393701
- IN → IN: no conversion
================================================================================
*/
