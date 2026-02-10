/*
================================================================================
Validation Test: UniUni File Total vs Charges Total
================================================================================
Purpose: Verify that the sum of all charges in shipment_charges matches the
         sum of Total Billed Amount from the original CSV file (delta_uniuni_bill).

Expected Result: difference < $0.01 = PASS

Usage: Run this after executing all transform scripts:
       1. Insert_ELT_&_CB.sql
       2. Sync_Reference_Data.sql
       3. Insert_Unified_tables.sql

Note: Replace @Carrier_id with actual UniUni carrier_id from carrier table
================================================================================
*/

DECLARE @Carrier_id INT;

-- Get UniUni carrier_id
SELECT @Carrier_id = carrier_id
FROM dbo.carrier
WHERE carrier_name = 'UniUni';

-- Validation Query
WITH file_total AS (
    SELECT 
        SUM(CAST([Total Billed Amount] AS DECIMAL(18,2))) AS expected
    FROM 
        test.delta_uniuni_bill
),
charges_total AS (
    SELECT 
        SUM(sc.amount) AS actual
    FROM 
        Test.shipment_charges sc
        JOIN Test.shipment_attributes sa ON sa.id = sc.shipment_attribute_id
    WHERE 
        sa.carrier_id = @Carrier_id
)
SELECT 
    expected,
    actual,
    ABS(expected - actual) AS difference,
    CASE 
        WHEN ABS(expected - actual) < 0.01 THEN '✅ PASS' 
        ELSE '❌ FAIL' 
    END AS result
FROM 
    file_total, charges_total;

/*
================================================================================
Additional Validation Queries
================================================================================
*/

-- Count validation: Total tracking numbers should match
SELECT 
    'Tracking Number Count Validation' AS test,
    COUNT(DISTINCT [Tracking Number]) AS expected_from_file,
    (SELECT COUNT(*) FROM Test.shipment_attributes WHERE carrier_id = @Carrier_id) AS actual_in_attributes,
    CASE 
        WHEN COUNT(DISTINCT [Tracking Number]) = (SELECT COUNT(*) FROM Test.shipment_attributes WHERE carrier_id = @Carrier_id)
        THEN '✅ PASS' 
        ELSE '❌ FAIL' 
    END AS result
FROM 
    test.delta_uniuni_bill;

-- Invoice count validation
SELECT 
    'Invoice Count Validation' AS test,
    COUNT(DISTINCT [Invoice Number]) AS expected_from_file,
    (SELECT COUNT(*) FROM Test.carrier_bill WHERE carrier_id = @Carrier_id) AS actual_in_carrier_bill,
    CASE 
        WHEN COUNT(DISTINCT [Invoice Number]) = (SELECT COUNT(*) FROM Test.carrier_bill WHERE carrier_id = @Carrier_id)
        THEN '✅ PASS' 
        ELSE '❌ FAIL' 
    END AS result
FROM 
    test.delta_uniuni_bill;
