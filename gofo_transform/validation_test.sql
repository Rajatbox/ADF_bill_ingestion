/*
================================================================================
Validation Test: GOFO File Total Reconciliation (carrier_bill -> shipment_charges)
================================================================================
Compares carrier_bill.total_amount (SUM of delta [Total] for the file) against
the sum of shipment_charges.amount for the same file, to verify no money is
lost or duplicated through the pipeline.

Replace @File_id with the actual file_id processed for this GOFO file.
================================================================================
*/

DECLARE @File_id INT = /* replace with actual file_id */;

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
    expected,
    actual,
    ABS(expected - actual) AS difference,
    CASE WHEN ABS(expected - actual) < 0.01
         THEN 'PASS'
         ELSE 'FAIL'
    END AS result
FROM file_total, charges_total;
