/*
================================================================================
Validation Test: Speedship File Total vs Shipment Charges
================================================================================
Run after pipeline completes for a test file.

Replace placeholders:
  @File_id   — from file_ingestion_tracker
  @Carrier_id — Speedship carrier_id in dbo.carrier
================================================================================
*/

DECLARE @File_id INT = /* test file_id */;
DECLARE @Carrier_id INT = /* Speedship carrier_id */;

WITH file_total AS (
    SELECT SUM(CAST(NULLIF(TRIM(d.[Charge Total]), '') AS DECIMAL(18,2))) AS expected
    FROM billing.delta_speedship_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number = d.[Invoice #]
        AND cb.bill_date = CAST(d.[Invoice Date] AS DATE)
        AND cb.carrier_id = @Carrier_id
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
    CASE
        WHEN ABS(expected - actual) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM file_total, charges_total;
