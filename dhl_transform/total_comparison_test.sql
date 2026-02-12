/*
================================================================================
Validation Test: DHL Total Reconciliation (Delta → dhl_bill → shipment_charges)
================================================================================
Compares the sum of 4 charge columns across all 3 tables to verify
no money is lost or duplicated through the pipeline.

Replace @Carrier_id with the actual DHL carrier_id value.
================================================================================
*/

DECLARE @Carrier_id INT = /* replace with actual DHL carrier_id */;

WITH delta_total AS (
    SELECT 
        SUM(
            ISNULL(CAST(NULLIF(TRIM(d.transportation_cost), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.non_qualified_dimensional_charges), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.fuel_surcharge_amount), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.delivery_area_surcharge_amount), '') AS decimal(18,2)), 0)
        ) AS total
    FROM billing.delta_dhl_bill d
),
dhl_bill_total AS (
    SELECT 
        SUM(
            ISNULL(dhl.transportation_cost, 0)
            + ISNULL(dhl.non_qualified_dimensional_charges, 0)
            + ISNULL(dhl.fuel_surcharge_amount, 0)
            + ISNULL(dhl.delivery_area_surcharge_amount, 0)
        ) AS total
    FROM billing.dhl_bill dhl
),
charges_total AS (
    SELECT SUM(sc.amount) AS total
    FROM billing.shipment_charges sc
    JOIN billing.shipment_attributes sa ON sa.id = sc.shipment_attribute_id
    WHERE sa.carrier_id = @Carrier_id
)
SELECT 
    dt.total   AS delta_total,
    db.total   AS dhl_bill_total,
    sc.total   AS shipment_charges_total,
    ABS(dt.total - db.total)  AS delta_vs_bill_diff,
    ABS(db.total - sc.total)  AS bill_vs_charges_diff,
    CASE WHEN ABS(dt.total - db.total) < 0.01 THEN '✅' ELSE '❌' END AS delta_to_bill,
    CASE WHEN ABS(db.total - sc.total) < 0.01 THEN '✅' ELSE '❌' END AS bill_to_charges,
    CASE WHEN ABS(dt.total - sc.total) < 0.01 THEN '✅ ALL MATCH' ELSE '❌ MISMATCH' END AS end_to_end
FROM delta_total dt, dhl_bill_total db, charges_total sc;

