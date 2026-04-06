/*
================================================================================
Validation Test: DHL Total Reconciliation (Delta -> dhl_bill -> shipment_charges)
================================================================================
Compares the sum of all 38 charge columns across all 3 tables to verify
no money is lost or duplicated through the pipeline.

Replace @Carrier_id with the actual DHL carrier_id value.
================================================================================
*/

DECLARE @Carrier_id INT = /* replace with actual DHL carrier_id */;

WITH delta_total AS (
    SELECT 
        SUM(
            ISNULL(CAST(NULLIF(TRIM(d.charge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.workshare_dropoff), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.workshare_sort), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.workshare_stamp), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.workshare_machine), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.workshare_manifest), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.workshare_bpm), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_content_endorse), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_unassignable_add), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_special_handling), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_late_arrival), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_usps_qualif), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_client_srd), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_nqd), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_unassignable), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_unprocessable), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_recall), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_duplicate), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_cont_assur), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_move_update), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.gst_tax), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.hst_tax), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.pst_tax), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.vat_tax), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.duties), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.other_tax), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_paper_invoice), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_screening), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.returned_mail_non_auto_flats), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.xb_customs_surcharge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.surcharge_fuel), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.min_pickup_charge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.peak_surcharge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.broker_fee), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.extra_length_surcharge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.extra_volume_surcharge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.delivery_area_surcharge), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.dangerous_goods_charge), '') AS decimal(18,2)), 0)
        ) AS total
    FROM billing.delta_dhl_bill d
),
dhl_bill_total AS (
    SELECT 
        SUM(
            ISNULL(dhl.transportation_cost, 0)
            + ISNULL(dhl.workshare_dropoff, 0)
            + ISNULL(dhl.workshare_sort, 0)
            + ISNULL(dhl.workshare_stamp, 0)
            + ISNULL(dhl.workshare_machine, 0)
            + ISNULL(dhl.workshare_manifest, 0)
            + ISNULL(dhl.workshare_bpm, 0)
            + ISNULL(dhl.surcharge_content_endorse, 0)
            + ISNULL(dhl.surcharge_unassignable_add, 0)
            + ISNULL(dhl.surcharge_special_handling, 0)
            + ISNULL(dhl.surcharge_late_arrival, 0)
            + ISNULL(dhl.surcharge_usps_qualif, 0)
            + ISNULL(dhl.surcharge_client_srd, 0)
            + ISNULL(dhl.non_qualified_dimensional_charges, 0)
            + ISNULL(dhl.returned_mail_unassignable, 0)
            + ISNULL(dhl.returned_mail_unprocessable, 0)
            + ISNULL(dhl.returned_mail_recall, 0)
            + ISNULL(dhl.returned_mail_duplicate, 0)
            + ISNULL(dhl.returned_mail_cont_assur, 0)
            + ISNULL(dhl.returned_mail_move_update, 0)
            + ISNULL(dhl.gst_tax, 0)
            + ISNULL(dhl.hst_tax, 0)
            + ISNULL(dhl.pst_tax, 0)
            + ISNULL(dhl.vat_tax, 0)
            + ISNULL(dhl.duties, 0)
            + ISNULL(dhl.other_tax, 0)
            + ISNULL(dhl.returned_mail_paper_invoice, 0)
            + ISNULL(dhl.returned_mail_screening, 0)
            + ISNULL(dhl.returned_mail_non_auto_flats, 0)
            + ISNULL(dhl.xb_customs_surcharge, 0)
            + ISNULL(dhl.fuel_surcharge_amount, 0)
            + ISNULL(dhl.min_pickup_charge, 0)
            + ISNULL(dhl.peak_surcharge, 0)
            + ISNULL(dhl.broker_fee, 0)
            + ISNULL(dhl.extra_length_surcharge, 0)
            + ISNULL(dhl.extra_volume_surcharge, 0)
            + ISNULL(dhl.delivery_area_surcharge_amount, 0)
            + ISNULL(dhl.dangerous_goods_charge, 0)
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
    CASE WHEN ABS(dt.total - db.total) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS delta_to_bill,
    CASE WHEN ABS(db.total - sc.total) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS bill_to_charges,
    CASE WHEN ABS(dt.total - sc.total) < 0.01 THEN 'PASS - ALL MATCH' ELSE 'FAIL - MISMATCH' END AS end_to_end
FROM delta_total dt, dhl_bill_total db, charges_total sc;
