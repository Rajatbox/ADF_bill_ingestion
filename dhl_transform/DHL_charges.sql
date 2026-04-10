/*
================================================================================
View: DHL Charges Unpivot (Wide to Narrow Format)
================================================================================
Purpose: Unpivots DHL bill wide format (38 charge columns) into narrow format
         for easier processing.

Usage: Used by Sync_Reference_Data.sql and Insert_Unified_tables.sql
       Filtered by file_id to process only current file's charges

Design: Uses OUTER APPLY to unpivot static charge column/amount pairs.
        Unlike FedEx (dynamic charge descriptions from data), DHL has fixed
        columns -- the charge type names here are the single source of truth.
        Joins carrier_bill to expose file_id for file-based filtering.
        Includes resolved tracking number (overlabel-preferred, then Column 20 fallback).
        
Output: One row per charge per tracking number (narrow format)
================================================================================
*/
CREATE VIEW billing.vw_DHLCharges
AS
SELECT
    dhl.carrier_bill_id,
    dhl.invoice_number,
    CASE 
        WHEN NULLIF(TRIM(dhl.overlabel_tracking_number), '') IS NOT NULL
            THEN dhl.overlabel_tracking_number
        WHEN UPPER(TRIM(dhl.recipient_country)) = 'US'
            THEN dhl.domestic_tracking_number
        ELSE dhl.international_tracking_number
    END AS tracking_number,
    dhl.created_date,
    cb.file_id,
    v.charge_type,
    v.charge_amount
FROM billing.dhl_bill dhl
JOIN billing.carrier_bill cb ON cb.carrier_bill_id = dhl.carrier_bill_id
OUTER APPLY (
    VALUES
        (N'Transportation Cost',            dhl.transportation_cost),
        (N'Workshare Dropoff',              dhl.workshare_dropoff),
        (N'Workshare Sort',                 dhl.workshare_sort),
        (N'Workshare Stamp',                dhl.workshare_stamp),
        (N'Workshare Machine',              dhl.workshare_machine),
        (N'Workshare Manifest',             dhl.workshare_manifest),
        (N'Workshare Bpm',                  dhl.workshare_bpm),
        (N'Surcharge Content Endorse',      dhl.surcharge_content_endorse),
        (N'Surcharge Unassignable Add',     dhl.surcharge_unassignable_add),
        (N'Surcharge Special Handling',     dhl.surcharge_special_handling),
        (N'Surcharge Late Arrival',         dhl.surcharge_late_arrival),
        (N'Surcharge Usps Qualif',          dhl.surcharge_usps_qualif),
        (N'Surcharge Client Srd',           dhl.surcharge_client_srd),
        (N'Non-Qualified Dim',              dhl.non_qualified_dimensional_charges),
        (N'Returned Mail Unassignable',     dhl.returned_mail_unassignable),
        (N'Returned Mail Unprocessable',    dhl.returned_mail_unprocessable),
        (N'Returned Mail Recall',           dhl.returned_mail_recall),
        (N'Returned Mail Duplicate',        dhl.returned_mail_duplicate),
        (N'Returned Mail Cont Assur',       dhl.returned_mail_cont_assur),
        (N'Returned Mail Move Update',      dhl.returned_mail_move_update),
        (N'Gst Tax',                        dhl.gst_tax),
        (N'Hst Tax',                        dhl.hst_tax),
        (N'Pst Tax',                        dhl.pst_tax),
        (N'Vat Tax',                        dhl.vat_tax),
        (N'Duties',                         dhl.duties),
        (N'Other Tax',                      dhl.other_tax),
        (N'Returned Mail Paper Invoice',    dhl.returned_mail_paper_invoice),
        (N'Returned Mail Screening',        dhl.returned_mail_screening),
        (N'Returned Mail Non Auto Flats',   dhl.returned_mail_non_auto_flats),
        (N'Xb Customs Surcharge',           dhl.xb_customs_surcharge),
        (N'Fuel Surcharge',                 dhl.fuel_surcharge_amount),
        (N'Min Pickup Charge',              dhl.min_pickup_charge),
        (N'Peak Surcharge',                 dhl.peak_surcharge),
        (N'Broker Fee',                     dhl.broker_fee),
        (N'Extra Length Surcharge',         dhl.extra_length_surcharge),
        (N'Extra Volume Surcharge',         dhl.extra_volume_surcharge),
        (N'Delivery Area Surcharge',        dhl.delivery_area_surcharge_amount),
        (N'Dangerous Goods Charge',         dhl.dangerous_goods_charge)
) v (charge_type, charge_amount)
WHERE v.charge_amount IS NOT NULL
  AND v.charge_amount <> 0;
