/*
================================================================================
Migration: Add overlabel_tracking_number to dhl_bill
================================================================================
Purpose: DHL Column 67 (overlabeled_value) contains an alternate tracking number
         used by WMS for last-mile delivery. Previously discarded at the silver
         layer, now carried through so tracking resolution can prefer it over the
         Column 20 (domestic/international) logic when present.

Affected scripts (updated in this release):
  - dhl_transform/Insert_ELT_&_CB.sql      (maps bronze → silver)
  - dhl_transform/DHL_charges.sql           (vw_DHLCharges tracking resolution)
  - dhl_transform/Insert_Unified_tables.sql (shipment_attributes tracking resolution)

Idempotent: Safe to run multiple times.
================================================================================
*/

SET NOCOUNT ON;

-- Add overlabel_tracking_number column to dhl_bill (Col 67)
IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.dhl_bill')
      AND name = 'overlabel_tracking_number'
)
BEGIN
    ALTER TABLE billing.dhl_bill
    ADD overlabel_tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;

    PRINT 'Added overlabel_tracking_number to billing.dhl_bill';
END
ELSE
BEGIN
    PRINT 'Column overlabel_tracking_number already exists on billing.dhl_bill — skipped';
END

-- Recreate vw_DHLCharges with updated tracking resolution (overlabel-preferred)
IF OBJECT_ID('billing.vw_DHLCharges', 'V') IS NOT NULL
    DROP VIEW billing.vw_DHLCharges;
GO

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
GO

PRINT 'Migration complete: DHL overlabel tracking number support enabled';
GO
