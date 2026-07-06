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

-- ============================================================
-- Phase 3: Shippo delta + silver tables
-- ============================================================
-- Note: Shippo export contains 24 columns — no from_*/to_* address columns.
-- label_url is VARCHAR(MAX) because signed CloudFront URLs exceed 255 chars.

IF OBJECT_ID('billing.delta_shippo_bill', 'U') IS NULL
BEGIN
    CREATE TABLE billing.delta_shippo_bill (
        [object_id]               VARCHAR(255) NULL,
        [object_created]          VARCHAR(255) NULL,
        [object_updated]          VARCHAR(255) NULL,
        [status]                  VARCHAR(255) NULL,
        [tracking_number]         VARCHAR(255) NULL,
        [tracking_status]         VARCHAR(255) NULL,
        [tracking_url_provider]   VARCHAR(255) NULL,
        [eta]                     VARCHAR(255) NULL,
        [label_url]               VARCHAR(MAX)  NULL,
        [label_file_type]         VARCHAR(255) NULL,
        [metadata]                VARCHAR(255) NULL,
        [test]                    VARCHAR(255) NULL,
        [rate_amount]             VARCHAR(255) NULL,
        [rate_currency]           VARCHAR(255) NULL,
        [rate_provider]           VARCHAR(255) NULL,
        [rate_servicelevel_name]  VARCHAR(255) NULL,
        [rate_servicelevel_token] VARCHAR(255) NULL,
        [rate_carrier_account]    VARCHAR(255) NULL,
        [parcel_weight]           VARCHAR(255) NULL,
        [parcel_length]           VARCHAR(255) NULL,
        [parcel_width]            VARCHAR(255) NULL,
        [parcel_height]           VARCHAR(255) NULL,
        [parcel_distance_unit]    VARCHAR(255) NULL,
        [parcel_mass_unit]        VARCHAR(255) NULL
    );
    PRINT 'Created billing.delta_shippo_bill';
END
ELSE
    PRINT 'billing.delta_shippo_bill already exists — skipped';

IF OBJECT_ID('billing.shippo_bill', 'U') IS NULL
BEGIN
    CREATE TABLE billing.shippo_bill (
        id                      INT IDENTITY(1,1) NOT NULL,
        carrier_bill_id         INT              NULL,
        object_id               NVARCHAR(100)    NOT NULL,
        object_created          DATETIME2        NULL,
        tracking_number         NVARCHAR(255)    NULL,
        tracking_status         NVARCHAR(50)     NULL,
        rate_amount             DECIMAL(18,2)    NULL,
        rate_currency           NVARCHAR(10)     NULL,
        rate_provider           NVARCHAR(100)    NULL,
        rate_servicelevel_name  NVARCHAR(255)    NULL,
        rate_servicelevel_token NVARCHAR(255)    NULL,
        rate_carrier_account    NVARCHAR(100)    NULL,
        parcel_weight           DECIMAL(18,4)    NULL,
        parcel_length           DECIMAL(18,4)    NULL,
        parcel_width            DECIMAL(18,4)    NULL,
        parcel_height           DECIMAL(18,4)    NULL,
        parcel_distance_unit    NVARCHAR(10)     NULL,
        parcel_mass_unit        NVARCHAR(10)     NULL,
        created_date            DATETIME2        DEFAULT SYSDATETIME() NOT NULL,

        CONSTRAINT PK_shippo_bill PRIMARY KEY (id),
        CONSTRAINT FK_shippo_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
            REFERENCES billing.carrier_bill(carrier_bill_id)
    );

    CREATE NONCLUSTERED INDEX IX_shippo_bill_carrier_bill_id
        ON billing.shippo_bill (carrier_bill_id);

    CREATE NONCLUSTERED INDEX IX_shippo_bill_object_id
        ON billing.shippo_bill (object_id);

    CREATE NONCLUSTERED INDEX IX_shippo_bill_tracking_number
        ON billing.shippo_bill (tracking_number);

    PRINT 'Created billing.shippo_bill and indexes';
END
ELSE
    PRINT 'billing.shippo_bill already exists — skipped';

PRINT 'Migration complete.';
