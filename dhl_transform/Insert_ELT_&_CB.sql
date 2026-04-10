/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of dhl_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_dhl_bill 
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_dhl_bill into dhl_bill
            with carrier_bill_id foreign key

Source:   billing.delta_dhl_bill  (bronze -- DHL official column names)
Targets:  billing.carrier_bill    (invoice summaries with file_id)
          billing.dhl_bill        (silver -- our custom column names)

Bronze-to-Silver Mapping (this script):
  delta_dhl_bill.charge              -> dhl_bill.transportation_cost
  delta_dhl_bill.surcharge_nqd       -> dhl_bill.non_qualified_dimensional_charges
  delta_dhl_bill.surcharge_fuel      -> dhl_bill.fuel_surcharge_amount
  delta_dhl_bill.delivery_area_surcharge -> dhl_bill.delivery_area_surcharge_amount
  delta_dhl_bill.dangerous_goods_charge  -> dhl_bill.dangerous_goods_charge
  delta_dhl_bill.overlabeled_value      -> dhl_bill.overlabel_tracking_number
  (34 new charge columns keep same name in both layers)

Tracking Number Logic (applied in Step 2):
  - international_tracking_number: customer_confirm (Col 12) saved as-is
  - domestic_tracking_number:      '420' + LEFT(zip, 5) + delivery_confirm (Col 13)
  - overlabel_tracking_number:     '420' + LEFT(zip, 5) + overlabeled_value (Col 67)

Carrier Bill Total: SUM of all 38 charge columns from delta.
                    Computed from DTL rows (no HDR row in delta table).

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Transaction: Both inserts wrapped in transaction for atomicity
Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @InvoicesInserted INT, @LineItemsInserted INT;

BEGIN TRANSACTION;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Invoice-Level Summary Data
    ================================================================================
    Aggregates DTL rows by invoice_number and invoice_date to create invoice-level
    summaries in carrier_bill. Total = SUM of all 38 charge columns.
    
    No HDR row in delta table; total is computed from DTL rows.
    Generates carrier_bill_id values which will be joined in Step 2.
    ================================================================================
    */

    INSERT INTO billing.carrier_bill (
        carrier_id,
        bill_number,
        bill_date,
        total_amount,
        num_shipments,
        account_number,
        file_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        CAST(d.invoice_number AS nvarchar(50)) AS bill_number,
        CAST(NULLIF(TRIM(d.invoice_date), '') AS date) AS bill_date,
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
        ) AS total_amount,
        COUNT(*) AS num_shipments,
        MAX(d.sold_to) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_dhl_bill AS d
    WHERE
        d.invoice_number IS NOT NULL
        AND NULLIF(TRIM(d.invoice_date), '') IS NOT NULL
    GROUP BY
        CAST(d.invoice_number AS nvarchar(50)),
        CAST(NULLIF(TRIM(d.invoice_date), '') AS date)
    HAVING
        NOT EXISTS (
            SELECT 1
            FROM billing.carrier_bill AS cb
            WHERE cb.file_id = @File_id
        );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Each DTL row represents one shipment. Inserts into dhl_bill with:
    - carrier_bill_id FK from Step 1
    - Tracking numbers: customer_confirm (Col 12) and delivery_confirm (Col 13)
    - All 38 charge columns mapped from DHL names (delta) to silver names (dhl_bill)
    - Column order matches real bill sequence
    
    NOT EXISTS check uses carrier_bill_id (Design Constraint #4).
    ================================================================================
    */

    INSERT INTO billing.dhl_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        shipping_date,
        international_tracking_number,
        domestic_tracking_number,
        recipient_zip_postal_code,
        recipient_country,
        shipping_method,
        shipped_weight,
        shipped_weight_unit,
        billed_weight,
        billed_weight_unit,
        [zone],
        transportation_cost,
        workshare_dropoff,
        workshare_sort,
        workshare_stamp,
        workshare_machine,
        workshare_manifest,
        workshare_bpm,
        surcharge_content_endorse,
        surcharge_unassignable_add,
        surcharge_special_handling,
        surcharge_late_arrival,
        surcharge_usps_qualif,
        surcharge_client_srd,
        non_qualified_dimensional_charges,
        returned_mail_unassignable,
        returned_mail_unprocessable,
        returned_mail_recall,
        returned_mail_duplicate,
        returned_mail_cont_assur,
        returned_mail_move_update,
        gst_tax,
        hst_tax,
        pst_tax,
        vat_tax,
        duties,
        other_tax,
        returned_mail_paper_invoice,
        returned_mail_screening,
        returned_mail_non_auto_flats,
        xb_customs_surcharge,
        fuel_surcharge_amount,
        min_pickup_charge,
        overlabel_tracking_number,
        peak_surcharge,
        broker_fee,
        extra_length_surcharge,
        extra_volume_surcharge,
        delivery_area_surcharge_amount,
        dangerous_goods_charge
    )
    SELECT 
        cb.carrier_bill_id,
        CAST(d.invoice_number AS nvarchar(50)) AS invoice_number,
        CAST(NULLIF(TRIM(d.invoice_date), '') AS date) AS invoice_date,
        CAST(NULLIF(TRIM(d.pickup_date), '') AS date) AS shipping_date,
        TRIM(CAST(d.customer_confirm AS nvarchar(255))),
        '420' + LEFT(REPLACE(CAST(d.recipient_zip AS varchar(50)), ' ', ''), 5)
             + TRIM(CAST(d.delivery_confirm AS varchar(255))) AS domestic_tracking_number,
        CAST(d.recipient_zip AS nvarchar(255)),
        CAST(d.recipient_country AS nvarchar(10)),
        CAST(d.material_or_vas_desc AS nvarchar(350)),
        CAST(NULLIF(TRIM(d.actual_weight), '') AS decimal(18,2)),
        CAST(d.uom_actual_weight AS nvarchar(10)),
        CAST(NULLIF(TRIM(d.billing_weight), '') AS decimal(18,2)),
        CAST(d.uom_billing_weight AS nvarchar(10)),
        CAST(d.pricing_zone AS nvarchar(255)),
        -- Bronze -> Silver charge mapping (38 charges)
        CAST(NULLIF(TRIM(d.charge), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.workshare_dropoff), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.workshare_sort), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.workshare_stamp), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.workshare_machine), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.workshare_manifest), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.workshare_bpm), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_content_endorse), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_unassignable_add), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_special_handling), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_late_arrival), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_usps_qualif), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_client_srd), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_nqd), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_unassignable), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_unprocessable), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_recall), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_duplicate), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_cont_assur), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_move_update), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.gst_tax), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.hst_tax), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.pst_tax), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.vat_tax), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.duties), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.other_tax), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_paper_invoice), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_screening), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.returned_mail_non_auto_flats), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.xb_customs_surcharge), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.surcharge_fuel), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.min_pickup_charge), '') AS decimal(18,2)),
        CASE WHEN NULLIF(TRIM(CAST(d.overlabeled_value AS varchar(255))), '') IS NOT NULL
             THEN '420' + LEFT(REPLACE(CAST(d.recipient_zip AS varchar(50)), ' ', ''), 5)
                        + TRIM(CAST(d.overlabeled_value AS varchar(255)))
        END AS overlabel_tracking_number,
        CAST(NULLIF(TRIM(d.peak_surcharge), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.broker_fee), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.extra_length_surcharge), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.extra_volume_surcharge), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.delivery_area_surcharge), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.dangerous_goods_charge), '') AS decimal(18,2))
    FROM billing.delta_dhl_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number = CAST(d.invoice_number AS nvarchar(50))
        AND cb.bill_date = CAST(NULLIF(TRIM(d.invoice_date), '') AS date)
        AND cb.carrier_id = @Carrier_id
    WHERE d.invoice_number IS NOT NULL
      AND NULLIF(TRIM(d.invoice_date), '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM billing.dhl_bill t
          WHERE t.carrier_bill_id = cb.carrier_bill_id
      );

    SET @LineItemsInserted = @@ROWCOUNT;

    COMMIT TRANSACTION;
    
    SELECT 
        'SUCCESS' AS Status, 
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        'DHL Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW 50000, @DetailedError, 1;
END CATCH;
