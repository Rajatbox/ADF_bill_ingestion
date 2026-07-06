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
    - LineItemsInserted: INT - Number of shipx_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate invoice summaries from delta_shipx_bill into carrier_bill
         2. Insert typed line items into billing.shipx_bill with carrier_bill_id FK

Invoice Grouping: One carrier_bill row per distinct (Invoice Number, Invoice Date).

Source:   billing.delta_shipx_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.shipx_bill (line items)

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
    Aggregates line items by Invoice Number and Invoice Date. Total Charge is the
    authoritative per-row total. account_number sourced from Company Id.
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
        CAST(TRIM(d.[Invoice Number]) AS VARCHAR(100)) AS bill_number,
        CAST(TRIM(d.[Invoice Date]) AS DATE) AS bill_date,
        SUM(CAST(ISNULL(NULLIF(TRIM(d.[Total Charge]), ''), '0') AS DECIMAL(18,2))) AS total_amount,
        COUNT(d.[Tracking Number]) AS num_shipments,
        MAX(CAST(TRIM(d.[Company Id]) AS VARCHAR(100))) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_shipx_bill AS d
    WHERE
        d.[Invoice Number] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Number]), '') IS NOT NULL
        AND d.[Invoice Date] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
        AND d.[Tracking Number] IS NOT NULL
        AND NULLIF(TRIM(d.[Tracking Number]), '') IS NOT NULL
    GROUP BY
        CAST(TRIM(d.[Invoice Number]) AS VARCHAR(100)),
        CAST(TRIM(d.[Invoice Date]) AS DATE)
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
    Inserts individual shipment records into shipx_bill. Each row is one tracking
    number with physical attributes and charge amounts. Uses carrier_bill_id join
    per Design Constraint #9 for idempotency.
    ================================================================================
    */

    INSERT INTO billing.shipx_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        tracking_number,
        shipment_number,
        company_id,
        service_level,
        destination_zone,
        shipment_date,
        actual_delivery_date,
        weight,
        weight_uom,
        length,
        width,
        height,
        dims_uom,
        fuel_surcharge,
        delivery_charge,
        total_charge,
        currency,
        status
    )
    SELECT
        cb.carrier_bill_id,
        CAST(TRIM(d.[Invoice Number]) AS NVARCHAR(100)) AS invoice_number,
        CAST(TRIM(d.[Invoice Date]) AS DATE) AS invoice_date,
        CAST(TRIM(d.[Tracking Number]) AS NVARCHAR(255)) AS tracking_number,
        NULLIF(TRIM(d.[Shipment Number]), '') AS shipment_number,
        NULLIF(TRIM(d.[Company Id]), '') AS company_id,
        NULLIF(TRIM(d.[Service Level]), '') AS service_level,
        NULLIF(TRIM(d.[Zone]), '') AS destination_zone,
        CAST(NULLIF(TRIM(d.[Creation Date]), '') AS DATETIME2) AS shipment_date,
        CAST(NULLIF(TRIM(d.[Actual Delivery Date]), '') AS DATETIME2) AS actual_delivery_date,
        CAST(NULLIF(TRIM(d.[Weight]), '') AS DECIMAL(18,4)) AS weight,
        NULLIF(TRIM(d.[Weight UOM]), '') AS weight_uom,
        CAST(NULLIF(TRIM(d.[Length]), '') AS DECIMAL(18,4)) AS length,
        CAST(NULLIF(TRIM(d.[Width]), '') AS DECIMAL(18,4)) AS width,
        CAST(NULLIF(TRIM(d.[Height]), '') AS DECIMAL(18,4)) AS height,
        NULLIF(TRIM(d.[Dims UOM]), '') AS dims_uom,
        CAST(ISNULL(NULLIF(TRIM(d.[Fuel Surcharge]), ''), '0') AS DECIMAL(18,2)) AS fuel_surcharge,
        CAST(ISNULL(NULLIF(TRIM(d.[Delivery Charge]), ''), '0') AS DECIMAL(18,2)) AS delivery_charge,
        CAST(ISNULL(NULLIF(TRIM(d.[Total Charge]), ''), '0') AS DECIMAL(18,2)) AS total_charge,
        NULLIF(TRIM(d.[Currency]), '') AS currency,
        NULLIF(TRIM(d.[Status]), '') AS status
    FROM
        billing.delta_shipx_bill AS d
        INNER JOIN billing.carrier_bill AS cb
            ON cb.bill_number = CAST(TRIM(d.[Invoice Number]) AS VARCHAR(100))
            AND cb.bill_date = CAST(TRIM(d.[Invoice Date]) AS DATE)
            AND cb.carrier_id = @Carrier_id
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM billing.shipx_bill AS sb
            WHERE sb.carrier_bill_id = cb.carrier_bill_id
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
        'Shipx Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
