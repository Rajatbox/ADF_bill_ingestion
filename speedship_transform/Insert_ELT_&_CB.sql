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
    - LineItemsInserted: INT - Number of speedship_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion with file tracking:
         1. Aggregate invoice summaries from delta_speedship_bill into carrier_bill
         2. Insert typed line items into billing.speedship_bill

Invoice Grouping: One carrier_bill row per distinct (Invoice #, Invoice Date).
                  Supports multiple invoices in a single file.

SCAC Mapping: UNITED PARCEL SERVICE → UPS (other SCAC values stored as-is)

Source:   billing.delta_speedship_bill
Targets:  billing.carrier_bill, billing.speedship_bill

Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @InvoicesInserted INT, @LineItemsInserted INT;

BEGIN TRANSACTION;

BEGIN TRY

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
        d.[Invoice #] AS bill_number,
        CAST(d.[Invoice Date] AS DATE) AS bill_date,
        SUM(CAST(NULLIF(TRIM(d.[Charge Total]), '') AS DECIMAL(18,2))) AS total_amount,
        COUNT(*) AS num_shipments,
        MAX(d.[Customer #]) AS account_number,
        @File_id AS file_id
    FROM billing.delta_speedship_bill AS d
    WHERE
        d.[Invoice #] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice #]), '') IS NOT NULL
        AND d.[Invoice Date] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
        AND d.[Airbill #] IS NOT NULL
        AND NULLIF(TRIM(d.[Airbill #]), '') IS NOT NULL
    GROUP BY
        d.[Invoice #],
        CAST(d.[Invoice Date] AS DATE)
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    INSERT INTO billing.speedship_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        customer_number,
        line_of_business,
        tracking_number,
        shipment_date,
        scac,
        integrated_carrier,
        bill_type,
        service_level,
        destination_zone,
        charged_weight,
        weight_unit,
        charge_total,
        charge_type_1,
        charge_amount_1,
        charge_type_2,
        charge_amount_2,
        charge_type_3,
        charge_amount_3,
        charge_type_4,
        charge_amount_4,
        charge_type_5,
        charge_amount_5,
        charge_type_6,
        charge_amount_6,
        charge_type_7,
        charge_amount_7,
        charge_type_8,
        charge_amount_8,
        billing_reference_1,
        billing_reference_2,
        customer_name
    )
    SELECT
        cb.carrier_bill_id,
        d.[Invoice #] AS invoice_number,
        CAST(d.[Invoice Date] AS DATE) AS invoice_date,
        NULLIF(TRIM(d.[Customer #]), '') AS customer_number,
        NULLIF(TRIM(d.[Line of Business]), '') AS line_of_business,
        TRIM(d.[Airbill #]) AS tracking_number,
        CAST(d.[Ship date] AS DATE) AS shipment_date,
        NULLIF(TRIM(d.[SCAC]), '') AS scac,
        CASE LOWER(LTRIM(RTRIM(d.[SCAC])))
            WHEN 'united parcel service' THEN 'UPS'
            ELSE NULLIF(LTRIM(RTRIM(d.[SCAC])), '')
        END AS integrated_carrier,
        NULLIF(TRIM(d.[Bill Type]), '') AS bill_type,
        NULLIF(TRIM(d.[Service level]), '') AS service_level,
        NULLIF(LTRIM(RTRIM(d.[ Zone])), '') AS destination_zone,
        CAST(
            LTRIM(RTRIM(
                LEFT(
                    LTRIM(RTRIM(d.[Charged Weight])),
                    NULLIF(CHARINDEX(' ', LTRIM(RTRIM(d.[Charged Weight])) + ' '), 0) - 1
                )
            ))
        AS DECIMAL(18,6)) AS charged_weight,
        UPPER(
            LTRIM(RTRIM(
                SUBSTRING(
                    LTRIM(RTRIM(d.[Charged Weight])),
                    NULLIF(CHARINDEX(' ', LTRIM(RTRIM(d.[Charged Weight]))), 0),
                    10
                )
            ))
        ) AS weight_unit,
        CAST(NULLIF(TRIM(d.[Charge Total]), '') AS DECIMAL(18,2)) AS charge_total,
        NULLIF(TRIM(d.[Charge Type 1]), '') AS charge_type_1,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 1]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 1]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 2]), '') AS charge_type_2,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 2]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 2]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 3]), '') AS charge_type_3,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 3]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 3]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 4]), '') AS charge_type_4,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 4]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 4]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 5]), '') AS charge_type_5,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 5]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 5]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 6]), '') AS charge_type_6,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 6]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 6]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 7]), '') AS charge_type_7,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 7]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 7]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Charge Type 8]), '') AS charge_type_8,
        CASE WHEN NULLIF(TRIM(d.[Charge Amount 8]), '') IS NULL THEN NULL
             ELSE CAST(NULLIF(TRIM(d.[Charge Amount 8]), '') AS DECIMAL(18,2)) END,
        NULLIF(TRIM(d.[Billing Reference 1]), '') AS billing_reference_1,
        NULLIF(TRIM(d.[Billing Reference 2]), '') AS billing_reference_2,
        NULLIF(TRIM(d.[Customer Name]), '') AS customer_name
    FROM billing.delta_speedship_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number = d.[Invoice #]
        AND cb.bill_date = CAST(d.[Invoice Date] AS DATE)
        AND cb.carrier_id = @Carrier_id
    WHERE
        d.[Invoice #] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice #]), '') IS NOT NULL
        AND d.[Invoice Date] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
        AND d.[Airbill #] IS NOT NULL
        AND NULLIF(TRIM(d.[Airbill #]), '') IS NOT NULL
        AND d.[Charged Weight] IS NOT NULL
        AND NULLIF(TRIM(d.[Charged Weight]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.speedship_bill t
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
        '[Speedship] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
