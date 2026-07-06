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
    - LineItemsInserted: INT - Number of veho_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_veho_bill
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_veho_bill (ELT staging)
            into veho_bill (Carrier Bill line items) with carrier_bill_id foreign key

Source:   billing.delta_veho_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.veho_bill (line items)

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Validation: Fails if tracking_number is NULL or empty (fail-fast CAST)
Match:      Step 1: bill_number + bill_date + carrier_id (INSERT WHERE NOT EXISTS)
            Step 2: carrier_bill_id only (INSERT WHERE NOT EXISTS) per Design Constraint #9
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;  -- Automatically rollback on error

DECLARE @InvoicesInserted INT, @LineItemsInserted INT;

BEGIN TRANSACTION;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Invoice-Level Summary Data
    ================================================================================
    Aggregates line items by Invoice Number to create invoice-level summaries
    in carrier_bill. Uses Invoice Total (stripped of commas) as the invoice
    total amount. Account Number column holds value '1123'.
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
        -- Invoice Total has commas (e.g., "9,266.78") — strip before casting
        CAST(REPLACE(TRIM(d.[Invoice Total]), ',', '') AS DECIMAL(18,2)) AS total_amount,
        COUNT(DISTINCT d.[Tracking ID]) AS num_shipments,
        CAST(TRIM(d.[Account Number]) AS VARCHAR(100)) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_veho_bill AS d
    GROUP BY
        d.[Invoice Number],
        CAST(TRIM(d.[Invoice Date]) AS DATE),
        CAST(REPLACE(TRIM(d.[Invoice Total]), ',', '') AS DECIMAL(18,2)),
        d.[Account Number]
    HAVING
        NOT EXISTS (
            SELECT 1
            FROM billing.carrier_bill AS cb
            WHERE cb.file_id = @File_id  -- File-based idempotency: same file = same data
        );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment line items from delta_veho_bill into veho_bill.
    Each row represents one tracking ID with all its associated charges and
    dimensional data. [Created Timestamp] format: "June 21, 2026, 6:18 AM"
    is parsed using CONVERT with style 100.

    Charge slots: 3 slots (Charge Code 1/2/3) stored as-is — name, code, amount.
    shipping_method derived from [Charge Name 1] where [Charge Code 1] != 'ADC'.
    ================================================================================
    */

    INSERT INTO billing.veho_bill (
        carrier_bill_id,
        invoice_date,
        invoice_number,
        tracking_id,
        package_id,
        shipment_date,
        tendered_timestamp,
        origin_zip,
        injection_market,
        delivery_market,
        delivery_zip,
        [zone],
        actual_weight_lb,
        billable_weight_lb,
        dim_length_in,
        dim_width_in,
        dim_height_in,
        external_id,
        bar_code,
        total_rate,
        invoice_total,
        account_number,
        shipping_method,
        charge_name_1,
        charge_code_1,
        charge_amount_1,
        charge_name_2,
        charge_code_2,
        charge_amount_2,
        charge_name_3,
        charge_code_3,
        charge_amount_3,
        ship_from_street,
        ship_from_city,
        ship_from_state,
        ship_from_zip,
        ship_to_street,
        ship_to_city,
        ship_to_state,
        ship_to_zip
    )
    SELECT
        cb.carrier_bill_id,
        CAST(TRIM(d.[Invoice Date]) AS DATE) AS invoice_date,
        CAST(TRIM(d.[Invoice Number]) AS NVARCHAR(100)) AS invoice_number,
        CAST(TRIM(d.[Tracking ID]) AS NVARCHAR(255)) AS tracking_id,
        CAST(NULLIF(TRIM(d.[Package ID]), '') AS NVARCHAR(255)) AS package_id,
        -- "June 21, 2026, 6:18 AM" — full month name + commas; PARSE handles free-form en-US
        PARSE(TRIM(d.[Created Timestamp]) AS DATETIME USING 'en-US') AS shipment_date,
        PARSE(NULLIF(TRIM(d.[Tendered Timestamp]), '') AS DATETIME USING 'en-US') AS tendered_timestamp,
        CAST(NULLIF(TRIM(d.[Origin Zip]), '') AS NVARCHAR(20)) AS origin_zip,
        CAST(NULLIF(TRIM(d.[Injection Market]), '') AS NVARCHAR(100)) AS injection_market,
        CAST(NULLIF(TRIM(d.[Delivery Market]), '') AS NVARCHAR(100)) AS delivery_market,
        CAST(NULLIF(TRIM(d.[Delivery Zip]), '') AS NVARCHAR(20)) AS delivery_zip,
        CAST(NULLIF(TRIM(d.[Zone]), '') AS INT) AS [zone],
        CAST(NULLIF(TRIM(d.[Actual Weight]), '') AS DECIMAL(18,4)) AS actual_weight_lb,
        CAST(NULLIF(TRIM(d.[Billable Weight]), '') AS DECIMAL(18,4)) AS billable_weight_lb,
        CAST(NULLIF(TRIM(d.[Length]), '') AS DECIMAL(18,4)) AS dim_length_in,
        CAST(NULLIF(TRIM(d.[Width]), '') AS DECIMAL(18,4)) AS dim_width_in,
        CAST(NULLIF(TRIM(d.[Height]), '') AS DECIMAL(18,4)) AS dim_height_in,
        CAST(NULLIF(TRIM(d.[External ID]), '') AS NVARCHAR(255)) AS external_id,
        CAST(NULLIF(TRIM(d.[Bar Code]), '') AS NVARCHAR(255)) AS bar_code,
        CAST(NULLIF(TRIM(d.[Total Rate]), '') AS DECIMAL(18,2)) AS total_rate,
        CAST(REPLACE(TRIM(d.[Invoice Total]), ',', '') AS DECIMAL(18,2)) AS invoice_total,
        CAST(TRIM(d.[Account Number]) AS NVARCHAR(50)) AS account_number,
        -- shipping_method: derived from Charge Name 1 when Charge Code 1 is NOT 'ADC'
        CASE
            WHEN UPPER(TRIM(d.[Charge Code 1])) = 'ADC' THEN NULL
            ELSE CAST(NULLIF(TRIM(d.[Charge Name 1]), '') AS NVARCHAR(255))
        END AS shipping_method,
        CAST(NULLIF(TRIM(d.[Charge Name 1]), '') AS NVARCHAR(255)) AS charge_name_1,
        CAST(NULLIF(TRIM(d.[Charge Code 1]), '') AS NVARCHAR(50)) AS charge_code_1,
        CAST(ISNULL(NULLIF(TRIM(d.[Charge Code 1 Amount]), ''), '0') AS DECIMAL(18,2)) AS charge_amount_1,
        CAST(NULLIF(TRIM(d.[Charge Name 2]), '') AS NVARCHAR(255)) AS charge_name_2,
        CAST(NULLIF(TRIM(d.[Charge Code 2]), '') AS NVARCHAR(50)) AS charge_code_2,
        CAST(ISNULL(NULLIF(TRIM(d.[Charge Code 2 Amount]), ''), '0') AS DECIMAL(18,2)) AS charge_amount_2,
        CAST(NULLIF(TRIM(d.[Charge Name 3]), '') AS NVARCHAR(255)) AS charge_name_3,
        CAST(NULLIF(TRIM(d.[Charge Code 3]), '') AS NVARCHAR(50)) AS charge_code_3,
        CAST(ISNULL(NULLIF(TRIM(d.[Charge Code 3 Amount]), ''), '0') AS DECIMAL(18,2)) AS charge_amount_3,
        CAST(NULLIF(TRIM(d.[Ship From Street]), '') AS NVARCHAR(500)) AS ship_from_street,
        CAST(NULLIF(TRIM(d.[Ship From City]), '') AS NVARCHAR(100)) AS ship_from_city,
        CAST(NULLIF(TRIM(d.[Ship From State]), '') AS NVARCHAR(50)) AS ship_from_state,
        CAST(NULLIF(TRIM(d.[Ship From Zip]), '') AS NVARCHAR(20)) AS ship_from_zip,
        CAST(NULLIF(TRIM(d.[Ship To Street]), '') AS NVARCHAR(500)) AS ship_to_street,
        CAST(NULLIF(TRIM(d.[Ship To City]), '') AS NVARCHAR(100)) AS ship_to_city,
        CAST(NULLIF(TRIM(d.[Ship To State]), '') AS NVARCHAR(50)) AS ship_to_state,
        CAST(NULLIF(TRIM(d.[Ship To Zip]), '') AS NVARCHAR(20)) AS ship_to_zip
    FROM
        billing.delta_veho_bill AS d
    INNER JOIN billing.carrier_bill AS cb
        ON cb.bill_number = CAST(TRIM(d.[Invoice Number]) AS VARCHAR(100))
        AND cb.bill_date = CAST(TRIM(d.[Invoice Date]) AS DATE)
        AND cb.carrier_id = @Carrier_id
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM billing.veho_bill AS vb
            WHERE vb.carrier_bill_id = cb.carrier_bill_id  -- Single field check per Design Constraint #9
        );

    SET @LineItemsInserted = @@ROWCOUNT;

    COMMIT TRANSACTION;

    -- Return success results
    SELECT
        'SUCCESS' AS Status,
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    -- Rollback on error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    -- Build descriptive error message
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'Veho Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    -- Return error details
    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
