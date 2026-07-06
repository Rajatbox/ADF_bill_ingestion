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
    - LineItemsInserted: INT - Number of shippo_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_shippo_bill
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_shippo_bill (ELT staging)
            into billing.shippo_bill (Carrier Bill line items) with carrier_bill_id FK

Invoice Number Generation:
         invoice_number = 'Shippo_' + yyyy-MM-dd from MAX(object_created)
         Example: "Shippo_2026-06-29"

         bill_date = MAX(object_created AS DATE)

         Note: Single invoice per file using latest label timestamp's date portion.
               All SUCCESS rows in the file aggregate to one carrier_bill record.

Status Filter: Only rows with status = 'SUCCESS' are processed.
               ERROR rows have no tracking number and represent failed label attempts.

Source:   billing.delta_shippo_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.shippo_bill (line items)

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Validation: Fails if object_created or tracking_number is NULL or empty
Match:      Step 1: file_id (INSERT WHERE NOT EXISTS)
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
    Aggregates all SUCCESS label rows into one invoice per file.

    Calculates:
    - bill_number: 'Shippo_' + FORMAT of MAX(object_created) date
    - bill_date: MAX(object_created) as DATE
    - total_amount: SUM of rate_amount for SUCCESS rows
    - num_shipments: COUNT of tracking numbers
    - account_number: rate_carrier_account (consistent across file)

    Invoice Grouping Strategy: All SUCCESS rows in file grouped under a single invoice.
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
        'Shippo_' + CONVERT(VARCHAR(10), MAX(CAST(REPLACE(TRIM(d.[object_created]), 'Z', '') AS DATE)), 23) AS bill_number,
        MAX(CAST(REPLACE(TRIM(d.[object_created]), 'Z', '') AS DATE)) AS bill_date,
        SUM(CAST(TRIM(d.[rate_amount]) AS DECIMAL(18,2))) AS total_amount,
        COUNT(d.[tracking_number]) AS num_shipments,
        MAX(TRIM(d.[rate_carrier_account])) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_shippo_bill AS d
    WHERE
        UPPER(TRIM(d.[status])) = 'SUCCESS'
        AND NULLIF(TRIM(d.[tracking_number]), '') IS NOT NULL
        AND d.[object_created] IS NOT NULL
        AND NULLIF(TRIM(d.[object_created]), '') IS NOT NULL
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill AS cb
        WHERE cb.file_id = @File_id  -- File-based idempotency: same file = same data
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual label records from delta_shippo_bill into billing.shippo_bill.
    Only SUCCESS rows with a valid tracking number are inserted.

    object_created format: ISO 8601 with Z suffix (2026-06-29T12:51:54.133Z)
    — strip Z then CAST as DATETIME2.

    Join Strategy:
    - Join to carrier_bill on carrier_id and file_id (just inserted in Step 1)
    - One invoice per file, so this matches exactly one carrier_bill row
    - Use carrier_bill_id only in NOT EXISTS check (Design Constraint #9)
    ================================================================================
    */

    INSERT INTO billing.shippo_bill (
        carrier_bill_id,
        object_id,
        object_created,
        tracking_number,
        tracking_status,
        rate_amount,
        rate_currency,
        rate_provider,
        rate_servicelevel_name,
        rate_servicelevel_token,
        rate_carrier_account,
        parcel_weight,
        parcel_length,
        parcel_width,
        parcel_height,
        parcel_distance_unit,
        parcel_mass_unit
    )
    SELECT
        cb.carrier_bill_id,
        CAST(TRIM(d.[object_id]) AS NVARCHAR(100)) AS object_id,
        CAST(REPLACE(TRIM(d.[object_created]), 'Z', '') AS DATETIME2) AS object_created,
        CAST(TRIM(d.[tracking_number]) AS NVARCHAR(255)) AS tracking_number,
        CAST(NULLIF(TRIM(d.[tracking_status]), '') AS NVARCHAR(50)) AS tracking_status,
        CAST(TRIM(d.[rate_amount]) AS DECIMAL(18,2)) AS rate_amount,
        CAST(NULLIF(TRIM(d.[rate_currency]), '') AS NVARCHAR(10)) AS rate_currency,
        CAST(NULLIF(TRIM(d.[rate_provider]), '') AS NVARCHAR(100)) AS rate_provider,
        CAST(NULLIF(TRIM(d.[rate_servicelevel_name]), '') AS NVARCHAR(255)) AS rate_servicelevel_name,
        CAST(NULLIF(TRIM(d.[rate_servicelevel_token]), '') AS NVARCHAR(255)) AS rate_servicelevel_token,
        CAST(TRIM(d.[rate_carrier_account]) AS NVARCHAR(100)) AS rate_carrier_account,
        CAST(NULLIF(TRIM(d.[parcel_weight]), '') AS DECIMAL(18,4)) AS parcel_weight,
        CAST(NULLIF(TRIM(d.[parcel_length]), '') AS DECIMAL(18,4)) AS parcel_length,
        CAST(NULLIF(TRIM(d.[parcel_width]), '') AS DECIMAL(18,4)) AS parcel_width,
        CAST(NULLIF(TRIM(d.[parcel_height]), '') AS DECIMAL(18,4)) AS parcel_height,
        CAST(NULLIF(TRIM(d.[parcel_distance_unit]), '') AS NVARCHAR(10)) AS parcel_distance_unit,
        CAST(NULLIF(TRIM(d.[parcel_mass_unit]), '') AS NVARCHAR(10)) AS parcel_mass_unit
    FROM
        billing.delta_shippo_bill AS d
    INNER JOIN billing.carrier_bill AS cb
        ON cb.carrier_id = @Carrier_id
        AND cb.file_id = @File_id  -- Join to the record just inserted in Step 1
    WHERE
        UPPER(TRIM(d.[status])) = 'SUCCESS'
        AND NULLIF(TRIM(d.[tracking_number]), '') IS NOT NULL
        AND d.[object_created] IS NOT NULL
        AND NULLIF(TRIM(d.[object_created]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shippo_bill AS sb
            WHERE sb.carrier_bill_id = cb.carrier_bill_id  -- Design Constraint #9: carrier_bill_id only
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
        'Shippo Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
