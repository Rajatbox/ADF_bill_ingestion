/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional (Vanlo)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of vanlo_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_vanlo_bill
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_vanlo_bill (ELT staging)
            into billing.vanlo_bill (Carrier Bill line items) with carrier_bill_id FK

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Invoice Number Generation:
         invoice_number = 'Vanlo_' + yyyy-MM-dd from MAX(Date)
         Example: "Vanlo_2026-03-03"
         
         invoice_date = CAST(LEFT(MAX(Date), 10) AS DATE)
         
         account_number = 'FALCON' (hardcoded - no account column in CSV)

UniUni Exclusion: Service column values starting with 'UniUni' are excluded from
         all tables beyond the delta layer. UniUni shipments are billed through
         their own separate pipeline.

Service Column Splitting: The Service column combines carrier + method
         (e.g., "USPS GroundAdvantage"). Split on first space:
         - Left part  → integrated_carrier (e.g., "USPS")
         - Right part → service_method (e.g., "GroundAdvantage")

Package Column Parsing: Format "L x W x H" (e.g., "7.5 x 6.0 x 1.25")
         Parsed using UPS CHARINDEX + SUBSTRING + CROSS APPLY pattern.

Source:   billing.delta_vanlo_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.vanlo_bill (line items)

Match:      Step 1: file_id (INSERT WHERE NOT EXISTS) — file-based idempotency
            Step 2: carrier_bill_id only (INSERT WHERE NOT EXISTS) per Design Constraint #9
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

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
    All non-UniUni shipments in file grouped under a single invoice using the
    latest timestamp's date portion.

    Calculates:
    - invoice_number: 'Vanlo_' + FORMAT(MAX(Date) as date, 'yyyy-MM-dd')
    - invoice_date: CAST(LEFT(MAX(Date), 10) AS DATE)
    - total_amount: SUM of Cost (excluding UniUni rows)
    - num_shipments: COUNT of shipments (excluding UniUni rows)
    - account_number: 'FALCON' (hardcoded)
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
        'Vanlo_' + FORMAT(CAST(LEFT(MAX(d.[Date]), 10) AS DATE), 'yyyy-MM-dd') AS bill_number,
        CAST(LEFT(MAX(d.[Date]), 10) AS DATE) AS bill_date,
        SsUM(CAST(d.[Cost] AS DECIMAL(18,2))) AS total_amount,
        COUNT(*) AS num_shipments,
        'FALCON' AS account_number,
        @File_id AS file_id
    FROM billing.delta_vanlo_bill d
    WHERE
        d.[Service] NOT LIKE 'UniUni%'
        AND NULLIF(TRIM(d.[Tracking Code]), '') IS NOT NULL
        AND NULLIF(TRIM(d.[Date]), '') IS NOT NULL
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment records from delta_vanlo_bill into billing.vanlo_bill.

    Join Strategy:
    - Join to carrier_bill on carrier_id and file_id (just inserted in Step 1)
    - Use carrier_bill_id only in NOT EXISTS check (Design Constraint #9)

    Package Parsing (UPS CHARINDEX + CROSS APPLY pattern):
    - Format: "7.5 x 6.0 x 1.25" (Length x Width x Height)
    - Delimiter: " x " (3 chars)
    - TRY_CAST for optional dimension fields (NULL if malformed)

    Service Column Splitting:
    - LEFT(Service, CHARINDEX(' ', Service) - 1) → integrated_carrier
    - SUBSTRING(Service, CHARINDEX(' ', Service) + 1, LEN) → service_method

    Date Parsing:
    - "2026-03-03 00:40:31 UTC" → LEFT 19 chars → CAST AS DATETIME2

    Units: No conversion needed (weight already in OZ, dimensions already in IN)
    ================================================================================
    */

    INSERT INTO billing.vanlo_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        shipment_id,
        tracking_number,
        shipment_date,
        integrated_carrier,
        service_method,
        zone,
        cost,
        weight_oz,
        package_length_in,
        package_width_in,
        package_height_in,
        from_postal,
        to_postal,
        to_city,
        to_state,
        to_country,
        shipment_status
    )
    SELECT
        cb.carrier_bill_id,
        cb.bill_number AS invoice_number,
        cb.bill_date AS invoice_date,
        TRIM(d.[ID]) AS shipment_id,
        TRIM(d.[Tracking Code]) AS tracking_number,
        CAST(LEFT(d.[Date], 19) AS DATETIME2) AS shipment_date,
        CASE WHEN CHARINDEX(' ', d.[Service]) > 0
             THEN LEFT(d.[Service], CHARINDEX(' ', d.[Service]) - 1)
             ELSE d.[Service] END AS integrated_carrier,
        CASE WHEN CHARINDEX(' ', d.[Service]) > 0
             THEN SUBSTRING(d.[Service], CHARINDEX(' ', d.[Service]) + 1, LEN(d.[Service]))
             ELSE d.[Service] END AS service_method,
        NULLIF(TRIM(d.[Zone]), '') AS zone,
        CAST(d.[Cost] AS DECIMAL(18,2)) AS cost,
        CAST(d.[Weight] AS DECIMAL(18,6)) AS weight_oz,
        dims.dim_length AS package_length_in,
        dims.dim_width AS package_width_in,
        dims.dim_height AS package_height_in,
        NULLIF(TRIM(d.[From Postal Code]), '') AS from_postal,
        NULLIF(TRIM(d.[To Postal Code]), '') AS to_postal,
        NULLIF(TRIM(d.[To City]), '') AS to_city,
        NULLIF(TRIM(d.[To State]), '') AS to_state,
        NULLIF(TRIM(d.[To Country]), '') AS to_country,
        NULLIF(TRIM(d.[Status]), '') AS shipment_status
    FROM billing.delta_vanlo_bill d
    CROSS APPLY (
        SELECT
            pos1 = CHARINDEX(' x ', d.[Package]),
            pos2 = CHARINDEX(' x ', d.[Package], CHARINDEX(' x ', d.[Package]) + 3)
    ) p
    CROSS APPLY (
        SELECT
            NULLIF(TRY_CAST(TRIM(SUBSTRING(d.[Package], 1, p.pos1 - 1)) AS DECIMAL(18,2)), 0) AS dim_length,
            NULLIF(TRY_CAST(TRIM(SUBSTRING(d.[Package], p.pos1 + 3, p.pos2 - p.pos1 - 3)) AS DECIMAL(18,2)), 0) AS dim_width,
            NULLIF(TRY_CAST(TRIM(SUBSTRING(d.[Package], p.pos2 + 3, LEN(d.[Package]))) AS DECIMAL(18,2)), 0) AS dim_height
    ) dims
    INNER JOIN billing.carrier_bill cb
        ON cb.carrier_id = @Carrier_id
        AND cb.file_id = @File_id
    WHERE
        d.[Service] NOT LIKE 'UniUni%'
        AND NULLIF(TRIM(d.[Tracking Code]), '') IS NOT NULL
        AND NULLIF(TRIM(d.[Date]), '') IS NOT NULL
        AND d.[Package] IS NOT NULL
        AND d.[Package] LIKE '% x %'
        AND NOT EXISTS (
            SELECT 1
            FROM billing.vanlo_bill t
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
        '[Vanlo] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
