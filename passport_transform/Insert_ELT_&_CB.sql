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
    - LineItemsInserted: INT - Number of passport_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_passport_bill
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_passport_bill (ELT staging)
            into billing.passport_bill (Carrier Bill line items) with carrier_bill_id FK

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

account_number: [SHIPPER COMPANY] column (e.g., "Falcon Direct") — the billing party with the Passport account.
               Nullable (consistent with FedEx). [COMPANY] is the end brand/client; [CLIENT CODE] is unpopulated.

Date Parsing:
         - Invoice Date: [INVOICE DATE] column, ISO format "yyyy-mm-dd"
           Parsed via CAST(... AS DATE) — fail fast on bad data
         - Ship Date: [SHIP DATE] column, same ISO format

Source:   billing.delta_passport_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.passport_bill (line items)

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
    Aggregates line items by [INVOICE NUMBER] and [INVOICE DATE] to create
    invoice-level summaries in carrier_bill.

    Calculates:
    - bill_number: [INVOICE NUMBER] (e.g., "73966")
    - bill_date:   CAST([INVOICE DATE] AS DATE) — ISO yyyy-mm-dd format
    - total_amount: SUM of [TOTAL] per invoice
    - num_shipments: COUNT of shipments per invoice
    - account_number: NULLIF(TRIM([SHIPPER COMPANY]), '') — billing party ("Falcon Direct")
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
        d.[INVOICE NUMBER]                              AS bill_number,
        CAST(d.[INVOICE DATE] AS DATE)                  AS bill_date,
        SUM(CAST(d.[TOTAL] AS DECIMAL(18,2)))           AS total_amount,
        COUNT(*)                                        AS num_shipments,
        MAX(NULLIF(TRIM(d.[SHIPPER COMPANY]), ''))       AS account_number,
        @File_id                                        AS file_id
    FROM billing.delta_passport_bill AS d
    WHERE
        d.[INVOICE NUMBER] IS NOT NULL
        AND NULLIF(TRIM(d.[INVOICE NUMBER]), '') IS NOT NULL
        AND d.[TRACKING ID] IS NOT NULL
        AND NULLIF(TRIM(d.[TRACKING ID]), '') IS NOT NULL
    GROUP BY d.[INVOICE NUMBER], d.[INVOICE DATE]
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id  -- FILE-BASED IDEMPOTENCY
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment records from delta_passport_bill into
    billing.passport_bill.

    Join Strategy:
    - Join on bill_number = [INVOICE NUMBER] AND bill_date AND carrier_id to get carrier_bill_id
    - Use carrier_bill_id only in NOT EXISTS check (Design Constraint #9)

    Type Conversions (fail-fast, no TRY_CAST):
    - Dates:     CAST AS DATE (ISO yyyy-mm-dd format — no CONVERT style needed)
    - Weights:   CAST AS DECIMAL(18,6) — already in OZ, no unit conversion needed
    - Dims:      CAST AS DECIMAL(18,2) — already in IN, no unit conversion needed
    - Monetary:  CAST AS DECIMAL(18,2)
    ================================================================================
    */

    INSERT INTO billing.passport_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        ship_date,
        company,
        client_code,
        order_id,
        service_level,
        shipper_name,
        shipper_company,
        shipper_street,
        shipper_city,
        shipper_state,
        shipper_zip,
        shipper_country,
        dest_street,
        dest_city,
        dest_state,
        dest_zip,
        dest_country,
        tracking_number,
        actual_weight_oz,
        dimensional_weight_oz,
        billable_weight_oz,
        length_in,
        width_in,
        height_in,
        currency,
        rate,
        fuel_surcharge,
        tax,
        duty,
        insurance,
        clearance_fee,
        fee_1_description,
        fee_1_amount,
        fee_2_description,
        fee_2_amount,
        fee_3_description,
        fee_3_amount,
        fee_4_description,
        fee_4_amount,
        fee_5_description,
        fee_5_amount,
        fee_6_description,
        fee_6_amount,
        fee_7_description,
        fee_7_amount,
        fee_8_description,
        fee_8_amount,
        total
    )
    SELECT
        cb.carrier_bill_id,

        d.[INVOICE NUMBER]                                          AS invoice_number,
        CAST(d.[INVOICE DATE] AS DATE)                              AS invoice_date,
        CAST(d.[SHIP DATE] AS DATE)                                 AS ship_date,

        d.[COMPANY]                                                 AS company,
        d.[CLIENT CODE]                                             AS client_code,
        d.[ORDER ID]                                                AS order_id,
        d.[SERVICE LEVEL]                                           AS service_level,

        d.[SHIPPER NAME]                                            AS shipper_name,
        d.[SHIPPER COMPANY]                                         AS shipper_company,
        d.[SHIPPER STREET]                                          AS shipper_street,
        d.[SHIPPER CITY]                                            AS shipper_city,
        d.[SHIPPER STATE]                                           AS shipper_state,
        d.[SHIPPER ZIP]                                             AS shipper_zip,
        d.[SHIPPER COUNTRY]                                         AS shipper_country,

        d.[DEST STREET]                                             AS dest_street,
        d.[DEST CITY]                                               AS dest_city,
        d.[DEST STATE]                                              AS dest_state,
        d.[DEST ZIP]                                                AS dest_zip,
        d.[DEST COUNTRY]                                            AS dest_country,

        TRIM(d.[TRACKING ID])                                       AS tracking_number,

        -- Weights: already in OZ per column header — no conversion
        CAST(d.[ACTUAL WEIGHT (OZ)]      AS DECIMAL(18,6))         AS actual_weight_oz,
        CAST(d.[DIMENSIONAL WEIGHT (OZ)] AS DECIMAL(18,6))         AS dimensional_weight_oz,
        CAST(d.[BILLABLE WEIGHT (OZ)]    AS DECIMAL(18,6))         AS billable_weight_oz,

        -- Dimensions: already in IN per column header — no conversion
        CAST(d.[LENGTH (IN)] AS DECIMAL(18,2))                     AS length_in,
        CAST(d.[WIDTH (IN)]  AS DECIMAL(18,2))                     AS width_in,
        CAST(d.[HEIGHT (IN)] AS DECIMAL(18,2))                     AS height_in,

        d.[CURRENCY]                                                AS currency,

        -- Charges
        CAST(d.[RATE]          AS DECIMAL(18,2))                   AS rate,
        CAST(d.[FUEL SURCHARGE] AS DECIMAL(18,2))                  AS fuel_surcharge,
        CAST(d.[TAX]           AS DECIMAL(18,2))                   AS tax,
        CAST(d.[DUTY]          AS DECIMAL(18,2))                   AS duty,
        CAST(d.[INSURANCE]     AS DECIMAL(18,2))                   AS insurance,
        CAST(d.[CLEARANCE FEE] AS DECIMAL(18,2))                   AS clearance_fee,

        -- Variable fees (description + amount pairs)
        d.[FEE 1 DESCRIPTION]                                      AS fee_1_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 1 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_1_amount,
        d.[FEE 2 DESCRIPTION]                                      AS fee_2_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 2 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_2_amount,
        d.[FEE 3 DESCRIPTION]                                      AS fee_3_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 3 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_3_amount,
        d.[FEE 4 DESCRIPTION]                                      AS fee_4_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 4 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_4_amount,
        d.[FEE 5 DESCRIPTION]                                      AS fee_5_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 5 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_5_amount,
        d.[FEE 6 DESCRIPTION]                                      AS fee_6_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 6 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_6_amount,
        d.[FEE 7 DESCRIPTION]                                      AS fee_7_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 7 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_7_amount,
        d.[FEE 8 DESCRIPTION]                                      AS fee_8_description,
        CAST(ISNULL(NULLIF(TRIM(d.[FEE 8 AMOUNT]), ''), '0') AS DECIMAL(18,2)) AS fee_8_amount,

        CAST(d.[TOTAL] AS DECIMAL(18,2))                           AS total

    FROM billing.delta_passport_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number  = d.[INVOICE NUMBER]
        AND cb.bill_date   = CAST(d.[INVOICE DATE] AS DATE)
        AND cb.carrier_id  = @Carrier_id
    WHERE
        d.[INVOICE NUMBER] IS NOT NULL
        AND NULLIF(TRIM(d.[INVOICE NUMBER]), '') IS NOT NULL
        AND d.[TRACKING ID] IS NOT NULL
        AND NULLIF(TRIM(d.[TRACKING ID]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.passport_bill t
            WHERE t.carrier_bill_id = cb.carrier_bill_id  -- carrier_bill_id only (Constraint #9)
        );

    SET @LineItemsInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Commit and Return Results
    ================================================================================
    */
    COMMIT TRANSACTION;

    SELECT
        'SUCCESS'           AS Status,
        @InvoicesInserted   AS InvoicesInserted,
        @LineItemsInserted  AS LineItemsInserted;

END TRY
BEGIN CATCH
    /*
    ================================================================================
    Error Handling: Rollback and Return Detailed Error Information
    ================================================================================
    */
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Passport] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - Transaction wraps both INSERTs for atomicity
✅ #3  - Direct CAST (fail fast), no TRY_CAST
✅ #4  - Idempotency: Step 1 via file_id check; Step 2 via carrier_bill_id only
✅ #7  - No unit conversions needed: weights already in OZ, dims already in IN
✅ #8  - Returns Status, InvoicesInserted, LineItemsInserted
✅ #9  - Line items NOT EXISTS check uses carrier_bill_id only
✅ #12 - File-based processing: file_id stored in carrier_bill, checked in HAVING
================================================================================
*/
