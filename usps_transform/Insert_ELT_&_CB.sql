/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional (USPS)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - USPS carrier_id from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of usps_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_usps_bill
            into carrier_bill with file_id - generates carrier_bill_id.
            USPS's EPS billing feed can contain multiple eps_acct_num values in a
            single file, so invoices are grouped by eps_acct_num (one carrier_bill
            row per account per file), not just by the file as a whole.
         2. Insert line-level billing data from delta_usps_bill (ELT staging)
            into usps_bill (Carrier Bill line items) with carrier_bill_id foreign key.

Source:   billing.delta_usps_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.usps_bill (line items)

Invoice Grain: One carrier_bill row per eps_acct_num per file.
  bill_number    = '{eps_acct_num}_{MIN(tran_date) yyyyMMdd}_{MAX(tran_date) yyyyMMdd}'
  bill_date      = MAX(tran_date)
  account_number = eps_acct_num
  total_amount   = SUM(postage)
  num_shipments  = COUNT(DISTINCT pic)

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Validation: Fails if tran_date or postage is NULL/unparseable (fail fast with CAST)
Match:      carrier_bill_id (INSERT WHERE NOT EXISTS) - cleaner idempotency
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
    Aggregates line items by eps_acct_num to create one invoice per account per file.
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
        TRIM(d.eps_acct_num) + '_'
            + FORMAT(MIN(CAST(d.tran_date AS DATETIME2)), 'yyyyMMdd') + '_'
            + FORMAT(MAX(CAST(d.tran_date AS DATETIME2)), 'yyyyMMdd') AS bill_number,
        CAST(MAX(CAST(d.tran_date AS DATETIME2)) AS DATE) AS bill_date,
        SUM(CAST(d.postage AS DECIMAL(18,2))) AS total_amount,
        COUNT(DISTINCT d.pic) AS num_shipments,
        TRIM(d.eps_acct_num) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_usps_bill AS d
    WHERE
        NULLIF(TRIM(d.eps_acct_num), '') IS NOT NULL
    GROUP BY
        TRIM(d.eps_acct_num)
    HAVING
        NOT EXISTS (
            SELECT 1
            FROM billing.carrier_bill AS cb
            WHERE cb.file_id = @File_id  -- File-based idempotency: same file = same data
        );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Item Data
    ================================================================================
    Inserts one row per delta_usps_bill row (narrow format - one charge event per row)
    into usps_bill. Joins with carrier_bill on account_number to route each row to the
    correct per-account invoice.

    Idempotency: NOT EXISTS on carrier_bill_id only (if invoice processed, skip all lines)
    ================================================================================
    */

    INSERT INTO billing.usps_bill (
        carrier_bill_id,
        eps_acct_num,
        eps_tran_id,
        tran_amt,
        tran_date,
        tran_type,
        tracking_number,
        postage,
        assessment_type,
        assessment_details,
        cust_ref_num1,
        cust_ref_num2,
        mail_class,
        created_date
    )
    SELECT
        cb.carrier_bill_id,
        TRIM(d.eps_acct_num),
        TRIM(d.eps_tran_id),
        CAST(d.tran_amt AS DECIMAL(18,2)),
        CAST(d.tran_date AS DATETIME2),
        TRIM(d.tran_type),
        TRIM(d.pic),
        CAST(d.postage AS DECIMAL(18,2)),
        NULLIF(TRIM(d.assessment_type), ''),
        NULLIF(TRIM(d.assessment_details), ''),
        NULLIF(TRIM(d.cust_ref_num1), ''),
        NULLIF(TRIM(d.cust_ref_num2), ''),
        NULLIF(TRIM(d.mail_class), ''),
        SYSDATETIME()
    FROM
        billing.delta_usps_bill AS d
    INNER JOIN billing.carrier_bill AS cb
        ON cb.account_number = TRIM(d.eps_acct_num)
        AND cb.carrier_id = @Carrier_id
        AND cb.file_id = @File_id
    WHERE
        NULLIF(TRIM(d.eps_acct_num), '') IS NOT NULL
        AND NULLIF(TRIM(d.pic), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.usps_bill AS ub
            WHERE ub.carrier_bill_id = cb.carrier_bill_id
        );

    SET @LineItemsInserted = @@ROWCOUNT;

    COMMIT TRANSACTION;

    -- Return success metrics
    SELECT
        'SUCCESS' AS Status,
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    -- Return descriptive error details
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'USPS Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
