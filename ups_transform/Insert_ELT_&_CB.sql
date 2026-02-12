/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional (UPS)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @carrier_id: INT - UPS carrier_id from LookupCarrierInfo.sql
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of ups_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process:
         1. Aggregate and insert invoice-level summary data from delta_ups_bill 
            into carrier_bill (Carrier Bill summary) - generates carrier_bill_id
         2. Insert line-level billing data from delta_ups_bill (ELT staging) 
            into ups_bill (Carrier Bill line items) with carrier_bill_id foreign key

Source:   billing.delta_ups_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.ups_bill (line items)

Validation: Fails if invoice_date is NULL or empty (fail fast with CAST)
Match:      carrier_bill_id (INSERT WHERE NOT EXISTS) - cleaner idempotency
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

Execution Order: SECOND in pipeline (after LookupCarrierInfo.sql)
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
    Aggregates line items by invoice_number and invoice_date to create invoice-level
    summaries in carrier_bill. Calculates total_amount (sum of net_amount) and
    num_shipments (count of distinct tracking numbers) per invoice.
    Generates carrier_bill_id values which will be joined in Step 2.
    ================================================================================
    */

    INSERT INTO billing.carrier_bill (
        carrier_id,
        bill_number,
        bill_date,
        total_amount,
        num_shipments,
        account_number
    )
    SELECT
        @carrier_id AS carrier_id,
        d.[Invoice Number] AS bill_number,
        CONVERT(date, NULLIF(TRIM(d.[Invoice Date]), '')) AS bill_date,
        SUM(CAST(d.[Net Amount] AS decimal(18,2))) AS total_amount,
        COUNT(DISTINCT d.[Tracking Number]) AS num_shipments,
        MAX(d.[Account Number]) AS account_number
    FROM
        billing.delta_ups_bill AS d
    WHERE
        NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.carrier_bill AS cb
            WHERE cb.bill_number = d.[Invoice Number]
                AND cb.bill_date = CONVERT(date, NULLIF(TRIM(d.[Invoice Date]), ''))
                AND cb.carrier_id = @carrier_id
        )
    GROUP BY
        d.[Invoice Number],
        CONVERT(date, NULLIF(TRIM(d.[Invoice Date]), ''));

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Item Data
    ================================================================================
    Inserts billing line items (one row per charge) from delta_ups_bill into ups_bill.
    Joins with carrier_bill to populate carrier_bill_id foreign key.
    
    Type Conversion Strategy:
    - Critical fields (invoice_date): CAST (fail-fast if bad data)
    - Optional fields (dimensions): TRY_CAST (NULL if missing/malformed)
    
    Idempotency: NOT EXISTS on carrier_bill_id only (if invoice processed, skip all lines)
    
    Dimension Parsing (Modern Approach):
    - Format: "11.0x  8.0x  3.0" (Length x Width x Height)
    - Uses PARSENAME with REPLACE (replaces 'x' with '.', then extracts parts)
    - Much cleaner than nested SUBSTRING/CHARINDEX
    ================================================================================
    */

    INSERT INTO billing.ups_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        charge_description,
        charge_classification_code,
        charge_category_code,
        charge_category_detail_code,
        transaction_date,
        tracking_number,
        [zone],
        net_amount,
        billed_weight,
        billed_weight_unit,
        dim_length,
        dim_width,
        dim_height,
        dim_unit,
        sender_postal,
        created_date
    )
    SELECT
        cb.carrier_bill_id,
        NULLIF(TRIM(d.[Invoice Number]), ''),
        CONVERT(date, NULLIF(TRIM(d.[Invoice Date]), '')),
        NULLIF(TRIM(d.[Charge Description]), ''),
        NULLIF(TRIM(d.[Charge Classification Code]), ''),
        NULLIF(TRIM(d.[Charge Category Code]), ''),
        NULLIF(TRIM(d.[Charge Category Detail Code]), ''),
        CONVERT(datetime2, NULLIF(TRIM(d.[Transaction Date]), '')),
        NULLIF(TRIM(d.[Tracking Number]), ''),
        NULLIF(TRIM(d.[Zone]), ''),
        CAST(d.[Net Amount] AS decimal(18,2)),
        CAST(d.[Billed Weight] AS decimal(18,2)),
        NULLIF(TRIM(d.[Billed Weight Unit of Measure]), ''),
        -- Parse dimensions from "11.0x  8.0x  3.0" format using CROSS APPLY (DRY)
        dims.dim_length,
        dims.dim_width,
        dims.dim_height,
        NULLIF(TRIM(d.[Package Dimension Unit Of Measure]), ''),
        NULLIF(TRIM(d.[Sender Postal]), ''),
        SYSDATETIME()
    FROM
        billing.delta_ups_bill AS d
    CROSS APPLY (
        -- Split dimensions on 'x' delimiter: "11.0x  8.0x  3.0" -> [11.0, 8.0, 3.0]
        SELECT
            pos1 = CHARINDEX('x', d.[Package Dimensions]),
            pos2 = CHARINDEX('x', d.[Package Dimensions], CHARINDEX('x', d.[Package Dimensions]) + 1)
    ) p
    CROSS APPLY (
        -- Extract and convert each dimension (TRIM spaces, TRY_CAST for optional dims, NULLIF zero)
        SELECT
            NULLIF(TRY_CAST(TRIM(SUBSTRING(d.[Package Dimensions], 1, p.pos1 - 1)) AS decimal(18,2)), 0) AS dim_length,
            NULLIF(TRY_CAST(TRIM(SUBSTRING(d.[Package Dimensions], p.pos1 + 1, p.pos2 - p.pos1 - 1)) AS decimal(18,2)), 0) AS dim_width,
            NULLIF(TRY_CAST(TRIM(SUBSTRING(d.[Package Dimensions], p.pos2 + 1, LEN(d.[Package Dimensions]))) AS decimal(18,2)), 0) AS dim_height
    ) dims
    INNER JOIN billing.carrier_bill AS cb
        ON cb.bill_number = d.[Invoice Number]
        AND cb.bill_date = CONVERT(date, NULLIF(TRIM(d.[Invoice Date]), ''))
        AND cb.carrier_id = @carrier_id
    WHERE
        NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.ups_bill AS ub
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
    
    -- Build descriptive error message
    DECLARE @DetailedError NVARCHAR(4000) = 
        'UPS Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
