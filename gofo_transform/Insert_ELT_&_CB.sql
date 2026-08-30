/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
    - @Invoice_Number: NVARCHAR(50) - Real GOFO invoice number, extracted from the
      row-1 metadata line via an ADF Lookup activity + dynamic content expression
      (not present in any data row). See gofo_transform/Attribute_fetch.md.
    - @Bill_Date: NVARCHAR(10) - Invoice period end date ("MM/DD/YYYY"), extracted
      the same way. Converted in this script with CONVERT(date, @Bill_Date, 101).

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of gofo_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_gofo_bill
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_gofo_bill into gofo_bill
            with carrier_bill_id foreign key

Source:   billing.delta_gofo_bill  (bronze -- raw GOFO CSV column names)
Targets:  billing.carrier_bill     (invoice summaries with file_id)
          billing.gofo_bill        (silver -- typed columns)

Invoice Number / Bill Date: Not derived from body columns -- never present in any
                data row, regardless of format version. Both values are passed in
                as @Invoice_Number and @Bill_Date, sourced from the file's row-1
                metadata line by the ADF pipeline. One file = one customer = one
                invoice. See gofo_transform/Attribute_fetch.md.

A-scan Date: Present in the current/latest GOFO file format; used for
                gofo_bill.ascan_date and shipment_attributes.shipment_date
                (see Insert_Unified_tables.sql). Older-format files lack this
                column, but those are not expected going forward.

Dimension Parsing: [Dimensions (inch)] is "L*W*H" (e.g. "8.000*4.500*4.000"),
                    already in inches -- split on '*', no unit conversion.

Missing Shipping Method: ~30 rows per file are credit-only adjustments with
                          blank [Product]/[Zone]/hub columns (still carry a
                          real tracking number + weight/dims). [Product]
                          defaults to 'GOFO Parcel Pickup' when blank/NULL.

Carrier Bill Total: SUM of [Total] from delta (already the sum of all 13
                    charge columns per row, per GOFO's invoice).

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
    One file = one customer = one invoice. bill_number/bill_date come directly
    from @Invoice_Number/@Bill_Date (extracted from row 1 by the ADF pipeline --
    see header comment), not computed from body columns.
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
        CAST(NULLIF(TRIM(@Invoice_Number), '') AS varchar(50)) AS bill_number,
        CONVERT(date, NULLIF(TRIM(@Bill_Date), ''), 101) AS bill_date,
        SUM(CAST(NULLIF(TRIM(d.[Total]), '') AS decimal(18,2))) AS total_amount,
        COUNT(*) AS num_shipments,
        CAST(TRIM(d.[Customer ID]) AS varchar(100)) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_gofo_bill AS d
    WHERE
        NULLIF(TRIM(d.[Customer ID]), '') IS NOT NULL
    GROUP BY
        TRIM(d.[Customer ID])
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
    Each delta row represents one shipment. Inserts into gofo_bill with:
    - carrier_bill_id FK from Step 1
    - [Product] defaulted to 'GOFO Parcel Pickup' when blank
    - Dimensions split on '*' delimiter (CROSS APPLY, DRY)

    NOT EXISTS check uses carrier_bill_id only (Design Constraint #9).
    ================================================================================
    */

    INSERT INTO billing.gofo_bill (
        carrier_bill_id,
        customer_id,
        customer_name,
        tracking_number,
        order_number,
        ascan_date,
        delivery_date,
        product,
        prealerted_hub,
        injection_hub,
        destination_state,
        zip_code,
        [zone],
        actual_weight_lbs,
        invoicing_weight_lbs,
        dim_length_in,
        dim_width_in,
        dim_height_in,
        delivery_fee,
        overweight_fees,
        oversized_fees,
        return_fees,
        remote_area_fees,
        fuel_surcharges,
        delivery_area_fees,
        additional_interception_fees,
        relabelling_fee,
        return_reship_fee,
        credit_for_delivery_fees,
        credit_for_order_value,
        other_fee,
        total_amount,
        remarks
    )
    SELECT
        cb.carrier_bill_id,
        TRIM(d.[Customer ID]),
        NULLIF(TRIM(d.[Customer Name]), ''),
        TRIM(d.[Tracking Number]),
        NULLIF(TRIM(d.[Order Number]), ''),
        CONVERT(date, NULLIF(TRIM(d.[A-scan Date]), ''), 101),
        CONVERT(date, NULLIF(TRIM(d.[Delivery Date]), ''), 101),
        ISNULL(NULLIF(TRIM(d.[Product]), ''), 'GOFO Parcel Pickup'),
        NULLIF(TRIM(d.[Prealerted Hub]), ''),
        NULLIF(TRIM(d.[Injection Hub]), ''),
        NULLIF(TRIM(d.[Destination State]), ''),
        NULLIF(TRIM(d.[Zip Code]), ''),
        NULLIF(TRIM(d.[Zone]), ''),
        CAST(NULLIF(TRIM(d.[Actual weight (lbs)]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Invoicing weight (lbs)]), '') AS decimal(18,2)),
        -- Parse dimensions from "8.000*4.500*4.000" format (CROSS APPLY, DRY)
        dims.dim_length,
        dims.dim_width,
        dims.dim_height,
        CAST(NULLIF(TRIM(d.[Delivery Fee]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Overweight Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Oversized Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Return Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Remote Area Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Fuel Surcharges]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Delivery Area Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Additional Interception Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Relabelling Fee]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Return Reship FEE]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Credit for Delivery Fees]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Credit for Order Value]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Other]), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.[Total]), '') AS decimal(18,2)),
        NULLIF(TRIM(d.[Remarks]), '')
    FROM
        billing.delta_gofo_bill AS d
    CROSS APPLY (
        -- Split dimensions on '*' delimiter: "8.000*4.500*4.000" -> [8.000, 4.500, 4.000]
        SELECT
            pos1 = CHARINDEX('*', d.[Dimensions (inch)]),
            pos2 = CHARINDEX('*', d.[Dimensions (inch)], CHARINDEX('*', d.[Dimensions (inch)]) + 1)
    ) p
    CROSS APPLY (
        -- Extract each dimension (TRY_CAST: optional field, NULL if missing/malformed)
        -- Guard each SUBSTRING with CASE WHEN: CHARINDEX returns 0 when delimiter is absent,
        -- which would produce a negative length and trigger Error 537.
        SELECT
            TRY_CAST(TRIM(CASE WHEN p.pos1 > 0
                THEN SUBSTRING(d.[Dimensions (inch)], 1, p.pos1 - 1)
            END) AS decimal(18,2)) AS dim_length,
            TRY_CAST(TRIM(CASE WHEN p.pos1 > 0 AND p.pos2 > p.pos1
                THEN SUBSTRING(d.[Dimensions (inch)], p.pos1 + 1, p.pos2 - p.pos1 - 1)
            END) AS decimal(18,2)) AS dim_width,
            TRY_CAST(TRIM(CASE WHEN p.pos2 > 0
                THEN SUBSTRING(d.[Dimensions (inch)], p.pos2 + 1, LEN(d.[Dimensions (inch)]))
            END) AS decimal(18,2)) AS dim_height
    ) dims
    INNER JOIN billing.carrier_bill AS cb
        ON cb.account_number = CAST(TRIM(d.[Customer ID]) AS varchar(100))
        AND cb.file_id = @File_id
        AND cb.carrier_id = @Carrier_id
    WHERE
        NULLIF(TRIM(d.[Customer ID]), '') IS NOT NULL
        AND NULLIF(TRIM(d.[Tracking Number]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.gofo_bill AS gb
            WHERE gb.carrier_bill_id = cb.carrier_bill_id
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
        'GOFO Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
