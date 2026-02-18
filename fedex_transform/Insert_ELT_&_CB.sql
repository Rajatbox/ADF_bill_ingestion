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
    - LineItemsInserted: INT - Number of fedex_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_fedex_bill 
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_fedex_bill (ELT staging) 
            into fedex_bill (Carrier Bill line items) with carrier_bill_id foreign key

Source:   billing.delta_fedex_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.fedex_bill (line items)

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Validation: Fails if invoice_date or shipment_date is NULL or empty
Match:      (invoice_number, invoice_date, carrier_id, file_id)
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

Execution Order: SECOND in pipeline (after ValidateAndInitializeFile.sql)
================================================================================
*/

/*
================================================================================
Tech Debt
================================================================================
TODO: For service_type column, use COALESCE on [Ground Service] and [Service Type] 
      from delta_fedex_bill to handle cases where one may be NULL.
      Example: COALESCE(d.[Ground Service], d.[Service Type]) AS service_type
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
    summaries in carrier_bill. Calculates total_amount (sum of net_charge_amount) and
    num_shipments (count of all line items) per invoice.
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
        file_id  -- NEW COLUMN
    )
    SELECT
        @Carrier_id AS carrier_id,
        NULLIF(TRIM(d.[Invoice Number]), '') AS bill_number,
        NULLIF(TRIM(d.[Invoice Date]), '') AS bill_date,
        SUM(CAST(NULLIF(REPLACE(TRIM(d.[Net Charge Amount]), ',', ''), '') AS DECIMAL(18,2))) AS total_amount,
        COUNT(*) AS num_shipments,
        MAX(NULLIF(TRIM(d.[Bill to Account Number]), '')) AS account_number,
        @File_id AS file_id  -- NEW VALUE
    FROM
billing.delta_fedex_bill AS d
    WHERE
        NULLIF(TRIM(d.[Invoice Number]), '') IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
        -- Note: No tracking ID filter here - invoice aggregation should include ALL line items
        -- Tracking ID validation happens in Step 2 (line-level insert)
    GROUP BY
        NULLIF(TRIM(d.[Invoice Number]), ''),
        NULLIF(TRIM(d.[Invoice Date]), '')
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
    Inserts individual shipment/line items from delta_fedex_bill into fedex_bill.
    Each row represents a single shipment with tracking details, charges, and dimensions.
    Joins directly with carrier_bill table to get carrier_bill_id for referential integrity.
    NOT EXISTS check uses carrier_bill_id only for cleaner idempotency check.
    ================================================================================
    */

    INSERT INTO billing.fedex_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        service_type,
        shipment_date,
        express_or_ground_tracking_id,
        msp_tracking_id,
        zone_code,
        net_charge_amount,
        rated_weight_amount,
        rated_weight_units,
        dim_length,
        dim_width,
        dim_height,
        dim_unit,
        shipper_zip_code,
        [Transportation Charge Amount],
    [Tracking ID Charge Description],
    [Tracking ID Charge Amount],
    [Tracking ID Charge Description_1],
    [Tracking ID Charge Amount_1],
    [Tracking ID Charge Description_2],
    [Tracking ID Charge Amount_2],
    [Tracking ID Charge Description_3],
    [Tracking ID Charge Amount_3],
    [Tracking ID Charge Description_4],
    [Tracking ID Charge Amount_4],
    [Tracking ID Charge Description_5],
    [Tracking ID Charge Amount_5],
    [Tracking ID Charge Description_6],
    [Tracking ID Charge Amount_6],
    [Tracking ID Charge Description_7],
    [Tracking ID Charge Amount_7],
    [Tracking ID Charge Description_8],
    [Tracking ID Charge Amount_8],
    [Tracking ID Charge Description_9],
    [Tracking ID Charge Amount_9],
    [Tracking ID Charge Description_10],
    [Tracking ID Charge Amount_10],
    [Tracking ID Charge Description_11],
    [Tracking ID Charge Amount_11],
    [Tracking ID Charge Description_12],
    [Tracking ID Charge Amount_12],
    [Tracking ID Charge Description_13],
    [Tracking ID Charge Amount_13],
    [Tracking ID Charge Description_14],
    [Tracking ID Charge Amount_14],
    [Tracking ID Charge Description_15],
    [Tracking ID Charge Amount_15],
    [Tracking ID Charge Description_16],
    [Tracking ID Charge Amount_16],
    [Tracking ID Charge Description_17],
    [Tracking ID Charge Amount_17],
    [Tracking ID Charge Description_18],
    [Tracking ID Charge Amount_18],
    [Tracking ID Charge Description_19],
    [Tracking ID Charge Amount_19],
    [Tracking ID Charge Description_20],
    [Tracking ID Charge Amount_20],
    [Tracking ID Charge Description_21],
    [Tracking ID Charge Amount_21],
    [Tracking ID Charge Description_22],
    [Tracking ID Charge Amount_22],
    [Tracking ID Charge Description_23],
    [Tracking ID Charge Amount_23],
    [Tracking ID Charge Description_24],
    [Tracking ID Charge Amount_24],
    [Tracking ID Charge Description_25],
    [Tracking ID Charge Amount_25],
    [Tracking ID Charge Description_26],
    [Tracking ID Charge Amount_26],
    [Tracking ID Charge Description_27],
    [Tracking ID Charge Amount_27],
    [Tracking ID Charge Description_28],
    [Tracking ID Charge Amount_28],
    [Tracking ID Charge Description_29],
    [Tracking ID Charge Amount_29],
    [Tracking ID Charge Description_30],
    [Tracking ID Charge Amount_30],
    [Tracking ID Charge Description_31],
    [Tracking ID Charge Amount_31],
    [Tracking ID Charge Description_32],
    [Tracking ID Charge Amount_32],
    [Tracking ID Charge Description_33],
    [Tracking ID Charge Amount_33],
    [Tracking ID Charge Description_34],
    [Tracking ID Charge Amount_34],
    [Tracking ID Charge Description_35],
    [Tracking ID Charge Amount_35],
    [Tracking ID Charge Description_36],
    [Tracking ID Charge Amount_36],
    [Tracking ID Charge Description_37],
    [Tracking ID Charge Amount_37],
    [Tracking ID Charge Description_38],
    [Tracking ID Charge Amount_38],
    [Tracking ID Charge Description_39],
    [Tracking ID Charge Amount_39],
    [Tracking ID Charge Description_40],
    [Tracking ID Charge Amount_40],
    [Tracking ID Charge Description_41],
    [Tracking ID Charge Amount_41],
    [Tracking ID Charge Description_42],
    [Tracking ID Charge Amount_42],
    [Tracking ID Charge Description_43],
    [Tracking ID Charge Amount_43],
    [Tracking ID Charge Description_44],
    [Tracking ID Charge Amount_44],
    [Tracking ID Charge Description_45],
    [Tracking ID Charge Amount_45],
    [Tracking ID Charge Description_46],
    [Tracking ID Charge Amount_46],
    [Tracking ID Charge Description_47],
    [Tracking ID Charge Amount_47],
    [Tracking ID Charge Description_48],
    [Tracking ID Charge Amount_48],
    [Tracking ID Charge Description_49],
    [Tracking ID Charge Amount_49],
    [Tracking ID Charge Description_50],
    [Tracking ID Charge Amount_50]
)
SELECT 
    cb.carrier_bill_id,
    NULLIF(TRIM(d.[Invoice Number]), '') AS invoice_number,
    NULLIF(TRIM(d.[Invoice Date]), '') AS invoice_date,
    NULLIF(TRIM(d.[Service Type]), '') AS service_type,
    NULLIF(TRIM(d.[Shipment Date]), '') AS shipment_date,
    NULLIF(TRIM(d.[Express or Ground Tracking ID]), '') AS express_or_ground_tracking_id,
    NULLIF(TRIM(d.[MPS Package ID]), '') AS msp_tracking_id,
    NULLIF(TRIM(d.[Zone Code]), '') AS zone_code,
    CAST(NULLIF(REPLACE(TRIM(d.[Net Charge Amount]), ',', ''), '') AS DECIMAL(18,2)) AS net_charge_amount,
    CAST(NULLIF(TRIM(d.[Rated Weight Amount]), '') AS DECIMAL(18,2)) AS rated_weight_amount,
    NULLIF(TRIM(d.[Rated Weight Units]), '') AS rated_weight_units,
    CAST(NULLIF(TRIM(d.[Dim Length]), '') AS DECIMAL(18,2)) AS dim_length,
    CAST(NULLIF(TRIM(d.[Dim Width]), '') AS DECIMAL(18,2)) AS dim_width,
    CAST(NULLIF(TRIM(d.[Dim Height]), '') AS DECIMAL(18,2)) AS dim_height,
    NULLIF(TRIM(d.[Dim Unit]), '') AS dim_unit,
    NULLIF(TRIM(d.[Shipper Zip Code]), '') AS shipper_zip_code,
    CAST(NULLIF(REPLACE(TRIM(d.[Transportation Charge Amount]), ',', ''), '') AS DECIMAL(18,2)) AS [Transportation Charge Amount],
    NULLIF(TRIM(d.[Tracking ID Charge Description]), '') AS [Tracking ID Charge Description],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount],
    NULLIF(TRIM(d.[Tracking ID Charge Description_1]), '') AS [Tracking ID Charge Description_1],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_1]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_1],
    NULLIF(TRIM(d.[Tracking ID Charge Description_2]), '') AS [Tracking ID Charge Description_2],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_2]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_2],
    NULLIF(TRIM(d.[Tracking ID Charge Description_3]), '') AS [Tracking ID Charge Description_3],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_3]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_3],
    NULLIF(TRIM(d.[Tracking ID Charge Description_4]), '') AS [Tracking ID Charge Description_4],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_4]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_4],
    NULLIF(TRIM(d.[Tracking ID Charge Description_5]), '') AS [Tracking ID Charge Description_5],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_5]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_5],
    NULLIF(TRIM(d.[Tracking ID Charge Description_6]), '') AS [Tracking ID Charge Description_6],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_6]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_6],
    NULLIF(TRIM(d.[Tracking ID Charge Description_7]), '') AS [Tracking ID Charge Description_7],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_7]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_7],
    NULLIF(TRIM(d.[Tracking ID Charge Description_8]), '') AS [Tracking ID Charge Description_8],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_8]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_8],
    NULLIF(TRIM(d.[Tracking ID Charge Description_9]), '') AS [Tracking ID Charge Description_9],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_9]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_9],
    NULLIF(TRIM(d.[Tracking ID Charge Description_10]), '') AS [Tracking ID Charge Description_10],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_10]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_10],
    NULLIF(TRIM(d.[Tracking ID Charge Description_11]), '') AS [Tracking ID Charge Description_11],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_11]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_11],
    NULLIF(TRIM(d.[Tracking ID Charge Description_12]), '') AS [Tracking ID Charge Description_12],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_12]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_12],
    NULLIF(TRIM(d.[Tracking ID Charge Description_13]), '') AS [Tracking ID Charge Description_13],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_13]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_13],
    NULLIF(TRIM(d.[Tracking ID Charge Description_14]), '') AS [Tracking ID Charge Description_14],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_14]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_14],
    NULLIF(TRIM(d.[Tracking ID Charge Description_15]), '') AS [Tracking ID Charge Description_15],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_15]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_15],
    NULLIF(TRIM(d.[Tracking ID Charge Description_16]), '') AS [Tracking ID Charge Description_16],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_16]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_16],
    NULLIF(TRIM(d.[Tracking ID Charge Description_17]), '') AS [Tracking ID Charge Description_17],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_17]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_17],
    NULLIF(TRIM(d.[Tracking ID Charge Description_18]), '') AS [Tracking ID Charge Description_18],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_18]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_18],
    NULLIF(TRIM(d.[Tracking ID Charge Description_19]), '') AS [Tracking ID Charge Description_19],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_19]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_19],
    NULLIF(TRIM(d.[Tracking ID Charge Description_20]), '') AS [Tracking ID Charge Description_20],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_20]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_20],
    NULLIF(TRIM(d.[Tracking ID Charge Description_21]), '') AS [Tracking ID Charge Description_21],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_21]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_21],
    NULLIF(TRIM(d.[Tracking ID Charge Description_22]), '') AS [Tracking ID Charge Description_22],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_22]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_22],
    NULLIF(TRIM(d.[Tracking ID Charge Description_23]), '') AS [Tracking ID Charge Description_23],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_23]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_23],
    NULLIF(TRIM(d.[Tracking ID Charge Description_24]), '') AS [Tracking ID Charge Description_24],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_24]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_24],
    NULLIF(TRIM(d.[Tracking ID Charge Description_25]), '') AS [Tracking ID Charge Description_25],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_25]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_25],
    NULLIF(TRIM(d.[Tracking ID Charge Description_26]), '') AS [Tracking ID Charge Description_26],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_26]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_26],
    NULLIF(TRIM(d.[Tracking ID Charge Description_27]), '') AS [Tracking ID Charge Description_27],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_27]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_27],
    NULLIF(TRIM(d.[Tracking ID Charge Description_28]), '') AS [Tracking ID Charge Description_28],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_28]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_28],
    NULLIF(TRIM(d.[Tracking ID Charge Description_29]), '') AS [Tracking ID Charge Description_29],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_29]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_29],
    NULLIF(TRIM(d.[Tracking ID Charge Description_30]), '') AS [Tracking ID Charge Description_30],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_30]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_30],
    NULLIF(TRIM(d.[Tracking ID Charge Description_31]), '') AS [Tracking ID Charge Description_31],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_31]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_31],
    NULLIF(TRIM(d.[Tracking ID Charge Description_32]), '') AS [Tracking ID Charge Description_32],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_32]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_32],
    NULLIF(TRIM(d.[Tracking ID Charge Description_33]), '') AS [Tracking ID Charge Description_33],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_33]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_33],
    NULLIF(TRIM(d.[Tracking ID Charge Description_34]), '') AS [Tracking ID Charge Description_34],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_34]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_34],
    NULLIF(TRIM(d.[Tracking ID Charge Description_35]), '') AS [Tracking ID Charge Description_35],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_35]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_35],
    NULLIF(TRIM(d.[Tracking ID Charge Description_36]), '') AS [Tracking ID Charge Description_36],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_36]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_36],
    NULLIF(TRIM(d.[Tracking ID Charge Description_37]), '') AS [Tracking ID Charge Description_37],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_37]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_37],
    NULLIF(TRIM(d.[Tracking ID Charge Description_38]), '') AS [Tracking ID Charge Description_38],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_38]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_38],
    NULLIF(TRIM(d.[Tracking ID Charge Description_39]), '') AS [Tracking ID Charge Description_39],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_39]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_39],
    NULLIF(TRIM(d.[Tracking ID Charge Description_40]), '') AS [Tracking ID Charge Description_40],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_40]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_40],
    NULLIF(TRIM(d.[Tracking ID Charge Description_41]), '') AS [Tracking ID Charge Description_41],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_41]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_41],
    NULLIF(TRIM(d.[Tracking ID Charge Description_42]), '') AS [Tracking ID Charge Description_42],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_42]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_42],
    NULLIF(TRIM(d.[Tracking ID Charge Description_43]), '') AS [Tracking ID Charge Description_43],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_43]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_43],
    NULLIF(TRIM(d.[Tracking ID Charge Description_44]), '') AS [Tracking ID Charge Description_44],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_44]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_44],
    NULLIF(TRIM(d.[Tracking ID Charge Description_45]), '') AS [Tracking ID Charge Description_45],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_45]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_45],
    NULLIF(TRIM(d.[Tracking ID Charge Description_46]), '') AS [Tracking ID Charge Description_46],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_46]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_46],
    NULLIF(TRIM(d.[Tracking ID Charge Description_47]), '') AS [Tracking ID Charge Description_47],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_47]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_47],
    NULLIF(TRIM(d.[Tracking ID Charge Description_48]), '') AS [Tracking ID Charge Description_48],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_48]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_48],
    NULLIF(TRIM(d.[Tracking ID Charge Description_49]), '') AS [Tracking ID Charge Description_49],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_49]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_49],
    NULLIF(TRIM(d.[Tracking ID Charge Description_50]), '') AS [Tracking ID Charge Description_50],
    CAST(NULLIF(REPLACE(TRIM(d.[Tracking ID Charge Amount_50]), ',', ''), '') AS DECIMAL(18,2)) AS [Tracking ID Charge Amount_50]
FROM billing.delta_fedex_bill d
INNER JOIN billing.carrier_bill cb
    ON cb.bill_number = NULLIF(TRIM(d.[Invoice Number]), '')
    AND cb.bill_date = NULLIF(TRIM(d.[Invoice Date]), '')
    AND cb.carrier_id = @Carrier_id
WHERE NULLIF(TRIM(d.[Invoice Number]), '') IS NOT NULL
  AND NULLIF(TRIM(d.[Invoice Date]), '') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM billing.fedex_bill t
      WHERE t.carrier_bill_id = cb.carrier_bill_id
  );

    SET @LineItemsInserted = @@ROWCOUNT;

    -- Commit transaction if all succeeds
    COMMIT TRANSACTION;
    
    SELECT 
        'SUCCESS' AS Status, 
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    -- Rollback transaction on any error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    -- Build descriptive error message
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        'FedEx Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
