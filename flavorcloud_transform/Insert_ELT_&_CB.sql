/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier ID from LookupCarrierInfo.sql
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of flavorcloud_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process:
         1. Aggregate and insert invoice-level summary data from delta_flavorcloud_bill 
            into carrier_bill (Carrier Bill summary) - generates carrier_bill_id
         2. Insert line-level billing data from delta_flavorcloud_bill (ELT staging) 
            into billing.flavorcloud_bill (Carrier Bill line items)

         Filtering: The CSV contains a "Total" row and summary distribution rows
         after the line items. These are filtered out by requiring:
         - [Invoice Number] is not 'Total' and not empty
         - [Shipment Number] is not empty (summary rows have no shipment number)

Date Parsing:
         - Invoice Date: [Date] column, format "Mon dd, yyyy" (e.g., "Jan 25, 2026")
           Parsed via CONVERT(DATE, ..., 107)
         - Shipment Date: [Shipment Date] column, format "mm-dd-yyyy" (e.g., "01-22-2026")
           Parsed via CONVERT(DATE, ..., 110)
         - Due Date: [Due Date] column, same mm-dd-yyyy format

         account_number = [Origin Location] (e.g., "Falcon Fulfillment UT")

Source:   billing.delta_flavorcloud_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.flavorcloud_bill (line items)

Validation: Fails if date columns or shipment number contain unparseable data
Match:      bill_number AND bill_date AND carrier_id (INSERT WHERE NOT EXISTS)
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

Execution Order: SECOND in pipeline (after LookupCarrierInfo.sql)
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
    Aggregates line items by Invoice Number and Date to create invoice-level 
    summaries in carrier_bill.
    
    Calculates:
    - bill_number: [Invoice Number] directly from CSV (e.g., "FLCL-1jfpthha9r78")
    - bill_date: CONVERT(DATE, [Date], 107) for "Mon dd, yyyy" format
    - total_amount: SUM of [Shipment Total Charges (USD)]
    - num_shipments: COUNT of shipments per invoice
    - account_number: [Origin Location] (placeholder - no explicit account in CSV)
    
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
        @Carrier_id AS carrier_id,
        d.[Invoice Number] AS bill_number,
        CONVERT(DATE, d.[Date], 107) AS bill_date,
        SUM(CAST(d.[Shipment Total Charges (USD)] AS DECIMAL(18,2))) AS total_amount,
        COUNT(*) AS num_shipments,
        MAX(d.[Origin Location]) AS account_number
    FROM
        billing.delta_flavorcloud_bill AS d
    WHERE
        d.[Invoice Number] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Number]), '') IS NOT NULL
        AND d.[Invoice Number] != 'Total'
        AND d.[Shipment Number] IS NOT NULL
        AND NULLIF(TRIM(d.[Shipment Number]), '') IS NOT NULL
    GROUP BY d.[Invoice Number], d.[Date]
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.bill_number = d.[Invoice Number]
          AND cb.bill_date = CONVERT(DATE, d.[Date], 107)
          AND cb.carrier_id = @Carrier_id
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment records from delta_flavorcloud_bill into 
    billing.flavorcloud_bill.
    
    Join Strategy:
    - Join on bill_number = [Invoice Number] and bill_date to get carrier_bill_id
    - Use carrier_bill_id only in NOT EXISTS check (Design Constraint #9)
    
    Type Conversions:
    - Direct CAST / CONVERT (fail-fast on bad data, no TRY_CAST / TRY_CONVERT)
    - Invoice Date: CONVERT(DATE, [Date], 107) for "Mon dd, yyyy"
    - Shipment Date: CONVERT(DATE, [Shipment Date], 110) for "mm-dd-yyyy"
    - Due Date: CONVERT(DATE, [Due Date], 110) for "mm-dd-yyyy"
    - All monetary columns: CAST AS DECIMAL(18,2)
    - Weight: CAST AS DECIMAL(18,6) (preserves precision from CSV)
    ================================================================================
    */

    INSERT INTO billing.flavorcloud_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        order_number,
        tracking_number,
        service_level,
        terms_of_trade,
        origin_location,
        destination_country,
        ship_to_zip,
        carrier_name,
        total_weight,
        weight_unit,
        length,
        width,
        height,
        dimension_unit,
        commissions,
        duties,
        taxes,
        fees,
        landed_cost,
        insurance,
        shipping_charges,
        order_value,
        shipment_total_charges,
        shipment_date,
        payment_terms,
        due_date
    )
    SELECT
        cb.carrier_bill_id,

        d.[Invoice Number] AS invoice_number,
        CONVERT(DATE, d.[Date], 107) AS invoice_date,
        d.[Order Number] AS order_number,
        TRIM(d.[Shipment Number]) AS tracking_number, -- Using Shipment Number as tracking number

        d.[Service Level] AS service_level,
        d.[Terms Of Trade] AS terms_of_trade,
        d.[Origin Location] AS origin_location,
        d.[Destination Country] AS destination_country,
        d.[Ship To Address Zip] AS ship_to_zip,
        d.[Carrier] AS carrier_name,

        CAST(d.[Total Weight] AS DECIMAL(18,6)) AS total_weight,
        d.[Weight Unit] AS weight_unit,
        CAST(d.[Length] AS DECIMAL(18,2)) AS length,
        CAST(d.[Width] AS DECIMAL(18,2)) AS width,
        CAST(d.[Height] AS DECIMAL(18,2)) AS height,
        d.[Dimension Unit] AS dimension_unit,

        CAST(d.[Commissions (USD)] AS DECIMAL(18,2)) AS commissions,
        CAST(d.[Duties (USD)] AS DECIMAL(18,2)) AS duties,
        CAST(d.[Taxes (USD)] AS DECIMAL(18,2)) AS taxes,
        CAST(d.[Fees (USD)] AS DECIMAL(18,2)) AS fees,
        CAST(d.[LandedCost (Duty + Taxes + Fees) (USD)] AS DECIMAL(18,2)) AS landed_cost,
        CAST(d.[Insurance (USD)] AS DECIMAL(18,2)) AS insurance,
        CAST(d.[Shipping Charges (USD)] AS DECIMAL(18,2)) AS shipping_charges,
        CAST(d.[Order Value (USD)] AS DECIMAL(18,2)) AS order_value,
        CAST(d.[Shipment Total Charges (USD)] AS DECIMAL(18,2)) AS shipment_total_charges,

        CONVERT(DATE, d.[Shipment Date], 110) AS shipment_date,
        d.[Payment Terms] AS payment_terms,
        CONVERT(DATE, d.[Due Date], 110) AS due_date

    FROM billing.delta_flavorcloud_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number = d.[Invoice Number]
        AND cb.bill_date = CONVERT(DATE, d.[Date], 107)
        AND cb.carrier_id = @Carrier_id
    WHERE
        d.[Invoice Number] IS NOT NULL
        AND NULLIF(TRIM(d.[Invoice Number]), '') IS NOT NULL
        AND d.[Invoice Number] != 'Total'
        AND d.[Shipment Number] IS NOT NULL
        AND NULLIF(TRIM(d.[Shipment Number]), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.flavorcloud_bill t
            WHERE t.carrier_bill_id = cb.carrier_bill_id
        );

    SET @LineItemsInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Commit and Return Results
    ================================================================================
    */
    COMMIT TRANSACTION;

    SELECT 
        'SUCCESS' AS Status,
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

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
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        '[FlavorCloud] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - Transaction wraps both INSERTs for atomicity
✅ #3  - Direct CAST / CONVERT (fail fast), no TRY_CAST / TRY_CONVERT
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #7  - Unit conversion handled in Insert_Unified_tables.sql (LB → OZ)
✅ #8  - Returns Status, InvoicesInserted, LineItemsInserted
✅ #9  - Line items NOT EXISTS check uses carrier_bill_id only
✅ #11 - Charge categories handled in Insert_Charge_Types.sql (all = Other/11)
================================================================================
*/
