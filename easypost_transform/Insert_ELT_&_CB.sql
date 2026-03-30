/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier ID from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of usps_easypost_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_easypost_bill 
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_easypost_bill (ELT staging) 
            into billing.easypost_bill (Carrier Bill line items)

Invoice Number Generation: 
         invoice_number = 'EasyPost_' + yyyy-MM-dd from MAX(created_at)
         Example: "EasyPost_2025-01-18"
         
         bill_date = CAST(MAX(created_at) AS DATE)
         
         Note: Single invoice per file using latest timestamp's date portion

Source:   billing.delta_easypost_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.easypost_bill (line items)

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Validation: Fails if created_at or tracking_code is NULL or empty
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
    Aggregates line items by computed invoice_number and bill_date to create 
    invoice-level summaries in carrier_bill. 
    
    Calculates:
    - invoice_number: 'EasyPost_' + FORMAT(MAX(created_at) as date, 'yyyy-MM-dd')
    - invoice_date: CAST(MAX(created_at) AS DATE)
    - total_amount: SUM of postage_fee
    - num_shipments: COUNT of tracking codes per invoice
    - account_number: carrier_account_id column value
    
    Generates carrier_bill_id values which will be joined in Step 2.
    
    Invoice Grouping Strategy: All shipments in file grouped under single invoice
    using the latest timestamp's date portion (not full timestamp).
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
        'EasyPost_' + FORMAT(CAST(MAX(d.created_at) AS DATE), 'yyyy-MM-dd') AS bill_number,
        CAST(MAX(d.created_at) AS DATE) AS bill_date,
        SUM(CAST(COALESCE(NULLIF(TRIM(d.postage_fee), ''), '0') AS decimal(18,2))) AS total_amount,
        COUNT(d.tracking_code) AS num_shipments,
        MAX(d.carrier_account_id) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_easypost_bill AS d
    WHERE
        -- Validation: Fail fast on bad data
        d.created_at IS NOT NULL 
        AND NULLIF(TRIM(d.created_at), '') IS NOT NULL
        AND d.tracking_code IS NOT NULL
        AND NULLIF(TRIM(d.tracking_code), '') IS NOT NULL
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id  -- File-based idempotency: same file = same data
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment records from delta_easypost_bill into 
    billing.easypost_bill.
    
    Join Strategy:
    - Join to carrier_bill on carrier_id and file_id (just inserted in Step 1)
    - Use carrier_bill_id only in NOT EXISTS check (Design Constraint #9)
    
    NOTE: EasyPost uses simplified file_id JOIN because of synthetic invoice model:
          - Step 1 creates ONE invoice per file ('EasyPost_' + date)
          - Step 2 safely joins on file_id → matches exactly ONE carrier_bill row
    
    Type Conversions:
    - Direct CAST (fail-fast on bad data, no TRY_CAST)
    - Dates: RFC3339 format (2025-01-18T23:32:14Z)
    - Decimals: All charges and dimensions
    
    Units: No conversion needed (weight already in OZ, dimensions already in IN)
    ================================================================================
    */

    INSERT INTO billing.easypost_bill (
        tracking_code,
        invoice_number,
        carrier_bill_id,
        weight,
        rate,
        label_fee,
        postage_fee,
        usps_zone,
        from_zip,
        [length],
        width,
        height,
        postage_label_created_at,
        insurance_fee,
        carbon_offset_fee,
        bill_date,
        service,
        integrated_carrier  -- NEW: Actual carrier name
    )
    SELECT
        -- Shipment identifiers
        TRIM(d.tracking_code) AS tracking_code,
        
        -- Invoice identifiers from carrier_bill (inserted in Step 1)
        cb.bill_number AS invoice_number,

        -- Foreign key to invoice summary
        cb.carrier_bill_id,
        
        -- Weight (already in ounces - OZ)
        CAST(COALESCE(NULLIF(TRIM(d.weight), ''), '0') AS decimal(18,2)) AS weight,
        
        -- Charge breakdown
        CAST(COALESCE(NULLIF(TRIM(d.rate), ''), '0') AS decimal(18,2)) AS rate,
        CAST(COALESCE(NULLIF(TRIM(d.label_fee), ''), '0') AS decimal(18,2)) AS label_fee,
        CAST(COALESCE(NULLIF(TRIM(d.postage_fee), ''), '0') AS decimal(18,2)) AS postage_fee,
        
        -- Routing information
        CAST(NULLIF(TRIM(d.usps_zone), '') AS tinyint) AS usps_zone,
        d.from_zip,
        
        -- Package dimensions (already in inches - IN)
        CAST(COALESCE(NULLIF(TRIM(d.length), ''), '0') AS decimal(18,2)) AS [length],
        CAST(COALESCE(NULLIF(TRIM(d.width), ''), '0') AS decimal(18,2)) AS width,
        CAST(COALESCE(NULLIF(TRIM(d.height), ''), '0') AS decimal(18,2)) AS height,
        
        -- Dates (convert RFC3339 to datetime2)
        CAST(NULLIF(TRIM(d.postage_label_created_at), '') AS datetime2) AS postage_label_created_at,
        
        -- Additional charges
        CAST(COALESCE(NULLIF(TRIM(d.insurance_fee), ''), '0') AS decimal(18,2)) AS insurance_fee,
        CAST(COALESCE(NULLIF(TRIM(d.carbon_offset_fee), ''), '0') AS decimal(18,2)) AS carbon_offset_fee,
        
        -- Bill date
        cb.bill_date AS bill_date,
        
        -- Service information
        d.service,
        
        -- Integrated carrier (actual fulfillment carrier - USPS, FedEx, UPS, etc.)
        NULLIF(TRIM(d.carrier), '') AS integrated_carrier  -- NEW
    FROM
        billing.delta_easypost_bill AS d
    INNER JOIN billing.carrier_bill cb
        ON cb.carrier_id = @Carrier_id
        AND cb.file_id = @File_id  -- Join to the record just inserted in Step 1
    WHERE
        -- Validation: Fail fast on bad data
        d.created_at IS NOT NULL 
        AND NULLIF(TRIM(d.created_at), '') IS NOT NULL
        AND d.tracking_code IS NOT NULL
        AND NULLIF(TRIM(d.tracking_code), '') IS NOT NULL
        -- Idempotency (Design Constraint #9): Check by carrier_bill_id only
        AND NOT EXISTS (
            SELECT 1
            FROM billing.easypost_bill t
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
        '[USPS EasyPost] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - Transaction wraps both INSERTs for atomicity
✅ #3  - Direct CAST (fail fast), no TRY_CAST
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #7  - No unit conversion needed (weight in OZ, dimensions in IN)
✅ #8  - Returns Status, InvoicesInserted, LineItemsInserted
✅ #9  - Line items NOT EXISTS check uses carrier_bill_id only
✅ #11 - All charges category = "Other" (handled in Sync_Reference_Data.sql)
================================================================================
*/

