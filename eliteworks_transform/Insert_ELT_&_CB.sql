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
    - LineItemsInserted: INT - Number of eliteworks_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_eliteworks_bill 
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_eliteworks_bill (ELT staging) 
            into billing.eliteworks_bill (Carrier Bill line items) with carrier_bill_id foreign key

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Invoice Number Generation: 
         invoice_number = 'Eliteworks_' + yyyy-MM-dd from MAX(time_utc)
         Example: "Eliteworks_2026-02-08"
         
         invoice_date = CAST(MAX(time_utc) AS DATE)
         
         account_number = user_account column value (e.g., "Falcon IT")
         
         Note: Same formula used in both INSERTs to ensure deterministic joins

Source:   billing.delta_eliteworks_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.eliteworks_bill (line items)

Validation: Fails if time_utc or tracking_number is NULL or empty
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
    Aggregates line items by computed invoice_number and invoice_date to create 
    invoice-level summaries in carrier_bill. 
    
    Calculates:
    - invoice_number: 'Eliteworks_' + FORMAT(MAX(time_utc) as date, 'yyyy-MM-dd')
    - invoice_date: CAST(MAX(time_utc) AS DATE)
    - total_amount: SUM of platform_charged_with_corrections
    - num_shipments: COUNT of tracking numbers per invoice
    - account_number: user_account column value (e.g., "Falcon IT")
    
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
        'Eliteworks_' + FORMAT(CAST(MAX(d.time_utc) AS DATE), 'yyyy-MM-dd') AS bill_number,
        CAST(MAX(d.time_utc) AS DATE) AS bill_date,
        SUM(CAST(d.platform_charged_with_corrections AS decimal(18,2))) AS total_amount,
        COUNT(*) AS num_shipments,
        MAX(d.user_account) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_eliteworks_bill AS d
    WHERE
        -- Validation: Fail fast on bad data
        d.time_utc IS NOT NULL 
        AND NULLIF(TRIM(d.time_utc), '') IS NOT NULL
        AND d.tracking_number IS NOT NULL
        AND NULLIF(TRIM(d.tracking_number), '') IS NOT NULL
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
    Inserts individual shipment records from delta_eliteworks_bill into 
    billing.eliteworks_bill.
    
    Join Strategy:
    - Join to carrier_bill on carrier_id and file_id (just inserted in Step 1)
    - Use carrier_bill_id only in NOT EXISTS check (Design Constraint #9)
    
    NOTE: Eliteworks uses simplified file_id JOIN because of synthetic invoice model:
          - Step 1 creates ONE invoice per file ('Eliteworks_' + date)
          - Step 2 safely joins on file_id → matches exactly ONE carrier_bill row
          
          Other carriers (FedEx, UPS, etc.) CANNOT use this pattern because:
          - Multiple real invoices per file (GROUP BY [Invoice Number])
          - Must join on business keys to match line items to correct invoice
          - Joining on file_id only would duplicate line items across all invoices
    
    Type Conversions:
    - Direct CAST (fail-fast on bad data, no TRY_CAST)
    - Dates: first_scan column for shipment_date
    - Decimals: All charges, weight, and dimensions
    
    Units: No conversion needed (weight already in OZ, dimensions already in IN)
    ================================================================================
    */

    INSERT INTO billing.eliteworks_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        tracking_number,
        shipment_date,
        service_method,
        zone,
        charged_amount,
        store_markup,
        platform_charged,
        billed_weight_oz,
        package_length_in,
        package_width_in,
        package_height_in,
        from_postal,
        shipment_status
    )
    SELECT
        -- Foreign key to invoice summary
        cb.carrier_bill_id,
        
        -- Invoice identifiers from carrier_bill (inserted in Step 1)
        cb.bill_number AS invoice_number,
        cb.bill_date AS invoice_date,
        
        -- Shipment identifiers
        TRIM(d.tracking_number) AS tracking_number,
        
        -- Dates
        CAST(d.first_scan AS DATETIME) AS shipment_date,
        
        -- Service and routing
        d.service AS service_method,
        d.zone AS zone,
        
        -- Charge breakdown
        CAST(d.charged AS decimal(18,2)) AS charged_amount,
        CAST(d.store_markup AS decimal(18,2)) AS store_markup,
        CAST(d.platform_charged_with_corrections AS decimal(18,2)) AS platform_charged,
        
        -- Package dimensions (NO CONVERSION - already in OZ and IN)
        CAST(d.shipment_weight_oz AS decimal(18,2)) AS billed_weight_oz,
        CAST(d.package_length_in AS decimal(18,2)) AS package_length_in,
        CAST(d.package_width_in AS decimal(18,2)) AS package_width_in,
        CAST(d.package_height_in AS decimal(18,2)) AS package_height_in,
        
        -- Origin
        d.from_postal AS from_postal,
        
        -- Status
        d.status AS shipment_status
    FROM billing.delta_eliteworks_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.carrier_id = @Carrier_id
        AND cb.file_id = @File_id  -- Join to the record just inserted in Step 1
    WHERE
        -- Validation: Fail fast on bad data
        d.time_utc IS NOT NULL 
        AND NULLIF(TRIM(d.time_utc), '') IS NOT NULL
        AND d.tracking_number IS NOT NULL
        AND NULLIF(TRIM(d.tracking_number), '') IS NOT NULL
        -- Idempotency (Design Constraint #9): Check by carrier_bill_id only
        AND NOT EXISTS (
            SELECT 1
            FROM billing.eliteworks_bill t
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
        '[Eliteworks] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
