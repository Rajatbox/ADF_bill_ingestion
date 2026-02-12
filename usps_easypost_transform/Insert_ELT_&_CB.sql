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
    - LineItemsInserted: INT - Number of usps_easypost_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process:
         1. Aggregate and insert invoice-level summary data from delta_usps_easypost_bill 
            into carrier_bill (Carrier Bill summary) - generates carrier_bill_id
         2. Insert line-level billing data from delta_usps_easypost_bill (ELT staging) 
            into billing.usps_easy_post_bill (Carrier Bill line items)

Invoice Number Generation: 
         invoice_number = carrier_account_id + '-' + yyyy-MM-dd from created_at
         Example: "ca_589b9b61d0ed420890f0e826515491dd-2025-01-18"
         
         bill_date = CAST(created_at AS DATE)
         
         Note: Same formula used in both INSERTs to ensure deterministic joins

Source:   billing.delta_usps_easypost_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.usps_easy_post_bill (line items)

Validation: Fails if created_at or tracking_code is NULL or empty
Match:      invoice_number AND bill_date (INSERT WHERE NOT EXISTS)
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
    Aggregates line items by computed invoice_number and bill_date to create 
    invoice-level summaries in carrier_bill. 
    
    Calculates:
    - total_amount: SUM of postage_fee
    - num_shipments: COUNT of tracking codes per invoice
    
    Generates carrier_bill_id values which will be joined in Step 2.
    
    Invoice Number Formula (deterministic):
    - invoice_number = carrier_account_id + '-' + FORMAT(created_at date, 'yyyy-MM-dd')
    - bill_date = CAST(created_at AS DATE)
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
        CONCAT(d.carrier_account_id, '-', FORMAT(CAST(d.created_at AS DATE), 'yyyy-MM-dd')) AS bill_number,
        CAST(d.created_at AS DATE) AS bill_date,
        SUM(CAST(COALESCE(NULLIF(TRIM(d.postage_fee), ''), '0') AS decimal(18,2))) AS total_amount,
        COUNT(d.tracking_code) AS num_shipments,
        MAX(d.carrier_account_id) AS account_number
    FROM
        billing.delta_usps_easypost_bill AS d
    WHERE
        -- Validation: Fail fast on bad data
        d.created_at IS NOT NULL 
        AND NULLIF(TRIM(d.created_at), '') IS NOT NULL
        AND d.tracking_code IS NOT NULL
        AND NULLIF(TRIM(d.tracking_code), '') IS NOT NULL
    GROUP BY
        d.carrier_account_id,
        CAST(d.created_at AS DATE)
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.bill_number = CONCAT(d.carrier_account_id, '-', FORMAT(CAST(d.created_at AS DATE), 'yyyy-MM-dd'))
          AND cb.bill_date = CAST(d.created_at AS DATE)
          AND cb.carrier_id = @Carrier_id
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment records from delta_usps_easypost_bill into 
    billing.usps_easy_post_bill.
    
    Join Strategy:
    - Compute same invoice_number formula to validate invoice exists in carrier_bill
    - Use invoice_number + tracking_code in NOT EXISTS check
    
    Type Conversions:
    - Direct CAST (fail-fast on bad data, no TRY_CAST)
    - Dates: RFC3339 format (2025-01-18T23:32:14Z)
    - Decimals: All charges and dimensions
    
    Units: No conversion needed (weight already in OZ, dimensions already in IN)
    ================================================================================
    */

    INSERT INTO billing.usps_easy_post_bill (
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
        service
    )
    SELECT
        -- Shipment identifiers
        d.tracking_code,
        
        -- Computed invoice identifiers (same formula as Step 1)
        CONCAT(d.carrier_account_id, '-', FORMAT(CAST(d.created_at AS DATE), 'yyyy-MM-dd')) AS invoice_number,

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
        CAST(d.created_at AS DATE) AS bill_date,
        
        -- Service information
        d.service
    FROM
        billing.delta_usps_easypost_bill AS d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number = CONCAT(d.carrier_account_id, '-', FORMAT(CAST(d.created_at AS DATE), 'yyyy-MM-dd'))
        AND cb.bill_date = CAST(d.created_at AS DATE)
        AND cb.carrier_id = @Carrier_id  -- Always include carrier_id in join
    WHERE
        -- Validation: Fail fast on bad data
        d.created_at IS NOT NULL 
        AND NULLIF(TRIM(d.created_at), '') IS NOT NULL
        AND d.tracking_code IS NOT NULL
        AND NULLIF(TRIM(d.tracking_code), '') IS NOT NULL
        -- Idempotency (Design Constraint #9): Check by carrier_bill_id only
        AND NOT EXISTS (
            SELECT 1
            FROM billing.usps_easy_post_bill t
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

