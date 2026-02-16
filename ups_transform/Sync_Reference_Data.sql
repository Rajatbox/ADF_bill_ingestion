/*
================================================================================
Sync Reference Data Script (UPS)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @carrier_id: INT - UPS carrier_id from LookupCarrierInfo.sql
    - @lastrun: DATETIME2 - Last successful run time from LookupCarrierInfo.sql
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsInserted: INT
    - ChargeTypesInserted: INT
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Discovers and inserts NEW shipping methods and charge types from ups_bill
         into reference tables. Runs incrementally based on created_date.

Source:   billing.ups_bill
Targets:  dbo.shipping_method
          dbo.charge_types

Transaction: NO TRANSACTION - Each INSERT is independently idempotent via NOT EXISTS
Idempotency: Safe to re-run - inserts only if not exists (with carrier_id check)

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ShippingMethodsInserted INT, @ChargeTypesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Discover and Insert New Shipping Methods
    ================================================================================
    Extracts unique service types from ups_bill where charge_category_code = 'SHP'
    AND charge_classification_code = 'FRT' (freight charges only).
    Only inserts NEW shipping methods not already in the reference table.
    ================================================================================
    */

    INSERT INTO dbo.shipping_method (
        carrier_id,
        method_name,
        service_level,
        average_transit_days,
        guaranteed_delivery,
        is_active
    )
    SELECT DISTINCT
        @carrier_id AS carrier_id,
        ub.charge_description AS method_name,
        'Standard' AS service_level,  -- Default service level
        NULL AS average_transit_days,  -- To be configured later
        0 AS guaranteed_delivery,      -- Default false
        1 AS is_active
    FROM
        billing.ups_bill AS ub
    WHERE
        ub.created_date > @lastrun
        AND ub.charge_category_code = 'SHP'
        AND ub.charge_classification_code = 'FRT'
        AND ub.charge_description IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method AS sm
            WHERE sm.carrier_id = @carrier_id
                AND sm.method_name = ub.charge_description
        );

    SET @ShippingMethodsInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Discover and Insert New Charge Types
    ================================================================================
    Extracts ALL unique charge descriptions from ups_bill and applies category logic
    based on charge_category_code:
    - charge_category_code = 'ADJ': charge_category_id = 16 (FK to dbo.charge_type_category)
    - All others: charge_category_id = 11 (FK to dbo.charge_type_category)
    
    Freight flag determined by:
    - charge_category_code = 'SHP' -> freight = 1
    - All others -> freight = 0
    
    Only inserts NEW charge types not already in the reference table.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        dt,
        markup,
        charge_category_id  -- FK to dbo.charge_type_category.id
    )
    SELECT DISTINCT
        @carrier_id AS carrier_id,
        ub.charge_description AS charge_name,
        CASE 
            WHEN ub.charge_category_code = 'SHP' THEN 1
            ELSE 0
        END AS freight,
        0 AS dt,      -- Default: not a dimensional weight charge
        0 AS markup,  -- Default: not a markup
        CASE 
            WHEN ub.charge_category_code = 'ADJ' THEN 16  -- FK to charge_type_category
            ELSE 11  -- FK to charge_type_category
        END AS charge_category_id
    FROM
        billing.ups_bill AS ub
    WHERE
        ub.created_date > @lastrun
        AND ub.charge_description IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.charge_types AS ct
            WHERE ct.carrier_id = @carrier_id
                AND ct.charge_name = ub.charge_description
        );

    SET @ChargeTypesInserted = @@ROWCOUNT;

    -- Return success metrics
    SELECT
        'SUCCESS' AS Status,
        @ShippingMethodsInserted AS ShippingMethodsInserted,
        @ChargeTypesInserted AS ChargeTypesInserted;

END TRY
BEGIN CATCH
    -- Return descriptive error details (no rollback needed - no transaction)
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    -- Build descriptive error message
    DECLARE @DetailedError NVARCHAR(4000) = 
        'UPS Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
