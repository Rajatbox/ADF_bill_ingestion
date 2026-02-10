/*
================================================================================
Sync Script: Reference Data (Shipping Methods & Charge Types)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - UniUni carrier ID (from LookupCarrierInfo.sql)
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
                            Defaults to '2000-01-01' for first run
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods inserted
    - ChargeTypesAdded: INT - Number of new charge types inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Idempotent synchronization of reference data tables:
         1. Sync distinct shipping methods from uniuni_bill into shipping_method
         2. Sync charge types (15 UniUni charge categories) into charge_types

Source:   Test.uniuni_bill (for shipping methods)
          Static list (for charge types)
Targets:  test.shipping_method
          test.charge_types

Idempotency: Uses NOT EXISTS with carrier_id - safe to run multiple times
Transaction: NO TRANSACTION (each insert is independently idempotent via unique constraints)

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT = 0;
DECLARE @ChargeTypesAdded INT = 0;
DECLARE @lastrun DATETIME2 = '2000-01-01';  -- Default for first run, overridden by ADF parameter

BEGIN TRY
    /*
    ================================================================================
    Step 1: Sync Shipping Methods
    ================================================================================
    Extracts distinct service types from uniuni_bill and inserts new methods into
    shipping_method table. Only adds methods that don't already exist for this carrier.
    
    Incremental Processing: Filters by created_date > @lastrun to only process
    new records since last successful run.
    ================================================================================
    */

    INSERT INTO test.shipping_method (
        carrier_id,
        method_name,
        service_level,
        guaranteed_delivery,
        is_active
    )
    SELECT DISTINCT
        @Carrier_id AS carrier_id,
        ub.service_type AS method_name,
        'Standard' AS service_level,  -- Default service level
        0 AS guaranteed_delivery,     -- Not guaranteed by default
        1 AS is_active
    FROM
        Test.uniuni_bill AS ub
    WHERE
        ub.created_date > @lastrun    -- Incremental filter: only new records
        AND ub.service_type IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM test.shipping_method AS sm
            WHERE sm.carrier_id = @Carrier_id
                AND sm.method_name = ub.service_type
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Sync Charge Types
    ================================================================================
    Inserts all 15 UniUni charge types into charge_types table.
    Matches charge names from reference stored procedure (line 122-136).
    Only inserts charge types that don't already exist for this carrier.
    ================================================================================
    */

    INSERT INTO test.charge_types (
        carrier_id,
        charge_name,
        freight,
        dt,
        markup,
        category
    )
    SELECT
        @Carrier_id AS carrier_id,
        charge_name,
        freight,
        dt,
        markup,
        category
    FROM (
        VALUES
            ('Base Rate', 1, 0, 0, 'Base'),
            ('Discount Fee', 0, 0, 0, 'Discount'),
            ('Discount Percentage', 0, 0, 0, 'Discount'),
            ('Signature Fee', 0, 0, 1, 'Accessorial'),
            ('Pick Up Fee', 0, 0, 1, 'Accessorial'),
            ('Over Dimension Fee', 0, 0, 1, 'Surcharge'),
            ('Over Max Size Fee', 0, 0, 1, 'Surcharge'),
            ('Over Weight Fee', 0, 0, 1, 'Surcharge'),
            ('Fuel Surcharge', 0, 0, 1, 'Surcharge'),
            ('Peak Season Surcharge', 0, 0, 1, 'Surcharge'),
            ('Delivery Area Surcharge', 0, 0, 1, 'Surcharge'),
            ('Delivery Area Surcharge Extend', 0, 0, 1, 'Surcharge'),
            ('Truck Fee', 0, 0, 1, 'Accessorial'),
            ('Relabel Fee', 0, 0, 1, 'Accessorial'),
            ('Miscellaneous Fee', 0, 0, 1, 'Accessorial'),
            ('Credit Card Surcharge', 0, 0, 1, 'Surcharge'),
            ('Credit', 0, 0, 0, 'Adjustment'),
            ('Approved Claim', 0, 0, 0, 'Adjustment')
    ) AS charges(charge_name, freight, dt, markup, category)
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM test.charge_types AS ct
            WHERE ct.carrier_id = @Carrier_id
                AND ct.charge_name = charges.charge_name
        );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    -- Return success results
    SELECT 
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded,
        @ChargeTypesAdded AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    -- Return error details (no rollback needed - no transaction)
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;

END CATCH;
