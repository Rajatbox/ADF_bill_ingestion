/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
                         Used to associate new reference data with correct carrier
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ChargeTypesAdded: INT - Number of new charge types discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Idempotent synchronization of reference data tables:
         
         Block 1: Sync shipping_method table with new service types
         Block 2: Sync charge_types table with 18 UniUni charge categories

Source:   Test.uniuni_bill (for shipping methods)
          Static list (for charge types - 18 UniUni charge categories)

Targets:  test.shipping_method
          test.charge_types

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
                 If a wrong bill is processed, the transaction in Insert_ELT_&_CB.sql
                 will rollback, protecting reference data integrity.

Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

/*
================================================================================
Block 1: Synchronize Shipping Methods
================================================================================
Discovers distinct service types from uniuni_bill and inserts any new
methods into the shipping_method table. Populates with sensible defaults:
- carrier_id: From @Carrier_id parameter
- method_name: The actual service type from UniUni data
- service_level: Default to 'Standard'
- guaranteed_delivery: Default to 0 (false)
- is_active: Default to 1 (true)
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
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active
FROM
    Test.uniuni_bill AS ub
WHERE
    ub.created_date > @lastrun
    AND ub.service_type IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM test.shipping_method AS sm
        WHERE sm.method_name = ub.service_type
            AND sm.carrier_id = @Carrier_id
    );

SET @ShippingMethodsAdded = @@ROWCOUNT;

/*
================================================================================
Block 2: Synchronize Charge Types
================================================================================
Inserts all 18 UniUni charge types into charge_types table.
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
        WHERE ct.charge_name = charges.charge_name
            AND ct.carrier_id = @Carrier_id
    );

SET @ChargeTypesAdded = @@ROWCOUNT;

-- Final single result set for the ADF Debug Window
SELECT 
    @ShippingMethodsAdded AS ShippingMethodsAdded, 
    @ChargeTypesAdded AS ChargeTypesAdded;
