/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ChargeTypesAdded: INT - Number of new charge types discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Automatically populate and maintain reference tables by discovering
         new values from processed GOFO billing data.

         Block 1: Sync shipping_method (discovered from gofo_bill.product;
                  blanks already defaulted to 'GOFO Parcel Pickup' upstream)
         Block 2: Sync charge_types dynamically (discovered from gofo_bill's 13
                  fixed charge columns, unpivoted inline; freight=1 only for
                  'Delivery Fee')
         Block 3: Seed charge_types statically (ONE-TIME, idempotent) -- GOFO's
                  13 charge types are fixed and known up front, so this can run
                  every time with no effect once seeded. Comment out at will;
                  Block 2 stays as a live fallback either way.

Source:  billing.gofo_bill + carrier_bill JOIN (file_id filtered)
Targets: dbo.shipping_method
         dbo.charge_types

File-Based Filtering: Uses @File_id to process only the current file's data:
         - Joins carrier_bill to filter by file_id

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.

Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

/*
================================================================================
Block 1: Synchronize Shipping Methods
================================================================================
Discovers distinct products from gofo_bill and inserts any new methods into
the shipping_method table. Populates with sensible defaults:
- carrier_id: From @Carrier_id parameter
- method_name: The product value from GOFO data (blanks already defaulted
  to 'GOFO Parcel Pickup' in Insert_ELT_&_CB.sql)
- service_level: Default to 'Standard'
- guaranteed_delivery: Default to 0 (false)
- is_active: Default to 1 (true)
================================================================================
*/

INSERT INTO dbo.shipping_method (
    carrier_id,
    method_name,
    service_level,
    guaranteed_delivery,
    is_active
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    CAST(gofo.product AS varchar(255)) AS method_name,
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active
FROM
    billing.gofo_bill gofo
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = gofo.carrier_bill_id
WHERE
    cb.file_id = @File_id
    AND gofo.product IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.shipping_method sm
        WHERE sm.method_name = CAST(gofo.product AS varchar(255))
            AND sm.carrier_id = @Carrier_id
    );

SET @ShippingMethodsAdded = @@ROWCOUNT;

/*
================================================================================
Block 2: Synchronize Charge Types (Dynamic Discovery)
================================================================================
Discovers distinct charge types from gofo_bill's 13 fixed charge columns,
unpivoted inline (OUTER APPLY VALUES) -- no view, since the column-to-charge
mapping is static and known at design time (unlike FedEx's dynamic charges,
where a view is warranted). Inserts any new charge types into dbo.charge_types.

Freight flag: 'Delivery Fee' is the base transportation charge -> freight = 1.
              All others -> freight = 0.
dt flag:      Not applicable to GOFO (no dimensional-weight-specific charge) -> 0.
Category (Design Constraint #11 - only Adjustment(16) and Other(11) known):
              'Credit for Delivery Fees' and 'Credit for Order Value' -> Adjustment (16).
              All other charge types -> Other (11).
================================================================================
*/

;WITH gofo_charges AS (
    SELECT
        chg.charge_type
    FROM billing.gofo_bill gofo
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = gofo.carrier_bill_id
    OUTER APPLY (
        VALUES
            (N'Delivery Fee',                   gofo.delivery_fee),
            (N'Overweight Fees',                gofo.overweight_fees),
            (N'Oversized Fees',                 gofo.oversized_fees),
            (N'Return Fees',                     gofo.return_fees),
            (N'Remote Area Fees',                gofo.remote_area_fees),
            (N'Fuel Surcharges',                 gofo.fuel_surcharges),
            (N'Delivery Area Fees',              gofo.delivery_area_fees),
            (N'Additional Interception Fees',    gofo.additional_interception_fees),
            (N'Relabelling Fee',                 gofo.relabelling_fee),
            (N'Return Reship Fee',               gofo.return_reship_fee),
            (N'Credit for Delivery Fees',        gofo.credit_for_delivery_fees),
            (N'Credit for Order Value',          gofo.credit_for_order_value),
            (N'Other',                           gofo.other_fee)
    ) chg (charge_type, charge_amount)
    WHERE cb.file_id = @File_id
      AND chg.charge_amount IS NOT NULL
      AND chg.charge_amount <> 0
)
INSERT INTO dbo.charge_types (
    carrier_id,
    charge_name,
    freight,
    dt,
    charge_category_id
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    gc.charge_type AS charge_name,
    CASE WHEN gc.charge_type = N'Delivery Fee' THEN 1 ELSE 0 END AS freight,
    0 AS dt,
    CASE WHEN gc.charge_type IN (N'Credit for Delivery Fees', N'Credit for Order Value')
         THEN 16 ELSE 11 END AS charge_category_id
FROM
    gofo_charges gc
WHERE
    gc.charge_type IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.charge_name = gc.charge_type
            AND ct.carrier_id = @Carrier_id
    );

SET @ChargeTypesAdded = @@ROWCOUNT;

/*
================================================================================
Block 3: Synchronize Charge Types (Static Seed, One-Time)
================================================================================
GOFO's 13 charge types are fixed and known up front -- no per-file discovery
is actually required. This seeds all 13 directly, idempotent via NOT EXISTS,
so it's harmless to leave running on every file even after Block 2 has already
covered the same ground (or forever, if never commented out).
================================================================================
*/

INSERT INTO dbo.charge_types (
    carrier_id,
    charge_name,
    freight,
    dt,
    charge_category_id
)
SELECT charge_data.carrier_id, charge_data.charge_name, charge_data.freight, charge_data.dt, charge_data.charge_category_id
FROM (
    VALUES
        (@Carrier_id, N'Delivery Fee',                   1, 0, 11),
        (@Carrier_id, N'Overweight Fees',                0, 0, 11),
        (@Carrier_id, N'Oversized Fees',                 0, 0, 11),
        (@Carrier_id, N'Return Fees',                    0, 0, 11),
        (@Carrier_id, N'Remote Area Fees',               0, 0, 11),
        (@Carrier_id, N'Fuel Surcharges',                0, 0, 11),
        (@Carrier_id, N'Delivery Area Fees',             0, 0, 11),
        (@Carrier_id, N'Additional Interception Fees',   0, 0, 11),
        (@Carrier_id, N'Relabelling Fee',                0, 0, 11),
        (@Carrier_id, N'Return Reship Fee',              0, 0, 11),
        (@Carrier_id, N'Credit for Delivery Fees',       0, 0, 16),
        (@Carrier_id, N'Credit for Order Value',         0, 0, 16),
        (@Carrier_id, N'Other',                          0, 0, 11)
) AS charge_data(carrier_id, charge_name, freight, dt, charge_category_id)
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.charge_types ct
    WHERE ct.charge_name = charge_data.charge_name
        AND ct.carrier_id = @Carrier_id
);

SET @ChargeTypesAdded = @ChargeTypesAdded + @@ROWCOUNT;

    SELECT
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded,
        @ChargeTypesAdded AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'GOFO Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
