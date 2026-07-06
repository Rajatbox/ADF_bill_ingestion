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
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Idempotent synchronization of shipping_method reference data.
         Discovers new service levels from the current file's shipx_bill rows.

Note:     Charge types (Delivery Charge, Fuel Surcharge) are fixed and seeded
          once via a one-time setup script, not part of the regular pipeline.

Source:   billing.shipx_bill + carrier_bill JOIN (file_id filtered)
Targets:  dbo.shipping_method

Idempotent: Safe to run multiple times - NOT EXISTS prevents duplicates

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT;

BEGIN TRY

    INSERT INTO dbo.shipping_method (
        carrier_id,
        method_name,
        service_level,
        guaranteed_delivery,
        is_active
    )
    SELECT DISTINCT
        @Carrier_id AS carrier_id,
        sb.service_level AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active
    FROM
        billing.shipx_bill AS sb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sb.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND sb.service_level IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method AS sm
            WHERE sm.method_name = sb.service_level
                AND sm.carrier_id = @Carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    SELECT
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'Shipx Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
