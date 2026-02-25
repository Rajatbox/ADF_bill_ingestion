/*
================================================================================
Backfill Script: Carrier Cost Ledger - WMS Enrichment
================================================================================
Purpose: When billing data arrives before WMS data, carrier_cost_ledger rows are
         inserted with status='unknown' and NULL WMS columns. Once new records
         land in shipment_package, this script backfills the WMS-sourced columns
         for any cost ledger rows that now have a matching shipment_package.

Columns Updated:
  - shipping_method_id    (from shipping_method via shipment_attributes)
  - shipment_package_id   (from shipment_package.shipment_package_id)
  - shipment_external_id  (from shipment.external_id)
  - customer_id           (from order.[3pl_customer_id])
  - status                (from vw_recon_variance + WMS match logic)

Scope: Only rows where shipment_package_id IS NULL (not yet matched to WMS)
       and a shipment_package match now exists by tracking_number.

Execution: Safe to run multiple times. No parameters required.
           Intended to run after new shipment_package records are loaded.
================================================================================
*/

SET NOCOUNT ON;
GO

DECLARE @RowsUpdated INT;

BEGIN TRY

    PRINT 'Starting carrier_cost_ledger WMS backfill...';
    PRINT '';

    UPDATE ccl
    SET
        ccl.shipping_method_id   = sm.shipping_method_id,
        ccl.shipment_package_id  = spw.shipment_package_id,
        ccl.shipment_external_id = sw.external_id,
        ccl.customer_id          = o.[3pl_customer_id],
        ccl.status = CASE
            WHEN rv.is_weight_exception = 1 THEN rv.weight_exception_type
            WHEN rv.is_cost_exception   = 1 THEN rv.cost_exception_type
            ELSE 'matched'
        END
    FROM dbo.carrier_cost_ledger AS ccl
    LEFT JOIN billing.shipment_attributes AS sa
        ON sa.id = ccl.shipment_attribute_id
    LEFT JOIN dbo.shipping_method AS sm
        ON sm.method_name = sa.shipping_method
        AND sm.carrier_id = sa.carrier_id
        AND ISNULL(sm.integrated_carrier_id, 0) = ISNULL(sa.integrated_carrier_id, 0)
    JOIN dbo.shipment_package AS spw
        ON spw.tracking_number = ccl.tracking_number
    LEFT JOIN dbo.shipment AS sw
        ON sw.shipment_id = spw.shipment_id
    LEFT JOIN dbo.[order] AS o
        ON o.order_id = sw.order_id
    LEFT JOIN dbo.vw_recon_variance AS rv
        ON rv.shipment_package_id = spw.shipment_package_id
    WHERE ccl.shipment_package_id IS NULL;

    SET @RowsUpdated = @@ROWCOUNT;

    PRINT 'Backfill completed successfully.';
    PRINT 'Rows updated: ' + CAST(@RowsUpdated AS VARCHAR(10));
    PRINT '';

END TRY
BEGIN CATCH
    PRINT '';
    PRINT 'Error during backfill:';
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
    PRINT '';

    THROW;
END CATCH;
GO
