/*
================================================================================
Rollback Script: Delete all traces of a file from the database
================================================================================
Carriers: EasyPost, Eliteworks, Vanlo

Input: @File_id — everything else resolved via JOIN to carrier_bill
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @File_id INT = /* enter file_id */;

BEGIN TRANSACTION;
BEGIN TRY

    -- 1. carrier_cost_ledger
    DELETE ccl
    FROM dbo.carrier_cost_ledger ccl
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = ccl.carrier_bill_id
    WHERE cb.file_id = @File_id;

    -- 2. shipment_charges + capture attribute IDs before deleting
    SELECT DISTINCT sc.shipment_attribute_id
    INTO #attr_ids
    FROM billing.shipment_charges sc
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
    WHERE cb.file_id = @File_id;

    DELETE sc
    FROM billing.shipment_charges sc
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
    WHERE cb.file_id = @File_id;

    -- 3. shipment_attributes
    DELETE sa
    FROM billing.shipment_attributes sa
    JOIN #attr_ids a ON a.shipment_attribute_id = sa.id;

    DROP TABLE #attr_ids;

    -- 4. easypost_bill
    DELETE eb
    FROM billing.easypost_bill eb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = eb.carrier_bill_id
    WHERE cb.file_id = @File_id;

    -- 5. eliteworks_bill
    DELETE ew
    FROM billing.eliteworks_bill ew
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = ew.carrier_bill_id
    WHERE cb.file_id = @File_id;

    -- 6. vanlo_bill
    DELETE vb
    FROM billing.vanlo_bill vb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = vb.carrier_bill_id
    WHERE cb.file_id = @File_id;

    -- 7. carrier_bill
    DELETE FROM billing.carrier_bill WHERE file_id = @File_id;

    -- 8. file_ingestion_tracker
    DELETE FROM billing.file_ingestion_tracker WHERE file_id = @File_id;

    COMMIT TRANSACTION;
    SELECT 'SUCCESS' AS Status;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    SELECT 'ERROR' AS Status, ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
    THROW;
END CATCH;
