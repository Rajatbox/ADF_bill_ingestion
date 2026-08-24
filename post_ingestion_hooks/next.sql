/*
================================================================================
Post-Ingestion Hook — Next Tenant
================================================================================
Inputs:  @File_id (INT), @Carrier_id (INT)
Outputs: Status, Message (success) | Status, ErrorNumber, ErrorMessage, ErrorLine (error)

Purpose: Tenant-specific post-ingestion logic for Next.
         Override dbo.usp_post_ingestion_hook on next_db with this body.

Execution Order: After Load_to_gold, before Complete File Processing.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @RecordsAssigned INT;

BEGIN TRY

    -- Assign UPS unknowns for Logicx Crew accounts to customer_id = 1
    UPDATE ccl
    SET    ccl.customer_id        = 1,               -- LogixCrew
           ccl.status             = 'matched',
           ccl.status_updated_at  = SYSUTCDATETIME()
    FROM   dbo.carrier_cost_ledger ccl
    JOIN   billing.carrier_bill    cb ON cb.carrier_bill_id = ccl.carrier_bill_id
    JOIN   dbo.carrier             c  ON c.carrier_id       = ccl.carrier_id
    WHERE  cb.file_id            = @File_id
      AND  LOWER(c.carrier_name) = 'ups'
      AND  cb.account_number     IN ('0000XH9591', '00000H3W29')
      AND  ccl.status            = 'unknown';

    SET @RecordsAssigned = @@ROWCOUNT;

    SELECT
        'SUCCESS'            AS Status,
        @RecordsAssigned     AS RecordsAssigned;

END TRY
BEGIN CATCH

    SELECT
        'ERROR'          AS Status,
        ERROR_NUMBER()   AS ErrorNumber,
        ERROR_MESSAGE()  AS ErrorMessage,
        ERROR_LINE()     AS ErrorLine;

    THROW;

END CATCH;
