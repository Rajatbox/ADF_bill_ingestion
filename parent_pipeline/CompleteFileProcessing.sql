/*
================================================================================
Parent Pipeline: Complete File Processing (Update Status to Completed)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @File_id: INT - File tracking surrogate key from ValidateAndInitializeFile

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - Message: VARCHAR - Confirmation message
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Updates file_ingestion_tracker record to mark file as completed after
         successful pipeline execution. Sets completion timestamp.

Execution Order: LAST in pipeline (after Load_to_gold.sql completes successfully)

Note: No separate failure handler needed. ValidateAndInitializeFile.sql creates
      records without completed_at timestamp. If pipeline fails, completed_at
      remains NULL (in-progress/failed state). Only success path sets timestamp.
================================================================================
*/

SET NOCOUNT ON;

BEGIN TRY

    -- Mark file as completed
    UPDATE billing.file_ingestion_tracker
    SET completed_at = SYSDATETIME()
    WHERE file_id = @File_id;

    -- Verify update succeeded
    IF @@ROWCOUNT = 0
    BEGIN
        RAISERROR('File record not found for file_id=%d', 16, 1, @File_id);
        RETURN;
    END

    -- Return success
    SELECT 
        'SUCCESS' AS Status,
        FORMATMESSAGE('File processing completed successfully. file_id=%d', @File_id) AS Message;

END TRY
BEGIN CATCH
    -- Return error details for ADF monitoring
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;
    
    -- Re-throw error for ADF pipeline to catch
    THROW;
END CATCH;

