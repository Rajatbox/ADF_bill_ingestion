/*
================================================================================
Sync Reference Data Script (USPS)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - USPS carrier_id from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ChargeTypesInserted: INT
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Discovers and inserts NEW charge types from usps_bill into dbo.charge_types.
         No shipping_method sync: USPS's EPS billing feed has no distinct service/rate
         concept to register (mail_class is stored on shipment_attributes only).

Source:   billing.usps_bill + carrier_bill JOIN (file_id filtered)
Targets:  dbo.charge_types

Charge Naming / Categorization (CASE-driven off tran_type):
  - PURCHASE   -> 'Postage'                       | freight = 1 | charge_category_id = 15 (Transportation)
  - ADJUSTMENT -> 'ADJ - {assessment_type}'        | freight = 0 | charge_category_id = 16 (Adjustment)
    USPS lumps many distinct adjustment reasons (postage delta, dimensional, zone,
    unused label, etc.) under tran_type = ADJUSTMENT. assessment_type distinguishes
    them, so each distinct assessment_type gets its own charge_type row instead of
    collapsing into one generic 'Adjustment' bucket. Falls back to 'ADJ - UNKNOWN'
    if assessment_type is NULL/blank.
  - REFUND     -> 'Refund'                         | freight = 0 | charge_category_id = 11 (Other)
  - Any other/future tran_type value -> the tran_type value itself, as-is | freight = 0
    | charge_category_id = 11 (Other) - so a new tran_type the feed starts emitting
    later is captured under its own distinct charge_type rather than silently merged
    into an existing bucket.

File-Based Filtering: Uses @File_id to process only the current file's data:
         - Joins carrier_bill to filter by file_id

Transaction: NO TRANSACTION - Each INSERT is independently idempotent via NOT EXISTS
Idempotency: Safe to re-run - inserts only if not exists (with carrier_id check)

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ChargeTypesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Discover and Insert New Charge Types
    ================================================================================
    Derives charge_name / freight / charge_category_id from tran_type via CASE.
    Only inserts NEW charge types not already registered for this carrier.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        dt,
        markup,
        charge_category_id
    )
    SELECT DISTINCT
        @Carrier_id AS carrier_id,
        CASE ub.tran_type
            WHEN 'PURCHASE'   THEN 'Postage'
            WHEN 'ADJUSTMENT' THEN 'ADJ - ' + ISNULL(NULLIF(TRIM(ub.assessment_type), ''), 'UNKNOWN')
            WHEN 'REFUND'     THEN 'Refund'
            ELSE ub.tran_type  -- Future/unknown tran_type: register as its own charge type
        END AS charge_name,
        CASE ub.tran_type
            WHEN 'PURCHASE' THEN 1
            ELSE 0
        END AS freight,
        0 AS dt,      -- Default: not a dimensional weight charge
        0 AS markup,  -- Default: not a markup
        CASE ub.tran_type
            WHEN 'PURCHASE'   THEN 15  -- Transportation
            WHEN 'ADJUSTMENT' THEN 16  -- Adjustment
            WHEN 'REFUND'     THEN 11  -- Other
            ELSE 11                    -- Other (unknown tran_type)
        END AS charge_category_id
    FROM
        billing.usps_bill AS ub
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = ub.carrier_bill_id
    WHERE
        cb.file_id = @File_id  -- File-based filtering
        AND ub.tran_type IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.charge_types AS ct
            WHERE ct.carrier_id = @Carrier_id
                AND ct.charge_name = CASE ub.tran_type
                    WHEN 'PURCHASE'   THEN 'Postage'
                    WHEN 'ADJUSTMENT' THEN 'ADJ - ' + ISNULL(NULLIF(TRIM(ub.assessment_type), ''), 'UNKNOWN')
                    WHEN 'REFUND'     THEN 'Refund'
                    ELSE ub.tran_type
                END
        );

    SET @ChargeTypesInserted = @@ROWCOUNT;

    -- Return success metrics
    SELECT
        'SUCCESS' AS Status,
        @ChargeTypesInserted AS ChargeTypesInserted;

END TRY
BEGIN CATCH
    -- Return descriptive error details (no rollback needed - no transaction)
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'USPS Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
