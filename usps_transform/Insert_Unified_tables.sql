/*
================================================================================
Insert Unified Tables Script (USPS)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - USPS carrier_id from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShipmentsInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Populates unified gold layer tables:
         Part 1: shipment_attributes (minimal - shipment_date only, no unit conversions
                 apply since USPS's EPS feed carries no weight/dimension columns)
         Part 2: shipment_charges (itemized charges)

Source:   billing.usps_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes
          billing.shipment_charges

File-Based Filtering: Uses @File_id to process only the current file's data via carrier_bill JOIN

Transaction: NO TRANSACTION - Each INSERT is independently idempotent
Idempotency: shipment_attributes - INSERT ... WHERE NOT EXISTS (carrier_id + tracking_number).
             No MERGE/correction handling needed: shipment_date is set once from the
             earliest tran_date for that tracking number and never revised.
             shipment_charges - NOT EXISTS on (carrier_bill_id, tracking_number, charge_type_id),
             the standard project convention. Verified against the sample feed: no tracking
             number carries two rows of the same tran_type within one account's invoice, so
             this key does not collapse distinct legitimate charges.

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ShipmentsInserted INT, @ChargesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Part 1: Insert Shipment Attributes (Minimal - shipment_date only)
    ================================================================================
    One row per distinct tracking_number. shipment_date = earliest tran_date
    across all charge events for that tracking number within this file.
    No shipping_method / physical dims - not applicable to this feed.
    ================================================================================
    */

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        tracking_number,
        shipment_date,
        created_date,
        updated_date
    )
    SELECT
        @Carrier_id AS carrier_id,
        agg.tracking_number,
        agg.shipment_date,
        SYSDATETIME(),
        SYSDATETIME()
    FROM (
        SELECT
            ub.tracking_number,
            MIN(ub.tran_date) AS shipment_date
        FROM billing.usps_bill AS ub
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = ub.carrier_bill_id
        WHERE cb.file_id = @File_id  -- File-based filtering
            AND ub.tracking_number IS NOT NULL
        GROUP BY ub.tracking_number
    ) AS agg
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes AS sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = agg.tracking_number
        );

    SET @ShipmentsInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Part 2: Insert Shipment Charges (Itemized Charges)
    ================================================================================
    Inserts ALL charges (one row per usps_bill line) - amount = postage.
    charge_type_id resolved via the same tran_type CASE mapping used in
    Sync_Reference_Data.sql (Postage / ADJ - {assessment_type} / Refund / tran_type
    as-is). ADJUSTMENT rows are split by assessment_type since USPS reports many
    distinct adjustment reasons under one tran_type.
    ================================================================================
    */

    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id,
        created_date
    )
    SELECT
        @Carrier_id AS carrier_id,
        cb.carrier_bill_id,
        ub.tracking_number,
        ct.charge_type_id,
        ub.postage AS amount,
        sa.id AS shipment_attribute_id,
        SYSDATETIME() AS created_date
    FROM
        billing.usps_bill AS ub
    INNER JOIN billing.carrier_bill AS cb
        ON cb.carrier_bill_id = ub.carrier_bill_id
    INNER JOIN billing.shipment_attributes AS sa
        ON sa.carrier_id = @Carrier_id
        AND sa.tracking_number = ub.tracking_number
    INNER JOIN dbo.charge_types AS ct
        ON ct.carrier_id = @Carrier_id
        AND ct.charge_name = CASE ub.tran_type
            WHEN 'PURCHASE'   THEN 'Postage'
            WHEN 'ADJUSTMENT' THEN 'ADJ - ' + ISNULL(NULLIF(TRIM(ub.assessment_type), ''), 'UNKNOWN')
            WHEN 'REFUND'     THEN 'Refund'
            ELSE ub.tran_type
        END
    WHERE
        cb.file_id = @File_id  -- File-based filtering
        AND ub.postage <> 0  -- Exclude zero charges
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges AS sc
            WHERE sc.carrier_bill_id = cb.carrier_bill_id
                AND sc.tracking_number = ub.tracking_number
                AND sc.charge_type_id = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success metrics
    SELECT
        'SUCCESS' AS Status,
        @ShipmentsInserted AS ShipmentsInserted,
        @ChargesInserted AS ChargesInserted;

END TRY
BEGIN CATCH
    -- Return descriptive error details (no rollback needed - no transaction)
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'USPS Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
