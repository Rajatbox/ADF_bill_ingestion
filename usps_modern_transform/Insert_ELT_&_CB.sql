/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id:    INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status:            'SUCCESS' or 'ERROR'
    - InvoicesInserted:  INT - Number of carrier_bill records inserted (0 or 1)
    - LineItemsInserted: INT - Number of usps_modern_bill records inserted
    - ErrorNumber:       INT (if error)
    - ErrorMessage:      NVARCHAR (if error)
    - ErrorLine:         INT (if error)

Purpose: Two-step transactional data insertion with file tracking.

    Source: billing.delta_usps_modern_bill
            ShipHero label API export — one row per shipment, pre-filtered to USPS Modern.

    Step 1: Insert one carrier_bill row per file.
            bill_number  = 'USPS_Modern_' + MIN(shipment_created_date date)
                                           + '_' + MAX(shipment_created_date date)
            bill_date    = MAX(CAST(shipment_created_date AS DATE))
            total_amount = SUM(cost)
            account_number = MAX(carrier_account_id)
            Idempotency: NOT EXISTS on file_id (one row per file).

    Step 2: Insert usps_modern_bill (one row per shipment).
            Dim/weight values parsed from embedded-unit strings:
              dim_weight  "1.5562 lb"   → billed_weight_lb DECIMAL
              dim_height  "1.0000 inch" → billed_height_in DECIMAL
              dim_width   "12.00 inch"  → billed_width_in  DECIMAL
              dim_length  "20.00 inch"  → billed_length_in DECIMAL
            Idempotency: NOT EXISTS on carrier_bill_id only (Design Constraint #9).

Sources:  billing.delta_usps_modern_bill
Targets:  billing.carrier_bill (invoice summary)
          billing.usps_modern_bill (shipment line items)

Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @InvoicesInserted  INT;
DECLARE @LineItemsInserted INT;

BEGIN TRANSACTION;
BEGIN TRY

    -- ============================================================
    -- STEP 1: Insert carrier_bill
    -- One row per file. Invoice number constructed from min/max of
    -- shipment_created_date across all rows in the delta table.
    -- NOT EXISTS on file_id alone is sufficient (single row per file).
    -- ============================================================

    INSERT INTO billing.carrier_bill (
        carrier_id,
        bill_number,
        bill_date,
        total_amount,
        num_shipments,
        account_number,
        file_id
    )
    SELECT
        @Carrier_id                                                 AS carrier_id,
        'USPS_Modern_'
            + CONVERT(NVARCHAR(10), MIN(CAST(d.shipment_created_date AS DATE)), 23)
            + '_'
            + CONVERT(NVARCHAR(10), MAX(CAST(d.shipment_created_date AS DATE)), 23)
                                                                    AS bill_number,
        MAX(CAST(d.shipment_created_date AS DATE))                  AS bill_date,
        SUM(CAST(d.cost AS DECIMAL(18,2)))                          AS total_amount,
        COUNT(DISTINCT NULLIF(TRIM(d.tracking_number), ''))         AS num_shipments,
        MAX(d.carrier_account_id)                                   AS account_number,
        @File_id                                                    AS file_id
    FROM billing.delta_usps_modern_bill d
    WHERE NULLIF(TRIM(d.tracking_number), '') IS NOT NULL
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    -- ============================================================
    -- STEP 2: Insert usps_modern_bill
    -- One row per shipment. Joins to the carrier_bill row just
    -- inserted (via file_id + carrier_id) to get carrier_bill_id.
    -- dim_weight / dim_height / dim_width / dim_length strip the
    -- trailing unit suffix with CHARINDEX — CAST fails fast on
    -- malformed values (Design Constraint #3).
    -- NOT EXISTS on carrier_bill_id only (Design Constraint #9).
    -- ============================================================

    INSERT INTO billing.usps_modern_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        tracking_number,
        label_id,
        label_legacy_id,
        order_number,
        order_account_id,
        shipment_created_date,
        label_created_date,
        shipping_method,
        billed_weight_lb,
        billed_height_in,
        billed_length_in,
        billed_width_in,
        cost,
        status,
        box_name,
        carrier_account_id,
        warehouse
    )
    SELECT
        cb.carrier_bill_id,
        cb.bill_number                                                              AS invoice_number,
        cb.bill_date                                                                AS invoice_date,
        NULLIF(TRIM(d.tracking_number), '')                                         AS tracking_number,
        NULLIF(TRIM(d.label_id), '')                                                AS label_id,
        NULLIF(TRIM(d.label_legacy_id), '')                                         AS label_legacy_id,
        NULLIF(TRIM(d.order_number), '')                                            AS order_number,
        NULLIF(TRIM(d.order_account_id), '')                                        AS order_account_id,
        CAST(d.shipment_created_date AS DATETIME2)                                  AS shipment_created_date,
        CAST(d.label_created_date AS DATETIME2)                                     AS label_created_date,
        NULLIF(TRIM(d.shipping_method), '')                                         AS shipping_method,
        CAST(LEFT(d.dim_weight, CHARINDEX(' ', d.dim_weight) - 1) AS DECIMAL(18,4)) AS billed_weight_lb,
        CAST(LEFT(d.dim_height, CHARINDEX(' ', d.dim_height) - 1) AS DECIMAL(18,4)) AS billed_height_in,
        CAST(LEFT(d.dim_length, CHARINDEX(' ', d.dim_length) - 1) AS DECIMAL(18,4)) AS billed_length_in,
        CAST(LEFT(d.dim_width,  CHARINDEX(' ', d.dim_width)  - 1) AS DECIMAL(18,4)) AS billed_width_in,
        CAST(d.cost AS DECIMAL(18,2))                                               AS cost,
        NULLIF(TRIM(d.status), '')                                                  AS status,
        NULLIF(TRIM(d.box_name), '')                                                AS box_name,
        NULLIF(TRIM(d.carrier_account_id), '')                                      AS carrier_account_id,
        NULLIF(TRIM(d.warehouse), '')                                               AS warehouse
    FROM billing.delta_usps_modern_bill d
    INNER JOIN billing.carrier_bill cb
        ON  cb.file_id    = @File_id
        AND cb.carrier_id = @Carrier_id
    WHERE NULLIF(TRIM(d.tracking_number), '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM billing.usps_modern_bill u
          WHERE u.carrier_bill_id = cb.carrier_bill_id
      );

    SET @LineItemsInserted = @@ROWCOUNT;

COMMIT TRANSACTION;

    SELECT
        'SUCCESS'           AS Status,
        @InvoicesInserted   AS InvoicesInserted,
        @LineItemsInserted  AS LineItemsInserted;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

    DECLARE @ErrorMessage  NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine     INT            = ERROR_LINE();
    DECLARE @ErrorNumber   INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[USPS Modern] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
