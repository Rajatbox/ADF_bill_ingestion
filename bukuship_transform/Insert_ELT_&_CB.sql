/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of bukuship_bill line items inserted
    - ErrorNumber: INT (if error);
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate charge rows by AccountNumber + InvoiceDate into carrier_bill
            (one invoice summary row per account+date, SUM of NetCost)
         2. Insert one bukuship_bill row per charge row (narrow format —
            each row is a distinct charge for a tracking number)

Tracking Number Logic:
         - Landmark Global rows: TrackingNumber column used as-is
         - DHL eCommerce rows: Constructed as '420' + ReceiverZipCode + WaybillNumber
           (CASE on CarrierName to select correct tracking identifier)

Dimensions: NULL only when empty/missing, explicit zeros are preserved

Source:   billing.delta_bukuship_bill
Targets:  billing.carrier_bill (invoice summaries)
          billing.bukuship_bill (charge-level line items)

Match:    Step 1: file_id (INSERT WHERE NOT EXISTS)
          Step 2: carrier_bill_id only (INSERT WHERE NOT EXISTS) per Design Constraint #9
Transaction: Both inserts wrapped in transaction for atomicity

Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @InvoicesInserted INT, @LineItemsInserted INT;

BEGIN TRANSACTION;

BEGIN TRY

    /*
    ================================================================================
    Step 1: Insert Invoice-Level Summary Data
    ================================================================================
    Groups all charge rows by AccountNumber + InvoiceDate.
    SUM(NetCost) = total invoice amount.
    File-based idempotency: skip if file_id already exists in carrier_bill.
    ================================================================================
    */

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
        @Carrier_id AS carrier_id,
        d.AccountNumber + '_' + FORMAT(CAST(d.InvoiceDate AS DATE), 'ddMMyy') AS bill_number,
        CAST(d.InvoiceDate AS DATE) AS bill_date,
        SUM(CAST(d.NetCost AS DECIMAL(18,2))) AS total_amount,
        COUNT(DISTINCT
            CASE
                WHEN LOWER(d.CarrierName) LIKE '%dhl%'
                    THEN '420' + d.ReceiverZipCode + d.TrackingNumber
                ELSE d.TrackingNumber
            END
        ) AS num_shipments,
        MAX(d.AccountNumber) AS account_number,
        @File_id AS file_id
    FROM billing.delta_bukuship_bill AS d
    WHERE NULLIF(TRIM(d.AccountNumber), '') IS NOT NULL
      AND NULLIF(TRIM(d.InvoiceDate), '') IS NOT NULL
      AND NULLIF(TRIM(d.NetCost), '') IS NOT NULL
    GROUP BY d.AccountNumber, d.InvoiceDate
    HAVING NOT EXISTS (
        SELECT 1
        FROM billing.carrier_bill cb
        WHERE cb.file_id = @File_id
    );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Charge-Level Line Items into bukuship_bill
    ================================================================================
    One row per charge row from the delta table.
    Tracking number derived:
      - DHL eCommerce: '420' + ReceiverZipCode + TrackingNumber
      - All others (Landmark Global etc.): TrackingNumber column

    Dimensions stored as NULL only when empty (NULLIF only, explicit zeros preserved).

    Join to carrier_bill on AccountNumber + InvoiceDate + carrier_id to
    resolve carrier_bill_id. NOT EXISTS checks carrier_bill_id only
    (Design Constraint #9).
    ================================================================================
    */

    INSERT INTO billing.bukuship_bill (
        carrier_bill_id,
        carrier_name,
        account_number,
        invoice_date,
        ship_date,
        tracking_number,
        lead_tracking_number,
        charge_type,
        service_name,
        charge_name,
        [zone],
        net_cost,
        billed_weight,
        billed_weight_units,
        [length],
        [width],
        [height]
    )
    SELECT
        cb.carrier_bill_id,
        CASE WHEN LOWER(d.CarrierName) LIKE '%dhl%' THEN 'DHL' ELSE d.CarrierName END,
        d.AccountNumber,
        CAST(d.InvoiceDate AS DATE),
        NULLIF(TRIM(d.ShipDate), ''),

        -- Tracking number: DHL uses constructed key, others use TrackingNumber
        CASE
            WHEN LOWER(d.CarrierName) LIKE '%dhl%'
                THEN '420' + d.ReceiverZipCode + d.TrackingNumber
            ELSE d.TrackingNumber
        END AS tracking_number,

        NULLIF(TRIM(d.LeadTrackingNumber), ''),
        TRIM(d.ChargeType),
        NULLIF(TRIM(d.ServiceName), ''),
        TRIM(d.ChargeName),
        NULLIF(TRIM(d.Zone), ''),

        CAST(d.NetCost AS DECIMAL(18,2)),

        CASE WHEN NULLIF(TRIM(d.BilledWeight), '') IS NOT NULL
             THEN CAST(d.BilledWeight AS DECIMAL(18,6))
             ELSE NULL
        END,
        NULLIF(TRIM(d.BilledWeightUnits), ''),

        -- Dimensions: NULL only when empty string, preserve explicit zeros
        CASE WHEN NULLIF(TRIM(d.Length), '') IS NOT NULL
             THEN CAST(d.Length AS DECIMAL(18,2)) ELSE NULL END,
        CASE WHEN NULLIF(TRIM(d.Width), '') IS NOT NULL
             THEN CAST(d.Width AS DECIMAL(18,2)) ELSE NULL END,
        CASE WHEN NULLIF(TRIM(d.Height), '') IS NOT NULL
             THEN CAST(d.Height AS DECIMAL(18,2)) ELSE NULL END

    FROM billing.delta_bukuship_bill d
    INNER JOIN billing.carrier_bill cb
        ON cb.bill_number = d.AccountNumber + '_' + FORMAT(CAST(d.InvoiceDate AS DATE), 'ddMMyy')
        AND cb.bill_date  = CAST(d.InvoiceDate AS DATE)
        AND cb.carrier_id = @Carrier_id
    WHERE NULLIF(TRIM(d.AccountNumber), '') IS NOT NULL
      AND NULLIF(TRIM(d.InvoiceDate), '') IS NOT NULL
      AND NULLIF(TRIM(d.NetCost), '') IS NOT NULL
      AND (LOWER(d.CarrierName) NOT LIKE '%dhl%' OR NULLIF(TRIM(d.ReceiverZipCode), '') IS NOT NULL)
      AND NOT EXISTS (
            SELECT 1
            FROM billing.bukuship_bill t
            WHERE t.carrier_bill_id = cb.carrier_bill_id
      );

    SET @LineItemsInserted = @@ROWCOUNT;

    COMMIT TRANSACTION;

    SELECT
        'SUCCESS'          AS [Status],
        @InvoicesInserted  AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Bukuship] Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS [Status],
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - Transaction wraps both INSERTs for atomicity
✅ #3  - Direct CAST (fail fast), no TRY_CAST
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status, InvoicesInserted, LineItemsInserted
✅ #9  - Line items NOT EXISTS check uses carrier_bill_id only
✅ #12 - file_id stored in carrier_bill; HAVING NOT EXISTS checks file_id
================================================================================
*/
