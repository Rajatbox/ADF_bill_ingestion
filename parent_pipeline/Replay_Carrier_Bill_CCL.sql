/*
================================================================================
Replay: Carrier Cost Ledger — Carrier Bill Level
================================================================================
Input:   @Carrier_bill_ids table variable — add rows at top before executing
Outputs: LedgerDeleted, TrackedInserted, InvoiceInserted

Purpose:
  Full replay of dbo.carrier_cost_ledger for one or more carrier bills.
  Deletes all existing CCL rows for the given bills and re-derives from source.
  Handles both tracked shipment charges and invoice-level (null-tracking)
  account charges (e.g. payment processing fees, billing adjustments).

Idempotent: Safe to run multiple times — deletes first, then re-inserts.
Note:       Does not re-run Parts 1/2 from Load_to_gold (shipment/package
            dimension updates). Those are additive and remain intact.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @Carrier_bill_ids TABLE (carrier_bill_id INT); -- ← populate before running
INSERT INTO @Carrier_bill_ids VALUES
    (0); -- replace with your ids, e.g. (13), (14), (15)

DECLARE @LedgerDeleted      INT;
DECLARE @TrackedInserted    INT;
DECLARE @InvoiceInserted    INT;

IF EXISTS (
    SELECT 1 FROM @Carrier_bill_ids c
    WHERE NOT EXISTS (
        SELECT 1 FROM billing.carrier_bill cb
        WHERE cb.carrier_bill_id = c.carrier_bill_id
    )
)
BEGIN
    RAISERROR('One or more carrier_bill_ids not found in billing.carrier_bill.', 16, 1);
    RETURN;
END;

BEGIN TRY

    /*
    ============================================================================
    Step 1: Delete existing CCL rows for this bill
    ============================================================================
    */
    DELETE FROM dbo.carrier_cost_ledger
    WHERE carrier_bill_id IN (SELECT carrier_bill_id FROM @Carrier_bill_ids);

    SET @LedgerDeleted = @@ROWCOUNT;

    /*
    ============================================================================
    Step 2: Resolve tracked shipment charges into temp table
            Invoice-level (null-tracking) charges excluded here — handled in
            Step 4 without going through #BillShipments.
    ============================================================================
    */
    DROP TABLE IF EXISTS #BillShipments;

    WITH sa_id AS (
        SELECT DISTINCT sc.shipment_attribute_id
        FROM billing.shipment_charges sc
        WHERE sc.carrier_bill_id IN (SELECT carrier_bill_id FROM @Carrier_bill_ids)
          AND NULLIF(sc.tracking_number, '') IS NOT NULL
    )
    SELECT
        sa_id.shipment_attribute_id,
        sa.tracking_number,
        sa.destination_zone,
        sa.carrier_id,
        sa.shipment_date,
        sa.billed_weight_oz,
        sa.billed_length_in,
        sa.billed_width_in,
        sa.billed_height_in,
        spw.shipment_package_id,
        spw.shipment_id,
        sm.shipping_method_id,
        ROW_NUMBER() OVER (
            PARTITION BY sa.tracking_number
            ORDER BY spw.shipment_package_id DESC
        ) AS rn
    INTO #BillShipments
    FROM sa_id
    JOIN billing.shipment_attributes sa
        ON sa.id = sa_id.shipment_attribute_id
    LEFT JOIN dbo.shipment_package spw
        ON spw.tracking_number = sa.tracking_number
    LEFT JOIN dbo.shipping_method sm
        ON sm.method_name                        = sa.shipping_method
       AND sm.carrier_id                         = sa.carrier_id
       AND ISNULL(sm.integrated_carrier_id, 0)  = ISNULL(sa.integrated_carrier_id, 0);

    DELETE FROM #BillShipments WHERE rn > 1;

    CREATE NONCLUSTERED INDEX IX_BillShipments_sa
        ON #BillShipments (shipment_attribute_id)
        INCLUDE (tracking_number, shipment_package_id, shipment_id, shipping_method_id);

    /*
    ============================================================================
    Step 3: Insert tracked shipment charges into CCL
    ============================================================================
    */
    INSERT INTO dbo.carrier_cost_ledger (
        carrier_invoice_number,
        carrier_invoice_date,
        tracking_number,
        shipment_date,
        shipment_external_id,
        customer_id,
        carrier_id,
        shipping_method_id,
        category,
        cost_item,
        amount,
        charge_type_id,
        shipment_package_id,
        carrier_bill_id,
        shipment_attribute_id,
        status
    )
    SELECT
        cb.bill_number,
        cb.bill_date,
        fs.tracking_number,
        fs.shipment_date,
        sw.external_id,
        o.[3pl_customer_id],
        sc.carrier_id,
        fs.shipping_method_id,
        ctc.category,
        ct.charge_name,
        sc.amount,
        sc.charge_type_id,
        fs.shipment_package_id,
        sc.carrier_bill_id,
        sc.shipment_attribute_id,
        CASE
            WHEN rv.is_weight_exception = 1 THEN rv.weight_exception_type
            WHEN rv.is_cost_exception   = 1 THEN rv.cost_exception_type
            WHEN fs.shipment_package_id IS NOT NULL THEN 'matched'
            ELSE 'unknown'
        END
    FROM #BillShipments fs
    JOIN billing.shipment_charges sc
        ON  sc.shipment_attribute_id = fs.shipment_attribute_id
        AND sc.carrier_bill_id IN (SELECT carrier_bill_id FROM @Carrier_bill_ids)
    JOIN billing.carrier_bill cb
        ON  cb.carrier_bill_id       = sc.carrier_bill_id
    JOIN dbo.charge_types ct
        ON  ct.charge_type_id        = sc.charge_type_id
    JOIN dbo.charge_type_category ctc
        ON  ctc.category_id          = ct.charge_category_id
    LEFT JOIN dbo.vw_recon_variance rv
        ON  rv.shipment_package_id   = fs.shipment_package_id
    LEFT JOIN dbo.shipment sw
        ON  sw.shipment_id           = fs.shipment_id
    LEFT JOIN dbo.[order] o
        ON  o.order_id               = sw.order_id;

    SET @TrackedInserted = @@ROWCOUNT;

    /*
    ============================================================================
    Step 4: Insert invoice-level (null-tracking) charges into CCL
            Bypasses #BillShipments entirely — these charges have no shipment
            attributes to resolve. FK to shipment_attributes is satisfied via
            the sentinel row seeded in billing.shipment_attributes.
            status is always 'unknown' — no WMS match possible.
    ============================================================================
    */
    INSERT INTO dbo.carrier_cost_ledger (
        carrier_invoice_number,
        carrier_invoice_date,
        tracking_number,
        shipment_date,
        carrier_id,
        shipping_method_id,
        category,
        cost_item,
        amount,
        charge_type_id,
        shipment_package_id,
        carrier_bill_id,
        shipment_attribute_id,
        status
    )
    SELECT
        cb.bill_number,
        cb.bill_date,
        NULL,
        NULL,
        sc.carrier_id,
        NULL,
        ctc.category,
        ct.charge_name,
        sc.amount,
        sc.charge_type_id,
        NULL,
        sc.carrier_bill_id,
        sc.shipment_attribute_id,
        'unknown'
    FROM billing.shipment_charges sc
    JOIN billing.carrier_bill cb
        ON  cb.carrier_bill_id  = sc.carrier_bill_id
    JOIN dbo.charge_types ct
        ON  ct.charge_type_id   = sc.charge_type_id
    JOIN dbo.charge_type_category ctc
        ON  ctc.category_id     = ct.charge_category_id
    WHERE sc.carrier_bill_id IN (SELECT carrier_bill_id FROM @Carrier_bill_ids)
      AND sc.tracking_number IS NULL;

    SET @InvoiceInserted = @@ROWCOUNT;

    SELECT
        'SUCCESS'           AS Status,
        @LedgerDeleted      AS LedgerDeleted,
        @TrackedInserted    AS TrackedInserted,
        @InvoiceInserted    AS InvoiceInserted;

END TRY
BEGIN CATCH
    SELECT
        'ERROR'             AS Status,
        ERROR_NUMBER()      AS ErrorNumber,
        ERROR_MESSAGE()     AS ErrorMessage,
        ERROR_LINE()        AS ErrorLine;

    THROW;
END CATCH;
