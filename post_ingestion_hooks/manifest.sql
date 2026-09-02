/*
================================================================================
Post-Ingestion Hook — Manifest Tenant
================================================================================
Inputs:  @File_id (INT), @Carrier_id (INT)
Outputs: Status, RecordsAssigned (success) | Status, ErrorNumber, ErrorMessage, ErrorLine (error)

Purpose: Tenant-specific post-ingestion logic for Manifest.
         Run this directly on manifest_db to create or update the hook.

Execution Order: After Load_to_gold, before Complete File Processing.
================================================================================
*/

CREATE OR ALTER PROCEDURE dbo.usp_post_ingestion_hook
    @File_id    INT,
    @Carrier_id INT
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @RecordsAssigned INT;

    BEGIN TRY

        -- ── DHL (carrier_id = 5) ─────────────────────────────────────────────
        IF @Carrier_id = 5
        BEGIN

            -- Assign unknowns to Burlebo based on account number
            UPDATE ccl
            SET    ccl.customer_id       = (SELECT customer_id FROM dbo.[3pl_customer] WHERE customer_name = 'burlebo'),
                   ccl.status            = 'matched',
                   ccl.status_updated_at = SYSUTCDATETIME()
            FROM   dbo.carrier_cost_ledger ccl
            JOIN   billing.carrier_bill    cb ON cb.carrier_bill_id = ccl.carrier_bill_id
            WHERE  ccl.carrier_id    = 5
              AND  cb.account_number = '5123934'
              AND  ccl.status        = 'unknown';

        END

        -- ── FedEx (carrier_id = 10) ───────────────────────────────────────────
        IF @Carrier_id = 10
        BEGIN

            -- Step 1 — order_number → single customer match
            -- Pre-filter [order] to only refs with exactly one customer before joining.
            -- The HAVING clause is the guard against multi-match contamination.
            WITH single_match AS (
                SELECT
                    order_number,
                    MIN([3pl_customer_id]) AS customer_id
                FROM dbo.[order]
                GROUP BY order_number
                HAVING COUNT(DISTINCT [3pl_customer_id]) = 1
            )
            UPDATE ccl
            SET    ccl.customer_id       = sm.customer_id,
                   ccl.status            = 'matched',
                   ccl.status_updated_at = SYSUTCDATETIME()
            FROM   dbo.carrier_cost_ledger ccl
            JOIN   billing.fedex_bill      fb ON fb.carrier_bill_id               = ccl.carrier_bill_id
                                             AND fb.express_or_ground_tracking_id = ccl.tracking_number
            JOIN   single_match            sm ON sm.order_number                   = fb.original_customer_reference
            WHERE  ccl.carrier_id  = 10
              AND  ccl.status      = 'unknown'
              AND  ccl.customer_id IS NULL;

            -- Step 2 — external_id (Ref#2) disambiguates multi-match
            -- Requires original_ref2 column to be added to billing.fedex_bill first.
            -- For rows still unresolved after step 1, original_customer_reference returns multiple customers.
            -- original_ref2 is the ShipHero order ID on the label — unique per order row — which breaks the tie.
            WITH ext_match AS (
                SELECT
                    o.external_id,
                    o.[3pl_customer_id] AS customer_id
                FROM dbo.[order] o
                GROUP BY o.external_id, o.[3pl_customer_id]
            )
            UPDATE ccl
            SET    ccl.customer_id       = em.customer_id,
                   ccl.status            = 'matched',
                   ccl.status_updated_at = SYSUTCDATETIME()
            FROM   dbo.carrier_cost_ledger ccl
            JOIN   billing.fedex_bill      fb ON fb.carrier_bill_id               = ccl.carrier_bill_id
                                             AND fb.express_or_ground_tracking_id = ccl.tracking_number
            JOIN   ext_match               em ON em.external_id                    = fb.original_ref2
            WHERE  ccl.carrier_id  = 10
              AND  ccl.status      = 'unknown'
              AND  ccl.customer_id IS NULL;

            -- Step 3 — shipper name → customer name
            -- Requires shipper_company + shipper_name columns to be added to billing.fedex_bill first.
            -- Shipper string on the label uniquely identifies the customer (confirmed across all July unknowns — no cross-customer collisions).
            UPDATE ccl
            SET    ccl.customer_id       = c.customer_id,
                   ccl.status            = 'matched',
                   ccl.status_updated_at = SYSUTCDATETIME()
            FROM   dbo.carrier_cost_ledger ccl
            JOIN   billing.carrier_bill    cb ON cb.carrier_bill_id               = ccl.carrier_bill_id
            JOIN   billing.fedex_bill      fb ON fb.carrier_bill_id               = ccl.carrier_bill_id
                                             AND fb.express_or_ground_tracking_id = ccl.tracking_number
            JOIN   dbo.[3pl_customer]      c  ON c.customer_name                  = ISNULL(fb.shipper_company, fb.shipper_name)
            WHERE  ccl.carrier_id  = 10
              AND  ccl.status      = 'unknown'
              AND  ccl.customer_id IS NULL;

            -- Step 4 — backfill shipping_method on shipment_attributes from ShipHero fees
            UPDATE sa
            SET    sa.shipping_method    = 'Fedex 2 Day - One Rate'
            FROM   billing.shipment_attributes  sa
            JOIN   stage.shiphero_shipping_fees ssf ON ssf.tracking_number = sa.tracking_number
            WHERE  sa.carrier_id = 10
              AND  ssf.[method]  = 'Fedex 2 Day - One Rate';

            -- Step 5 — backfill shipping_method_id on carrier_cost_ledger from ShipHero fees
            UPDATE ccl
            SET    ccl.shipping_method_id = (
                       SELECT sm.shipping_method_id
                       FROM   dbo.shipping_method sm
                       WHERE  sm.carrier_id  = 10
                         AND  sm.method_name = 'Fedex 2 Day - One Rate'
                   )
            FROM   dbo.carrier_cost_ledger      ccl
            JOIN   stage.shiphero_shipping_fees ssf ON ssf.tracking_number = ccl.tracking_number
            WHERE  ccl.carrier_id = 10
              AND  ssf.[method]   = 'Fedex 2 Day - One Rate';

        END

        -- ── UPS (carrier_id = 4) ──────────────────────────────────────────────
        IF @Carrier_id = 4
        BEGIN

            -- Step 1 — order_number → single customer match
            -- Pre-filter [order] to refs with exactly one customer. HAVING COUNT = 1 prevents
            -- multi-customer order numbers from contaminating the assignment.
            WITH single_match AS (
                SELECT
                    order_number,
                    MIN([3pl_customer_id]) AS customer_id
                FROM dbo.[order]
                GROUP BY order_number
                HAVING COUNT(DISTINCT [3pl_customer_id]) = 1
            )
            UPDATE ccl
            SET    ccl.customer_id       = sm.customer_id,
                   ccl.status            = 'matched',
                   ccl.status_updated_at = SYSUTCDATETIME()
            FROM   dbo.carrier_cost_ledger ccl
            JOIN   billing.carrier_bill    cb ON cb.carrier_bill_id = ccl.carrier_bill_id
            JOIN   billing.ups_bill        ub ON ub.carrier_bill_id = ccl.carrier_bill_id
                                             AND ub.tracking_number = ccl.tracking_number
            JOIN   single_match            sm ON sm.order_number    = ub.shipment_reference_1
            WHERE  ccl.carrier_id = 4
              AND  ccl.status     = 'unknown';

            -- Step 2 — Ref2 (external_id) resolves anything still unmatched
            -- shipment_reference_2 is the ShipHero external_id — unique per order row — sufficient on its own.
            UPDATE ccl
            SET    ccl.customer_id       = o.[3pl_customer_id],
                   ccl.status            = 'matched',
                   ccl.status_updated_at = SYSUTCDATETIME()
            FROM   dbo.carrier_cost_ledger ccl
            JOIN   billing.ups_bill        ub ON ub.carrier_bill_id              = ccl.carrier_bill_id
                                             AND ub.tracking_number              = ccl.tracking_number
            JOIN   dbo.[order]             o  ON CAST(o.external_id AS nvarchar) = ub.shipment_reference_2
            WHERE  ccl.carrier_id = 4
              AND  ccl.status     = 'unknown';

        END

        SET @RecordsAssigned = @@ROWCOUNT;

        SELECT
            'SUCCESS'        AS Status,
            @RecordsAssigned AS RecordsAssigned;

    END TRY
    BEGIN CATCH

        SELECT
            'ERROR'          AS Status,
            ERROR_NUMBER()   AS ErrorNumber,
            ERROR_MESSAGE()  AS ErrorMessage,
            ERROR_LINE()     AS ErrorLine;

        THROW;

    END CATCH;

END
