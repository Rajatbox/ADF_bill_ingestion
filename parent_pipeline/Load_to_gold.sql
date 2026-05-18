/*
================================================================================
Load to Gold Layer - WMS Enrichment & Cost Ledger
================================================================================
Inputs:  @File_id (INT), @Carrier_id (INT)
Outputs: Status, ShipmentsUpdated, PackagesUpdated, LedgerInserted, Error details

Purpose:
  1. UPDATE dbo.shipment with zone and carrier info
  2. UPDATE dbo.shipment_package with dimensions, weights, dates, costs
  3. INSERT dbo.carrier_cost_ledger with itemized charges and matching status

Idempotent: Safe to rerun. Parts 1-2 update, Part 3 uses NOT EXISTS check.
Carrier-Agnostic: Works for all carriers using unified billing layer.
Execution Order: Runs after Insert_Unified_tables.sql

Performance:
  #FileShipments temp table pre-resolves tracking_number → shipment_package_id /
  shipment_id in a single pass using ROW_NUMBER() dedup (latest row per tracking
  number wins). Parts 1-3 then join on integers only — varchar mapping happens
  once, tracking number rotation is handled, and the optimizer gets real stats.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShipmentsUpdated INT, @PackagesUpdated INT, @LedgerInserted INT, @MarkupsInserted INT;

-- CTE scopes to distinct shipment_attribute_ids for this file and carrier.
-- Driving from sa_id (not shipment_charges directly) avoids the sc fanout —
-- one row per shipment enters the join, not one row per charge line.
-- ROW_NUMBER dedup then handles WMS tracking number rotation only.
DROP TABLE IF EXISTS #FileShipments;

WITH sa_id AS (
    SELECT DISTINCT sc.shipment_attribute_id
    FROM billing.shipment_charges sc
    JOIN billing.carrier_bill cb
        ON cb.carrier_bill_id = sc.carrier_bill_id
    WHERE cb.file_id = @File_id
      AND sc.carrier_id = @Carrier_id
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
INTO #FileShipments
FROM sa_id
JOIN billing.shipment_attributes sa
    ON sa.id = sa_id.shipment_attribute_id
LEFT JOIN dbo.shipment_package spw
    ON spw.tracking_number = sa.tracking_number
LEFT JOIN dbo.shipping_method sm
    ON sm.method_name                          = sa.shipping_method
   AND sm.carrier_id                           = sa.carrier_id
   AND ISNULL(sm.integrated_carrier_id, 0) = ISNULL(sa.integrated_carrier_id, 0);

-- Drop older WMS package rows — only the latest shipment_package_id per
-- tracking number survives. Duplicate suppression in the bill is already
-- enforced by the shipment_charges unique constraint upstream.
DELETE FROM #FileShipments WHERE rn > 1;

CREATE NONCLUSTERED INDEX IX_FileShipments_tracking
    ON #FileShipments (shipment_attribute_id)
    INCLUDE (tracking_number, shipment_package_id, shipment_id, shipping_method_id);

BEGIN TRY

    /*
    ================================================================================
    Part 1: Update shipment with zone and carrier info
    ================================================================================
    */
    UPDATE sw
    SET 
        sw.destination_zone = fs.destination_zone,
        sw.carrier_id       = fs.carrier_id
    FROM dbo.shipment AS sw
    JOIN #FileShipments fs
        ON fs.shipment_id = sw.shipment_id
    WHERE NULLIF(fs.tracking_number, '') IS NOT NULL;

    SET @ShipmentsUpdated = @@ROWCOUNT;

    /*
    ================================================================================
    Part 2: Update shipment_package with dimensions, weights, dates, costs
    ================================================================================
    */
    UPDATE spw
    SET 
        spw.carrier_pickup_date  = fs.shipment_date,
        spw.shipping_method_id   = fs.shipping_method_id,
        spw.billed_weight_oz     = fs.billed_weight_oz,
        spw.billed_length_in     = fs.billed_length_in,
        spw.billed_width_in      = fs.billed_width_in,
        spw.billed_height_in     = fs.billed_height_in,
        spw.billed_shipping_cost = vss.billed_shipping_cost
    FROM dbo.shipment_package AS spw
    JOIN #FileShipments fs
        ON fs.shipment_package_id = spw.shipment_package_id
    JOIN billing.vw_shipment_summary AS vss
        ON vss.id = fs.shipment_attribute_id
    WHERE NULLIF(fs.tracking_number, '') IS NOT NULL;

    SET @PackagesUpdated = @@ROWCOUNT;

    /*
    ================================================================================
    Part 3: Insert itemized charges into cost ledger with variance-based status
    ================================================================================
    Uses vw_recon_variance for intelligent status resolution:
    - Weight exceptions only flagged if cost variance exists (gated logic)
    - Status reflects combined cost + weight analysis
    - Unknown: No WMS match found
    - Matched: WMS match found, no variances
    - Cost/Weight exceptions: From variance view
    ================================================================================
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
        cb.bill_number AS carrier_invoice_number,
        cb.bill_date AS carrier_invoice_date,
        fs.tracking_number,
        fs.shipment_date,
        sw.external_id AS shipment_external_id,
        o.[3pl_customer_id] AS customer_id,
        sc.carrier_id,
        fs.shipping_method_id,
        ctc.category AS category,
        ct.charge_name AS cost_item,
        sc.amount,
        sc.charge_type_id,
        fs.shipment_package_id,
        sc.carrier_bill_id,
        sc.shipment_attribute_id,
        CASE 
            -- 1. If variance view found a weight exception (gated by cost exception)
            WHEN rv.is_weight_exception = 1 THEN rv.weight_exception_type
            -- 2. If variance view found a cost exception (but not weight)
            WHEN rv.is_cost_exception = 1 THEN rv.cost_exception_type
            -- 3. If WMS match exists but no exceptions
            WHEN fs.shipment_package_id IS NOT NULL THEN 'matched'
            -- 4. No WMS match found
            ELSE 'unknown'
        END AS status
    FROM #FileShipments fs
    JOIN billing.shipment_charges AS sc
        ON sc.shipment_attribute_id = fs.shipment_attribute_id
    JOIN billing.carrier_bill AS cb
        ON cb.carrier_bill_id = sc.carrier_bill_id
    JOIN dbo.charge_types AS ct
        ON ct.charge_type_id = sc.charge_type_id
    JOIN dbo.charge_type_category AS ctc
        ON ctc.category_id = ct.charge_category_id
    LEFT JOIN dbo.vw_recon_variance AS rv
        ON rv.shipment_package_id = fs.shipment_package_id
    LEFT JOIN dbo.shipment AS sw
        ON sw.shipment_id = fs.shipment_id
    LEFT JOIN dbo.[order] AS o
        ON o.order_id = sw.order_id
    WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.carrier_cost_ledger AS ccl
            WHERE ccl.shipment_attribute_id = sc.shipment_attribute_id
                AND ccl.carrier_bill_id = sc.carrier_bill_id
                AND ccl.charge_type_id = sc.charge_type_id
        );

    SET @LedgerInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Part 4: Sync shipping methods with default markups
    ================================================================================
    Ensures all shipping methods have a default markup entry in global_carrier_markups.
    Uses the latest active default markup from default_markups table.
    Assigns rule_id from dbo.seq_rule_id (one NEXT VALUE per new row).
    Idempotent: Uses NOT EXISTS to prevent duplicates.
    ================================================================================
    */
    INSERT INTO dbo.global_carrier_markups (
        rule_id,
        carrier_id,
        shipping_method_id,
        markup,
        markup_type,
        integrated_carrier_id
    )
    SELECT
        NEXT VALUE FOR dbo.seq_rule_id AS rule_id,
        sm.carrier_id,
        sm.shipping_method_id,
        dm.markup,
        'percentage' AS markup_type,
        sm.integrated_carrier_id
    FROM 
        dbo.shipping_method sm
    CROSS JOIN (
        SELECT TOP 1 markup 
        FROM dbo.default_markups 
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC
    ) dm
    WHERE 
        NOT EXISTS (
            SELECT 1
            FROM dbo.global_carrier_markups gcm
            WHERE gcm.shipping_method_id = sm.shipping_method_id
        );

    SET @MarkupsInserted = @@ROWCOUNT;

    SELECT 
        'SUCCESS' AS Status,
        @ShipmentsUpdated AS ShipmentsUpdated,
        @PackagesUpdated AS PackagesUpdated,
        @LedgerInserted AS LedgerInserted,
        @MarkupsInserted AS MarkupsInserted;

END TRY
BEGIN CATCH
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;
    
    THROW;
END CATCH;
