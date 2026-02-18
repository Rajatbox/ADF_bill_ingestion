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
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShipmentsUpdated INT, @PackagesUpdated INT, @LedgerInserted INT;

-- Pre-filter shipment_attribute_ids for current file (reused across all 3 operations)
DECLARE @FileShipments TABLE (
    shipment_attribute_id INT PRIMARY KEY
);

INSERT INTO @FileShipments (shipment_attribute_id)
SELECT DISTINCT sc.shipment_attribute_id
FROM billing.shipment_charges sc
JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
WHERE cb.file_id = @File_id
  AND sc.carrier_id = @Carrier_id;

BEGIN TRY

    /*
    ================================================================================
    Part 1: Update shipment with zone and carrier info
    ================================================================================
    */
    UPDATE sw
    SET 
        sw.destination_zone = sa.destination_zone,
        sw.carrier_id = sa.carrier_id
    FROM 
        dbo.shipment AS sw
    JOIN 
        dbo.shipment_package AS spw
        ON spw.shipment_id = sw.shipment_id
    JOIN 
        billing.shipment_attributes AS sa
        ON spw.tracking_number = sa.tracking_number
    JOIN
        @FileShipments fs
        ON fs.shipment_attribute_id = sa.id
    WHERE 
        NULLIF(sa.tracking_number, '') IS NOT NULL;

    SET @ShipmentsUpdated = @@ROWCOUNT;

    /*
    ================================================================================
    Part 2: Update shipment_package with dimensions, weights, dates, costs
    ================================================================================
    */
    UPDATE spw
    SET 
        spw.carrier_pickup_date = vss.shipment_date,
        spw.shipping_method_id = sm.shipping_method_id,
        spw.billed_weight_oz = vss.billed_weight_oz,
        spw.billed_length_in = vss.billed_length_in,
        spw.billed_width_in = vss.billed_width_in,
        spw.billed_height_in = vss.billed_height_in,
        spw.billed_shipping_cost = vss.billed_shipping_cost
    FROM 
        dbo.shipment_package AS spw
    JOIN 
        billing.vw_shipment_summary AS vss
        ON spw.tracking_number = vss.tracking_number
    JOIN
        @FileShipments fs
        ON fs.shipment_attribute_id = vss.id
    LEFT JOIN 
        dbo.shipping_method AS sm
        ON sm.method_name = vss.shipping_method
        AND sm.carrier_id = vss.carrier_id
    WHERE 
        NULLIF(vss.tracking_number, '') IS NOT NULL;

    SET @PackagesUpdated = @@ROWCOUNT;

    /*
    ================================================================================
    Part 3: Insert itemized charges into cost ledger with matching status
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
        sc.tracking_number,
        sa.shipment_date,
        sw.external_id AS shipment_external_id,
        o.[3pl_customer_id] AS customer_id,
        sc.carrier_id,
        spw.shipping_method_id,
        ctc.category AS category,  -- Category name from dbo.charge_type_category
        ct.charge_name AS cost_item,
        sc.amount,
        sc.charge_type_id,
        spw.shipment_package_id,
        sc.carrier_bill_id,
        sc.shipment_attribute_id,
        CASE 
            -- 1. If it's a weight exception, use the type from SPW
            WHEN spw.is_weight_exception = 1 THEN spw.weight_exception_type
            -- 2. If not weight but cost exception, use the type from SPW
            WHEN spw.is_cost_exception = 1 THEN spw.cost_exception_type
            -- 3. If it's matched but weight/cost are fine
            WHEN spw.shipment_package_id IS NOT NULL THEN 'matched'
            -- 4. If it's not found in the shipment_package table
            ELSE 'unknown'
        END AS status
    FROM 
        billing.shipment_charges AS sc
    JOIN 
        billing.carrier_bill AS cb
        ON cb.carrier_bill_id = sc.carrier_bill_id
        AND cb.file_id = @File_id
    JOIN 
        dbo.charge_types AS ct
        ON ct.charge_type_id = sc.charge_type_id
    JOIN
        dbo.charge_type_category AS ctc
        ON ctc.category_id = ct.charge_category_id
    LEFT JOIN 
        billing.shipment_attributes AS sa
        ON sa.tracking_number = sc.tracking_number
        AND sa.carrier_id = sc.carrier_id
    LEFT JOIN 
        dbo.shipment_package AS spw
        ON spw.tracking_number = sc.tracking_number
    LEFT JOIN 
        dbo.shipment AS sw
        ON sw.shipment_id = spw.shipment_id
    LEFT JOIN 
        dbo.[order] AS o
        ON o.order_id = sw.order_id
    WHERE 
        NOT EXISTS (
            SELECT 1
            FROM dbo.carrier_cost_ledger AS ccl
            WHERE ccl.shipment_attribute_id = sc.shipment_attribute_id
                AND ccl.carrier_bill_id = sc.carrier_bill_id
                AND ccl.charge_type_id = sc.charge_type_id
        );

    SET @LedgerInserted = @@ROWCOUNT;

    SELECT 
        'SUCCESS' AS Status,
        @ShipmentsUpdated AS ShipmentsUpdated,
        @PackagesUpdated AS PackagesUpdated,
        @LedgerInserted AS LedgerInserted;

END TRY
BEGIN CATCH
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;
    
    THROW;
END CATCH;
