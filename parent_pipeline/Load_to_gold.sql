/*
================================================================================
Parent Pipeline: Load to Gold Layer (WMS Enrichment + Cost Ledger)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment
      (DEV/UAT/PROD). Scripts reference only schema.table format.

ADF Pipeline Variables Required:
  INPUT:
    - @lastrun: DATETIME2 - Last run timestamp for incremental processing
    - @carrier_id: INT - Carrier identifier (from ValidateCarrierInfo)
  
  OUTPUT:
    - Status: 'SUCCESS' or 'ERROR'
    - ShipmentsUpdated: INT - Number of shipment_wip records updated
    - PackagesUpdated: INT - Number of shipment_package_wip records updated
    - LedgerInserted: INT - Number of carrier_cost_ledger records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Three-part pipeline to enrich WMS master data with carrier billing attributes
         and populate cost ledger with itemized charges:
         1. UPDATE shipment_wip with zone and carrier information
         2. UPDATE shipment_package_wip with dimensions, weights, dates, methods, costs
         3. INSERT carrier_cost_ledger with itemized charges and matching status

Source:   billing.shipment_attributes (unified carrier billing)
billing.vw_shipment_summary (aggregated view)
billing.shipment_charges (itemized charges)

Targets:  dbo.shipment_wip (WMS master data - UPDATE)
dbo.shipment_package_wip (WMS master data - UPDATE)
dbo.carrier_cost_ledger (billing ledger - INSERT)

Idempotency: - Parts 1 & 2: UPDATEs are inherently idempotent (last update wins)
             - Part 3: INSERT with NOT EXISTS check prevents duplicates
             - All parts use @lastrun for incremental filtering
             - Safe to rerun unlimited times

Carrier-Agnostic: Works for ALL carriers (FedEx, UPS, DHL, etc.)
                  Uses only unified layer - no carrier-specific tables

Execution Order: SIXTH in pipeline (after Insert_Unified_tables.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShipmentsUpdated INT, @PackagesUpdated INT, @LedgerInserted INT;

BEGIN TRY

    /*
    ================================================================================
    Part 1: UPDATE shipment_wip (WMS Master Data Enrichment)
    ================================================================================
    Enrich WMS shipment master data with carrier billing zone and carrier information.
    
    Match Logic: WMS external_id = billing tracking_number
    Incremental: Only update records with new billing data (created_date > @lastrun)
    Idempotent: UPDATE operation - safe to run multiple times
    ================================================================================
    */

    UPDATE sw
    SET 
        sw.destination_zone = sa.destination_zone,
        sw.carrier_id = sa.carrier_id
    FROM 
dbo.shipment_wip AS sw
    JOIN 
dbo.shipment_package_wip AS spw
        ON spw.shipment_id = sw.shipment_id
    JOIN 
billing.shipment_attributes AS sa
        ON spw.tracking_number = sa.tracking_number
    WHERE 
        sa.created_date > @lastrun
        AND NULLIF(sa.tracking_number, '') IS NOT NULL;

    SET @ShipmentsUpdated = @@ROWCOUNT;

    /*
    ================================================================================
    Part 2: UPDATE shipment_package_wip (WMS Master Data Enrichment)
    ================================================================================
    Enrich WMS package master data with carrier billing dimensions, weights, dates,
    shipping method, and calculated shipping cost.
    
    Source: vw_shipment_summary (pre-calculated dimensions, weights, cost, dates)
    Match Logic: WMS tracking_number = billing tracking_number
    Incremental: Only update records with new billing data (via view)
    Idempotent: UPDATE operation - safe to run multiple times
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
dbo.shipment_package_wip AS spw
    JOIN 
billing.vw_shipment_summary AS vss
        ON spw.tracking_number = vss.tracking_number
    LEFT JOIN 
dbo.shipping_method AS sm
        ON sm.method_name = vss.shipping_method
        AND sm.carrier_id = vss.carrier_id
    WHERE 
        vss.created_date > @lastrun
        AND NULLIF(vss.tracking_number, '') IS NOT NULL;

    SET @PackagesUpdated = @@ROWCOUNT;

    /*
    ================================================================================
    Part 3: INSERT carrier_cost_ledger (Billing Data Population)
    ================================================================================
    Populate cost ledger with itemized charges from carrier billing, linking to
    WMS master data for matching status.
    
    Source: shipment_charges (itemized billing charges)
    Enrichment: charge_types (charge_category_id -> dbo.charge_type_category), 
                carrier_bill (invoice info), shipment_attributes (shipment_date), 
                WMS tables (matching), order table (customer_id)
    Match Logic: Links to WMS via shipment_package_id FK, then to order via order_id
    Incremental: Only insert new charges (created_date > @lastrun)
    Idempotent: NOT EXISTS check on (shipment_attribute_id, carrier_bill_id, charge_type_id)
                Uses existing index on shipment_attribute_id for performance
    
    Note: INNER JOIN to charge_types ensures all charges have valid charge type definitions
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
        is_matched,
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
            WHEN spw.shipment_package_id IS NOT NULL THEN 1
            ELSE 0
        END AS is_matched,
        CASE 
            -- 1. If it's a weight exception, use the type from SPW
            WHEN spw.is_weight_exception = 1 THEN spw.weight_exception_type
            -- 2. If not weight but cost exception, use the type from SPW
            WHEN spw.is_cost_exception = 1 THEN spw.cost_exception_type
            -- 3. If it's matched but weight/cost are fine
            WHEN spw.shipment_package_id IS NOT NULL THEN 'matched'
            -- 4. If it's not found in the WIP table
            ELSE 'unknown'
        END AS status
    FROM 
billing.shipment_charges AS sc
    JOIN 
billing.carrier_bill AS cb
        ON cb.carrier_bill_id = sc.carrier_bill_id
    JOIN 
dbo.charge_types AS ct
        ON ct.charge_type_id = sc.charge_type_id
    JOIN
dbo.charge_type_category AS ctc
        ON ctc.id = ct.charge_category_id
    LEFT JOIN 
billing.shipment_attributes AS sa
        ON sa.tracking_number = sc.tracking_number
        AND sa.carrier_id = sc.carrier_id
    LEFT JOIN 
dbo.shipment_package_wip AS spw
        ON spw.tracking_number = sc.tracking_number
    LEFT JOIN 
dbo.shipment_wip AS sw
        ON sw.shipment_id = spw.shipment_id
    LEFT JOIN 
dbo.[order] AS o
        ON o.order_id = sw.order_id
    WHERE 
        sc.created_date > @lastrun
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.carrier_cost_ledger AS ccl
            WHERE ccl.shipment_attribute_id = sc.shipment_attribute_id
                AND ccl.carrier_bill_id = sc.carrier_bill_id
                AND ccl.charge_type_id = sc.charge_type_id
        );

    SET @LedgerInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Part 4: UPDATE carrier_ingestion_tracker (Timestamp Tracking)
    ================================================================================
    Update last_run_time for the carrier after successful completion.
    This timestamp is used by the next run for incremental processing.
    
    Note: This assumes @carrier_id parameter is passed from parent pipeline.
          If not available, this step can be moved to a separate script or
          handled at the child pipeline level.
    
    Idempotent: MERGE operation - inserts if missing, updates if exists
    ================================================================================
    */
    
    MERGE INTO billing.carrier_ingestion_tracker AS target
    USING (
        SELECT 
            c.carrier_name,
            SYSDATETIME() AS last_run_time
        FROM dbo.carrier c
        WHERE c.carrier_id = @carrier_id
    ) AS source
    ON target.carrier_name = source.carrier_name
    WHEN MATCHED THEN
        UPDATE SET 
            last_run_time = source.last_run_time
    WHEN NOT MATCHED THEN
        INSERT (carrier_name, last_run_time)
        VALUES (source.carrier_name, source.last_run_time);

    -- Return success with row counts for ADF monitoring
    SELECT 
        'SUCCESS' AS Status,
        @ShipmentsUpdated AS ShipmentsUpdated,
        @PackagesUpdated AS PackagesUpdated,
        @LedgerInserted AS LedgerInserted;

END TRY
BEGIN CATCH
    -- Return error details for ADF to handle
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;
    
    -- Re-throw error for ADF pipeline to catch
    THROW;
END CATCH;
