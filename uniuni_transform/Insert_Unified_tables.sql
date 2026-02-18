/*
================================================================================
Insert Script: Unified Tables (Shipment Attributes & Charges)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - AttributesInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Transform carrier-specific data into unified analytical schema:
         1. Insert physical shipment attributes with unit conversions:
            - Weight: dim_weight (LBS × 16 → OZ, OZS → OZ)
            - Dimensions: CM × 0.393701 → IN, IN → IN (no conversion)
         2. Unpivot 18 charge columns and insert into shipment_charges

Source:   billing.uniuni_bill (carrier-specific line items)
Targets:  billing.shipment_attributes (unified physical data - NO cost stored)
          billing.shipment_charges (unified charge data)
Joins:    dbo.charge_types (charge_type_id lookup)
          billing.carrier_bill (carrier_bill_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost on-the-fly)

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint on (carrier_id, tracking_number)
             - Part 2: NOT EXISTS check on (shipment_attribute_id, carrier_bill_id, charge_type_id)
             - Safe to rerun with same @lastrun
Transaction: NO TRANSACTION (each insert is independently idempotent)
Business Key: (carrier_id, tracking_number) - enforced by UNIQUE INDEX

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Shipment Attributes with Unit Conversions
    ================================================================================
    Transforms carrier-specific physical shipment data into unified schema.
    Converts all weight to OZ and all dimensions to IN per design constraints.
    
    Unit Conversions (matching reference stored procedure logic):
    - Weight: Uses dim_weight (LBS × 16 → OZ, OZS → OZ)
    - Dimensions: CM × 0.393701 → IN, IN → IN (no conversion)
    
    Incremental Processing: Filters by created_date > @lastrun to only process
    new records since last successful run.
    ================================================================================
    */

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        tracking_number,
        shipment_date,
        shipping_method,
        destination_zone,
        billed_weight_oz,
        billed_length_in,
        billed_width_in,
        billed_height_in
    )
    SELECT
        @Carrier_id AS carrier_id,
        ub.tracking_number,
        ub.shipment_date AS shipment_date,
        ub.service_type AS shipping_method,
        CAST(ub.[zone] AS VARCHAR(255)) AS destination_zone,
        
        -- Weight conversion to OZ (from dim_weight per reference stored procedure)
        CASE 
            WHEN UPPER(TRIM(ub.dim_weight_uom)) = 'LBS' THEN ub.dim_weight * 16.0
            WHEN UPPER(TRIM(ub.dim_weight_uom)) = 'OZS' THEN ub.dim_weight
            WHEN ub.dim_weight_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_weight_oz,
        
        -- Dimension conversions to IN (matching reference stored procedure logic)
        CASE 
            WHEN UPPER(TRIM(ub.package_dim_uom)) = 'CM' THEN ub.dim_length * 0.393701
            WHEN UPPER(TRIM(ub.package_dim_uom)) = 'IN' THEN ub.dim_length
            WHEN ub.package_dim_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_length_in,
        
        CASE 
            WHEN UPPER(TRIM(ub.package_dim_uom)) = 'CM' THEN ub.dim_width * 0.393701
            WHEN UPPER(TRIM(ub.package_dim_uom)) = 'IN' THEN ub.dim_width
            WHEN ub.package_dim_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_width_in,
        
        CASE 
            WHEN UPPER(TRIM(ub.package_dim_uom)) = 'CM' THEN ub.dim_height * 0.393701
            WHEN UPPER(TRIM(ub.package_dim_uom)) = 'IN' THEN ub.dim_height
            WHEN ub.package_dim_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_height_in
        
    FROM
        billing.uniuni_bill AS ub
    WHERE
        ub.created_date > @lastrun    -- Incremental filter: only new records
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes AS sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = ub.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Shipment Charges (Unpivot Wide Format)
    ================================================================================
    Unpivots 18 charge columns from uniuni_bill into normalized shipment_charges.
    Uses CROSS APPLY with VALUES to transform wide format into narrow format.
    Only inserts charges with amount > 0.
    
    Charge Types (matching Sync_Reference_Data.sql):
    - Base Rate, Discount Fee, Discount Percentage, Signature Fee, Pick Up Fee
    - Over Dimension Fee, Over Max Size Fee, Over Weight Fee, Fuel Surcharge
    - Peak Season Surcharge, Delivery Area Surcharge, Delivery Area Surcharge Extend
    - Truck Fee, Relabel Fee, Miscellaneous Fee, Credit Card Surcharge
    - Credit, Approved Claim
    
    Incremental Processing: Filters by created_date > @lastrun to only process
    new records since last successful run.
    ================================================================================
    */

    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        cb.carrier_bill_id,
        ub.tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.uniuni_bill AS ub
        
        -- Unpivot charge columns using CROSS APPLY
        CROSS APPLY (
            VALUES 
                ('Base Rate', ub.base_fee),
                ('Discount Fee', ub.discount_fee),
                ('Signature Fee', ub.signature_fee),
                ('Pick Up Fee', ub.pickup_fee),
                ('Over Dimension Fee', ub.over_dimension_fee),
                ('Over Max Size Fee', ub.over_max_size_fee),
                ('Over Weight Fee', ub.over_weight_fee),
                ('Fuel Surcharge', ub.fuel_surcharge),
                ('Peak Season Surcharge', ub.peak_season_surcharge),
                ('Delivery Area Surcharge', ub.delivery_area_surcharge),
                ('Delivery Area Surcharge Extend', ub.delivery_area_surcharge_extend),
                ('Truck Fee', ub.truck_fee),
                ('Relabel Fee', ub.relabel_fee),
                ('Miscellaneous Fee', ub.miscellaneous_fee),
                ('Credit Card Surcharge', ub.credit_card_surcharge),
                ('Credit', ub.credit),
                ('Approved Claim', ub.approved_claim)
        ) AS charges(charge_name, amount)
        
        -- Join to get charge_type_id
        INNER JOIN dbo.charge_types AS ct 
            ON ct.charge_name = charges.charge_name 
            AND ct.carrier_id = @Carrier_id
        
        -- Join to get carrier_bill_id
        INNER JOIN billing.carrier_bill AS cb
            ON cb.bill_number = CAST(ub.invoice_number AS VARCHAR(100))
            AND cb.bill_date = ub.invoice_time
            AND cb.carrier_id = @Carrier_id
        
        -- Join to get shipment_attribute_id
        INNER JOIN billing.shipment_attributes AS sa
            ON sa.tracking_number = ub.tracking_number
            AND sa.carrier_id = @Carrier_id
    WHERE
        ub.created_date > @lastrun    -- Incremental filter: only new records      -- amount can be negative 
        AND charges.amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges AS sc
            WHERE sc.shipment_attribute_id = sa.id
                AND sc.carrier_bill_id = cb.carrier_bill_id
                AND sc.charge_type_id = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success results
    SELECT 
        'SUCCESS' AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted AS ChargesInserted;

END TRY
BEGIN CATCH
    -- Build descriptive error message
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'UniUni Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    -- Return error details (no rollback needed - no transaction)
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;