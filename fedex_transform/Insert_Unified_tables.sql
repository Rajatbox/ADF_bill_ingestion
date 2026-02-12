/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges (MPS Logic)
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

Purpose: Two-part idempotent population script with Multi-Piece Shipment (MPS) handling:
         PART 1: INSERT shipment_attributes with MPS classification and hoisting logic
         PART 2: INSERT shipment_charges with FK reference to shipment_attributes
         
         MPS Logic classifies shipments into roles:
         - NORMAL_SINGLE: Single package shipments (count = 1, msp_tracking_id = NULL)
         - MPS_HEADER: Header row for MPS groups (count > 1, msp_tracking_id = NULL)
                       Contains aggregated totals and metadata, filtered out from final insert
         - MPS_PARENT: Parent package in MPS group (count > 1, msp_tracking_id = express_or_ground_id)
         - MPS_CHILD: Child packages in MPS group (count > 1, mps_tracking_id ≠ express_or_ground_id)
         
         Hoisting: Header row values (shipment_date, service_type, zone_code, aggregated dimensions) 
         are propagated to all packages in the MPS group via window functions.
         
         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary view from shipment_charges
         (single source of truth). This eliminates sync issues and ensures correctness.

Sources:  billing.fedex_bill (for attributes with MPS logic)
billing.vw_FedExCharges (for charges)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost)

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check prevents duplicate charges
             - Both parts use same pattern: INSERT ... WHERE NOT EXISTS
             - Safe to rerun with same @lastrun

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.

Business Key: shipment_attributes.id (IDENTITY) represents unique carrier_id + tracking_number
              One shipment_attributes row can have many shipment_charges rows (1-to-Many)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    PART 1: INSERT Shipment Attributes with MPS Logic
    ================================================================================
    Four-stage CTE pipeline:
    1. fx_tallied: Count occurrences of each express_or_ground_tracking_id
    2. fx_classified: Classify each row into MPS roles
    3. fx_hoisted: Hoist header values to all rows in MPS groups
    4. fx_final: Apply transformations and filter out MPS_HEADER rows
    
    INSERT Operation: Insert new shipments only (NOT EXISTS prevents duplicates)
    - UNIQUE constraint on (carrier_id, tracking_number) provides additional safety
    - Only inserts new tracking numbers (existing ones skipped)
    - No updates needed (core attributes never change after creation)
    
    Note: billed_shipping_cost is NOT stored in this table. It's calculated on-the-fly
    via vw_shipment_summary view from shipment_charges table (single source of truth).
    ================================================================================
    */

    WITH fx_tallied AS (
        -- Stage 1: Count occurrences to identify MPS groups
        SELECT 
            f.*,
            COUNT(*) OVER (PARTITION BY f.express_or_ground_tracking_id) as ground_id_count
        FROM billing.fedex_bill f
        WHERE f.created_date > @lastrun
          AND f.carrier_bill_id IS NOT NULL
    ),
    fx_classified AS (
        -- Stage 2: Classify rows by MPS role
        SELECT 
            *,
            CASE 
                WHEN ground_id_count = 1 THEN 'NORMAL_SINGLE'
                WHEN ground_id_count > 1 AND NULLIF(msp_tracking_id, '') IS NULL THEN 'MPS_HEADER'
                WHEN ground_id_count > 1 AND msp_tracking_id = express_or_ground_tracking_id THEN 'MPS_PARENT'
                WHEN ground_id_count > 1 AND msp_tracking_id <> express_or_ground_tracking_id THEN 'MPS_CHILD'
                ELSE 'UNKNOWN'
            END AS mps_role
        FROM fx_tallied
    ),
    fx_hoisted AS (
        -- Stage 3: Hoist header values to all rows in MPS group
        SELECT 
            COALESCE(NULLIF(msp_tracking_id, ''), express_or_ground_tracking_id) AS tracking_number,
            mps_role,
            express_or_ground_tracking_id as group_id,
            dim_length, dim_width, dim_height, dim_unit,
            rated_weight_units, rated_weight_amount,
            
            -- Hoist net_charge_amount from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN net_charge_amount END) 
                OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_net_charge,
            
            -- Hoist shipment_date from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN shipment_date END) 
                OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_shipment_date,
            
            -- Hoist service_type from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN service_type END) 
                OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_service_type,
            
            -- Hoist zone_code from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN zone_code END) 
                OVER (PARTITION BY express_or_ground_tracking_id) AS enriched_zone_code
        FROM fx_classified
    ),
    fx_final AS (
        -- Stage 4: Apply transformations and prepare for MERGE
        SELECT 
            @Carrier_id AS carrier_id,
            enriched_shipment_date AS shipment_date,
            enriched_service_type AS shipping_method,
            enriched_zone_code AS destination_zone,
            tracking_number,
            
            -- Weight conversion: Handle multiple unit variants to OZ
            CASE 
                WHEN UPPER(rated_weight_units) IN ('L', 'LB', 'LBS', 'P') 
                    THEN rated_weight_amount * 16.0  -- pounds to ounces
                WHEN UPPER(rated_weight_units) IN ('K', 'KG', 'KGS') 
                    THEN rated_weight_amount * 35.27396195  -- kilograms to ounces
                WHEN rated_weight_units IS NULL 
                    THEN rated_weight_amount  -- unknown/blank -> assume already oz
                ELSE rated_weight_amount  -- default: assume already oz
            END AS billed_weight_oz,
            
            -- Dimension conversions: Handle unit variants to inches
            CASE 
                WHEN UPPER(dim_unit) = 'C' THEN dim_length / 2.54  -- cm → in
                WHEN UPPER(dim_unit) = 'I' THEN dim_length  -- already in inches
                WHEN dim_unit IS NULL THEN dim_length  -- assume already inches
                ELSE dim_length  -- default: assume already inches
            END AS billed_length_in,
            CASE 
                WHEN UPPER(dim_unit) = 'C' THEN dim_width / 2.54  -- cm → in
                WHEN UPPER(dim_unit) = 'I' THEN dim_width  -- already in inches
                WHEN dim_unit IS NULL THEN dim_width  -- assume already inches
                ELSE dim_width  -- default: assume already inches
            END AS billed_width_in,
            CASE 
                WHEN UPPER(dim_unit) = 'C' THEN dim_height / 2.54  -- cm → in
                WHEN UPPER(dim_unit) = 'I' THEN dim_height  -- already in inches
                WHEN dim_unit IS NULL THEN dim_height  -- assume already inches
                ELSE dim_height  -- default: assume already inches
            END AS billed_height_in
        FROM fx_hoisted
        WHERE mps_role <> 'MPS_HEADER'  -- Filter out header rows
    )
    INSERT INTO billing.shipment_attributes (
        carrier_id,
        shipment_date,
        shipping_method,
        destination_zone,
        tracking_number,
        billed_weight_oz,
        billed_length_in,
        billed_width_in,
        billed_height_in
    )
    SELECT 
        carrier_id,
        shipment_date,
        shipping_method,
        destination_zone,
        tracking_number,
        billed_weight_oz,
        billed_length_in,
        billed_width_in,
        billed_height_in
    FROM fx_final
    WHERE NOT EXISTS (
        SELECT 1 
        FROM billing.shipment_attributes sa
        WHERE sa.carrier_id = fx_final.carrier_id
          AND sa.tracking_number = fx_final.tracking_number
    );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: Insert Shipment Charges with FK Reference
    ================================================================================
    Populates shipment_charges from unpivoted charge data with:
    - Charge type mapping via charge_types lookup
    - Foreign key reference to shipment_attributes.id for business key linkage
    
    The shipment_attribute_id establishes the 1-to-Many relationship:
    - One shipment_attributes row (carrier_id + tracking_number)
    - Many shipment_charges rows (different charge types for same shipment)
    
    Idempotency: NOT EXISTS check on composite key prevents duplicates
    ================================================================================
    */

    WITH charge_source AS (
        SELECT
            @Carrier_id AS carrier_id,
            v.carrier_bill_id,
            v.express_or_ground_tracking_id AS tracking_number,
            ct.charge_type_id,
            v.charge_amount AS amount,
            sa.id AS shipment_attribute_id  -- FK lookup to establish relationship
        FROM 
billing.vw_FedExCharges v
        INNER JOIN 
dbo.charge_types ct
            ON ct.charge_name = v.charge_type
            AND ct.carrier_id = @Carrier_id
        INNER JOIN
billing.shipment_attributes sa
            ON sa.carrier_id = @Carrier_id
            AND sa.tracking_number = v.express_or_ground_tracking_id
        WHERE 
            v.created_date > @lastrun
    )
    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    )
    SELECT
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    FROM charge_source
    WHERE NOT EXISTS (
        SELECT 1 
        FROM billing.shipment_charges sc
        WHERE sc.shipment_attribute_id = charge_source.shipment_attribute_id
          AND sc.carrier_bill_id = charge_source.carrier_bill_id
          AND sc.charge_type_id = charge_source.charge_type_id
    );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success with row counts for ADF monitoring
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
        'FedEx Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    -- Return error details for ADF to handle
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;

