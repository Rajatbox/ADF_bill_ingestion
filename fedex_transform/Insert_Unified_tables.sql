/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges (MPS Logic)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
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

File-Based Filtering: Uses @File_id to process only the current file's data:
         - Filters fedex_bill via carrier_bill JOIN on file_id
         - Filters vw_FedExCharges using file_id column
         - Enables cross-carrier parallel processing (different files simultaneously)
         - Supports reliable retry of failed files without reprocessing completed data

Sources:  billing.fedex_bill + carrier_bill JOIN (file_id filtered)
          billing.vw_FedExCharges (includes file_id for filtering)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost)

Idempotency: - Part 1: MERGE upserts shipment_attributes (INSERT new, UPDATE existing with COALESCE)
             - Part 2: NOT EXISTS check prevents duplicate charges
             - Safe to rerun with same @File_id

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
    PART 1: MERGE Shipment Attributes with MPS Logic
    ================================================================================
    Four-stage CTE pipeline:
    1. fx_tallied: Count occurrences of each (invoice_number, express_or_ground_tracking_id)
    2. fx_classified: Classify each row into MPS roles
    3. fx_hoisted: Hoist header values to all rows in MPS groups
    4. fx_final: Merge cross-invoice duplicates (GROUP BY + MAX), convert units, filter MPS_HEADERs
    
    MERGE Operation: Upsert shipment attributes
    - NOT MATCHED: Insert new tracking numbers
    - MATCHED: Update existing rows with COALESCE (correction invoices fill in
      missing attributes like dims/zone without wiping existing non-null values)
    - Handles corrections arriving in separate files from the original invoice
    
    Note: billed_shipping_cost is NOT stored in this table. It's calculated on-the-fly
    via vw_shipment_summary view from shipment_charges table (single source of truth).
    ================================================================================
    */

    WITH fx_tallied AS (
        -- Stage 1: Count occurrences to identify MPS groups
        SELECT 
            f.*,
            COUNT(*) OVER (PARTITION BY f.invoice_number, f.express_or_ground_tracking_id) as ground_id_count
        FROM billing.fedex_bill f
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = f.carrier_bill_id
        WHERE cb.file_id = @File_id  -- File-based filtering
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
            invoice_number,
            express_or_ground_tracking_id as group_id,
            dim_length, dim_width, dim_height, dim_unit,
            rated_weight_units, rated_weight_amount,
            
            -- Hoist net_charge_amount from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN net_charge_amount END) 
                OVER (PARTITION BY invoice_number, express_or_ground_tracking_id) AS enriched_net_charge,
            
            -- Hoist shipment_date from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN shipment_date END) 
                OVER (PARTITION BY invoice_number, express_or_ground_tracking_id) AS enriched_shipment_date,
            
            -- Hoist service_type from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN service_type END) 
                OVER (PARTITION BY invoice_number, express_or_ground_tracking_id) AS enriched_service_type,
            
            -- Hoist zone_code from header/normal row to entire group
            MAX(CASE WHEN mps_role IN ('MPS_HEADER', 'NORMAL_SINGLE') THEN zone_code END) 
                OVER (PARTITION BY invoice_number, express_or_ground_tracking_id) AS enriched_zone_code
        FROM fx_classified
    ),
    fx_final AS (
        -- Stage 4: Merge cross-invoice duplicates, convert units, filter MPS_HEADERs
        -- Same tracking_number can appear in multiple invoices (e.g., original + correction).
        -- GROUP BY merges them, MAX picks the best non-null value for each attribute.
        SELECT 
            @Carrier_id AS carrier_id,
            tracking_number,
            MAX(enriched_shipment_date) AS shipment_date,
            MAX(enriched_service_type) AS shipping_method,
            MAX(enriched_zone_code) AS destination_zone,
            
            -- Weight conversion: Handle multiple unit variants to OZ
            MAX(CASE 
                WHEN UPPER(rated_weight_units) IN ('L', 'LB', 'LBS', 'P') 
                    THEN rated_weight_amount * 16.0  -- pounds to ounces
                WHEN UPPER(rated_weight_units) IN ('K', 'KG', 'KGS') 
                    THEN rated_weight_amount * 35.27396195  -- kilograms to ounces
                WHEN rated_weight_units IS NULL 
                    THEN rated_weight_amount  -- unknown/blank -> assume already oz
                ELSE rated_weight_amount  -- default: assume already oz
            END) AS billed_weight_oz,
            
            -- Dimension conversions: Handle unit variants to inches
            MAX(CASE 
                WHEN UPPER(dim_unit) = 'C' THEN dim_length / 2.54  -- cm → in
                WHEN UPPER(dim_unit) = 'I' THEN dim_length  -- already in inches
                WHEN dim_unit IS NULL THEN dim_length  -- assume already inches
                ELSE dim_length  -- default: assume already inches
            END) AS billed_length_in,
            MAX(CASE 
                WHEN UPPER(dim_unit) = 'C' THEN dim_width / 2.54  -- cm → in
                WHEN UPPER(dim_unit) = 'I' THEN dim_width  -- already in inches
                WHEN dim_unit IS NULL THEN dim_width  -- assume already inches
                ELSE dim_width  -- default: assume already inches
            END) AS billed_width_in,
            MAX(CASE 
                WHEN UPPER(dim_unit) = 'C' THEN dim_height / 2.54  -- cm → in
                WHEN UPPER(dim_unit) = 'I' THEN dim_height  -- already in inches
                WHEN dim_unit IS NULL THEN dim_height  -- assume already inches
                ELSE dim_height  -- default: assume already inches
            END) AS billed_height_in
        FROM fx_hoisted
        WHERE mps_role <> 'MPS_HEADER'
        GROUP BY tracking_number
    )
    MERGE billing.shipment_attributes AS target
    USING fx_final AS source
    ON target.carrier_id = source.carrier_id
       AND target.tracking_number = source.tracking_number
    WHEN MATCHED THEN
        UPDATE SET
            shipment_date      = COALESCE(source.shipment_date, target.shipment_date),
            shipping_method    = COALESCE(source.shipping_method, target.shipping_method),
            destination_zone   = COALESCE(source.destination_zone, target.destination_zone),
            billed_weight_oz   = COALESCE(source.billed_weight_oz, target.billed_weight_oz),
            billed_length_in   = COALESCE(source.billed_length_in, target.billed_length_in),
            billed_width_in    = COALESCE(source.billed_width_in, target.billed_width_in),
            billed_height_in   = COALESCE(source.billed_height_in, target.billed_height_in)
    WHEN NOT MATCHED THEN
        INSERT (
            carrier_id, shipment_date, shipping_method, destination_zone,
            tracking_number, billed_weight_oz, billed_length_in, billed_width_in, billed_height_in
        )
        VALUES (
            source.carrier_id, source.shipment_date, source.shipping_method, source.destination_zone,
            source.tracking_number, source.billed_weight_oz, source.billed_length_in,
            source.billed_width_in, source.billed_height_in
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
            AND sa.tracking_number = ISNULL(v.express_or_ground_tracking_id, 'Service_charges')
        WHERE 
            v.file_id = @File_id  -- File-based filtering
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

