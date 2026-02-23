/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ChargeTypesAdded: INT - Number of new variable fee charge types discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Automatically populate and maintain reference/lookup tables by
         discovering new service levels and variable fee charge types from
         processed Passport billing data.

         Block 1: Discovers shipping methods from service_level column.
         Block 2: Discovers variable fee charge types from FEE N DESCRIPTION columns
                  (FedEx pattern — description column content becomes charge_name in
                  dbo.charge_types, enabling Insert_Unified_tables.sql to resolve
                  charge_type_id via INNER JOIN).

Source:  billing.passport_bill + carrier_bill JOIN (file_id filtered)
Targets: dbo.shipping_method
         dbo.charge_types (variable fee descriptions only; fixed charges seeded
         once by Insert_Charge_Types.sql)

File-Based Filtering: Joins carrier_bill and filters by file_id to process
         only the current file's data.

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 Must run BEFORE Insert_Unified_tables.sql so charge_type_ids
                 for variable fees are available.
Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates.
No Transaction: Each INSERT is independently idempotent via unique constraints.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Block 1: Synchronize Shipping Methods
    ================================================================================
    Discovers distinct service levels from passport_bill and inserts any new
    methods into the shipping_method table.

    Populates with sensible defaults:
    - carrier_id:            From @Carrier_id parameter
    - method_name:           service_level from passport_bill (e.g., "Priority DDP Delcon")
    - service_level:         Default 'Standard'
    - guaranteed_delivery:   Default 0 (false)
    - is_active:             Default 1 (true)

    File-Based Filtering: Joins carrier_bill and filters by file_id.
    ================================================================================
    */

    INSERT INTO dbo.shipping_method (
        carrier_id,
        method_name,
        service_level,
        guaranteed_delivery,
        is_active
    )
    SELECT DISTINCT
        @Carrier_id         AS carrier_id,
        p.service_level     AS method_name,
        'Standard'          AS service_level,
        0                   AS guaranteed_delivery,
        1                   AS is_active
    FROM billing.passport_bill p
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = p.carrier_bill_id
    WHERE
        cb.file_id = @File_id  -- FILE-BASED FILTERING
        AND p.service_level IS NOT NULL
        AND NULLIF(TRIM(p.service_level), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method sm
            WHERE sm.method_name = p.service_level
                AND sm.carrier_id = @Carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 2: Synchronize Variable Fee Charge Types (FedEx Pattern)
    ================================================================================
    Discovers distinct non-empty FEE N DESCRIPTION values from passport_bill and
    inserts any new ones into dbo.charge_types.

    This is the FedEx-style dynamic charge type discovery: instead of pre-seeding
    "Additional Fees" as a single bucket, each unique fee description found in the
    data becomes its own charge type. Insert_Unified_tables.sql can then resolve
    each fee individually via INNER JOIN on charge_name.

    Only descriptions associated with a non-zero amount are considered meaningful.

    Populates with sensible defaults:
    - carrier_id:         From @Carrier_id parameter
    - charge_name:        FEE N DESCRIPTION value (e.g., "Address Correction Fee")
    - freight:            0 (variable fees are ancillary, not base freight)
    - charge_category_id: 11 (Other)

    File-Based Filtering: Joins carrier_bill and filters by file_id.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        charge_category_id
    )
    SELECT DISTINCT
        @Carrier_id     AS carrier_id,
        v.fee_desc      AS charge_name,
        0               AS freight,
        11              AS charge_category_id
    FROM billing.passport_bill p
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = p.carrier_bill_id
    OUTER APPLY (
        VALUES
            (p.fee_1_description, p.fee_1_amount),
            (p.fee_2_description, p.fee_2_amount),
            (p.fee_3_description, p.fee_3_amount),
            (p.fee_4_description, p.fee_4_amount),
            (p.fee_5_description, p.fee_5_amount),
            (p.fee_6_description, p.fee_6_amount),
            (p.fee_7_description, p.fee_7_amount),
            (p.fee_8_description, p.fee_8_amount)
    ) v(fee_desc, fee_amount)
    WHERE
        cb.file_id = @File_id  -- FILE-BASED FILTERING
        AND NULLIF(TRIM(v.fee_desc), '') IS NOT NULL
        AND v.fee_amount IS NOT NULL
        AND v.fee_amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.charge_types ct
            WHERE ct.charge_name = v.fee_desc
                AND ct.carrier_id = @Carrier_id
        );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Return Results
    ================================================================================
    */
    SELECT
        'SUCCESS'               AS Status,
        @ShippingMethodsAdded   AS ShippingMethodsAdded,
        @ChargeTypesAdded       AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    /*
    ================================================================================
    Error Handling: Return Detailed Error Information
    ================================================================================
    Note: No transaction to rollback - each INSERT is independently idempotent
    ================================================================================
    */
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Passport] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (each INSERT independently idempotent)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status, ShippingMethodsAdded, ChargeTypesAdded
✅ #11 - Charge categories: All new fee types → Other (11)
✅ #12 - File-based filtering: joins carrier_bill, filters by file_id

Note: Fixed charge types (Rate, Fuel Surcharge, Tax, Duty, Insurance, Clearance Fee)
      are seeded once via Insert_Charge_Types.sql during initial carrier setup.
      Block 2 here dynamically discovers only variable fee descriptions.
================================================================================
*/
