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
    - ChargeTypesAdded: INT - Number of new charge types discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Idempotent synchronization of reference data tables:

         Block 1: Discovers distinct shipping methods from veho_bill.shipping_method
                  (pre-derived in Insert_ELT_&_CB.sql; NULL for ADC-only rows).

         Block 2: Discovers distinct charge types from all 3 charge slots via
                  OUTER APPLY. charge_category_id and freight flag are derived
                  from charge_code — known codes get correct categories; unknown
                  codes default to Other (11). Insert_Unified_tables.sql resolves
                  charge_type_id via INNER JOIN on charge_name + carrier_id.

Source:   billing.veho_bill + carrier_bill JOIN (file_id filtered)
Targets:  dbo.shipping_method
          dbo.charge_types

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql).
                 Must run BEFORE Insert_Unified_tables.sql so charge_type_ids
                 are available for the INNER JOIN.
Idempotent: Safe to run multiple times - NOT EXISTS prevents duplicates.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Block 1: Synchronize Shipping Methods
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
        @Carrier_id        AS carrier_id,
        vb.shipping_method AS method_name,
        'Standard'         AS service_level,
        0                  AS guaranteed_delivery,
        1                  AS is_active
    FROM
        billing.veho_bill AS vb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = vb.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND vb.shipping_method IS NOT NULL
        AND NULLIF(TRIM(vb.shipping_method), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method AS sm
            WHERE sm.method_name = vb.shipping_method
                AND sm.carrier_id = @Carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 2: Synchronize Charge Types (Dynamic Discovery)
    ================================================================================
    Unpivots 3 charge slots via OUTER APPLY and inserts any new charge names into
    dbo.charge_types. charge_category_id and freight are derived from charge_code:
      GP  (Ground Plus)             → Transportation (15), freight = 1
      DAS (Delivery Area Surcharge) → Delivery Area Surcharge (4), freight = 0
      ADC (Address Correction)      → Correction/Compliance (3), freight = 0
      *   (anything else)           → Other (11), freight = 0
    Only slots with a non-NULL, non-zero amount are considered meaningful.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        charge_category_id
    )
    SELECT DISTINCT
        @Carrier_id AS carrier_id,
        v.charge_name,
        CASE v.charge_code
            WHEN 'GP'  THEN 1
            ELSE 0
        END AS freight,
        CASE v.charge_code
            WHEN 'GP'  THEN 15  -- Transportation
            WHEN 'DAS' THEN 4   -- Delivery Area Surcharge
            WHEN 'ADC' THEN 3   -- Correction/Compliance
            ELSE 11             -- Other
        END AS charge_category_id
    FROM
        billing.veho_bill AS vb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = vb.carrier_bill_id
        OUTER APPLY (
            VALUES
                (vb.charge_name_1, vb.charge_code_1, vb.charge_amount_1),
                (vb.charge_name_2, vb.charge_code_2, vb.charge_amount_2),
                (vb.charge_name_3, vb.charge_code_3, vb.charge_amount_3)
        ) v(charge_name, charge_code, amount)
    WHERE
        cb.file_id = @File_id
        AND NULLIF(TRIM(v.charge_name), '') IS NOT NULL
        AND v.amount IS NOT NULL
        AND v.amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.charge_types ct
            WHERE ct.charge_name = v.charge_name
                AND ct.carrier_id = @Carrier_id
        );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    SELECT
        'SUCCESS'             AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded,
        @ChargeTypesAdded     AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'Veho Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'        AS Status,
        @ErrorNumber   AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine     AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
