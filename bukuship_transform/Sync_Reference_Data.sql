/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
                        (Bukuship — the aggregator carrier)
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - IntegratedCarriersAdded: INT - New integrated carriers auto-discovered
    - ShippingMethodsAdded: INT - New shipping methods discovered
    - ChargeTypesAdded: INT - New charge types discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Carrier Model:
         Bukuship is the aggregator (is_aggregator = 1 in dbo.carrier).
         Each row in bukuship_bill carries a carrier_name column identifying
         the integrated carrier that physically fulfilled the shipment:
           - "Landmark Global"  → fulfilled directly by Landmark Global
           - "DHL eCommerce"    → fulfilled by DHL, injected via Bukuship/Landmark

         @Carrier_id always refers to the Bukuship aggregator entry.
         Integrated carriers discovered in Block 0 are inserted with
         is_aggregator = 0 and linked via integrated_carrier_id in Block 1.

Purpose: Automatically populate and maintain reference/lookup tables by
         discovering new values from processed Bukuship billing data.

         Block 0: Auto-discover integrated carriers (carrier_name column) into dbo.carrier
                  e.g. "Landmark Global", "DHL eCommerce" → inserted as is_aggregator = 0
         Block 1: Sync shipping methods from service_name column, keyed by
                  (carrier_id = Bukuship, method_name, integrated_carrier_id)
         Block 2: Sync charge types from charge_name column, keyed by
                  (carrier_id = Bukuship, charge_name)

Source:  billing.bukuship_bill + carrier_bill JOIN (file_id filtered)
Targets: dbo.carrier (integrated carriers — Block 0)
         dbo.shipping_method (service methods per integrated carrier — Block 1)
         dbo.charge_types (charge names scoped to Bukuship carrier_id — Block 2)

File-Based Filtering: Joins carrier_bill to filter by @File_id

Idempotent: Safe to run multiple times — all blocks use NOT EXISTS
No Transaction: Each INSERT is independently idempotent

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @IntegratedCarriersAdded INT, @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Block 0: Auto-Discover Integrated Carriers
    ================================================================================
    Bukuship is the aggregator — carrier_name holds the actual fulfillment carrier
    (e.g., "Landmark Global", "DHL eCommerce"). Insert any new carrier names into
    dbo.carrier with is_aggregator = 0 so that integrated_carrier_id FK resolves
    in Block 1 and downstream unified scripts.

    Case-insensitive NOT EXISTS to avoid "DHL eCommerce" vs "dhl ecommerce" dupes.
    ================================================================================
    */

    INSERT INTO dbo.carrier (carrier_name, is_active, is_aggregator)
    SELECT DISTINCT
        bb.carrier_name,
        1 AS is_active,
        0 AS is_aggregator
    FROM billing.bukuship_bill bb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = bb.carrier_bill_id
    WHERE cb.file_id = @File_id
      AND NULLIF(TRIM(bb.carrier_name), '') IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 FROM dbo.carrier c
            WHERE LOWER(c.carrier_name) = LOWER(bb.carrier_name)
      );

    SET @IntegratedCarriersAdded = @@ROWCOUNT;
    

    /*
    ================================================================================
    Block 1: Synchronize Shipping Methods
    ================================================================================
    Discovers distinct (service_name, carrier_name) combinations from bukuship_bill
    and inserts new entries into dbo.shipping_method.

    Key:  (carrier_id = Bukuship aggregator carrier_id, method_name = service_name,
           integrated_carrier_id = resolved from carrier_name via dbo.carrier)

    service_name examples: "DHL SmartMail Parcel Plus Expedited",
                           "Landmark Intl Standard"
    ================================================================================
    */

    INSERT INTO dbo.shipping_method (
        carrier_id,
        method_name,
        service_level,
        guaranteed_delivery,
        is_active,
        integrated_carrier_id
    )
    SELECT DISTINCT
        @Carrier_id                AS carrier_id,
        bb.service_name            AS method_name,
        'Standard'                 AS service_level,
        0                          AS guaranteed_delivery,
        1                          AS is_active,
        c.carrier_id               AS integrated_carrier_id
    FROM billing.bukuship_bill bb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = bb.carrier_bill_id
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(bb.carrier_name)
    WHERE cb.file_id = @File_id
      AND NULLIF(TRIM(bb.service_name), '') IS NOT NULL
      AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method sm
            WHERE sm.carrier_id = @Carrier_id
              AND sm.method_name = bb.service_name
              AND (
                    (sm.integrated_carrier_id IS NULL AND c.carrier_id IS NULL)
                    OR sm.integrated_carrier_id = c.carrier_id
                  )
      );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 2: Synchronize Charge Types
    ================================================================================
    Discovers distinct charge_name values from bukuship_bill and inserts new entries
    into dbo.charge_types, scoped to the Bukuship aggregator carrier_id.

    charge_name examples: "Freight Charge", "Fuel Surcharge", "Fuel", "Broker Fee"

    Category: All charges → 'Other' (charge_category_id = 11)
    Freight flag: 1 for "Freight Charge", 0 for all accessorial charges
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
        bb.charge_name,
        CASE
            WHEN LOWER(bb.charge_name) = 'freight charge' THEN 1
            ELSE 0
        END AS freight,
        11  AS charge_category_id   -- Other (CTC = 11)
    FROM billing.bukuship_bill bb
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = bb.carrier_bill_id
    WHERE cb.file_id = @File_id
      AND NULLIF(TRIM(bb.charge_name), '') IS NOT NULL
      AND NOT EXISTS (
            SELECT 1
            FROM dbo.charge_types ct
            WHERE ct.charge_name = bb.charge_name
              AND ct.carrier_id  = @Carrier_id
      );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    SELECT
        'SUCCESS'                AS [Status],
        @IntegratedCarriersAdded AS IntegratedCarriersAdded,
        @ShippingMethodsAdded    AS ShippingMethodsAdded,
        @ChargeTypesAdded        AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Bukuship] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'        AS [Status],
        @ErrorNumber   AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine     AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (each INSERT independently idempotent)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status + discovery counts
✅ #11 - charge_category_id = 11 (Other) for all; is_freight=1 only for "Freight Charge"
✅ #12 - Joins carrier_bill and filters by @File_id in all blocks
Aggregator rule: Bukuship is the aggregator (@Carrier_id); Block 0 auto-discovers
                 integrated carriers (Landmark Global, DHL eCommerce) before Block 1
                 uses integrated_carrier_id FK
================================================================================
*/
