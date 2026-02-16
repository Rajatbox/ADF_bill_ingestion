CREATE   PROCEDURE [elt_stage].[usp_SyncDHLBillingData]
    AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;  -- safer: auto-rollback on many runtime errors

DECLARE @ProcedureName SYSNAME = 'usp_SyncDHLBillingData';
DECLARE @MinCreatedDate DATETIME2;
DECLARE @CarrierId INT;

BEGIN TRY
-- 1) Get last successful run time
SELECT @MinCreatedDate = last_run_time
FROM elt_stage.etl_run_tracker
WHERE procedure_name = @ProcedureName;

SELECT @CarrierId = carrier_id
FROM dbo.carrier
WHERE carrier_name = 'DHL';

IF @MinCreatedDate IS NULL
SET @MinCreatedDate = '2000-01-01';  -- fallback default

BEGIN TRANSACTION;

-- 2) Update [shipment] from [dhl_bill]
UPDATE s
SET
    s.destination_zone = dhl.zone,
    s.carrier_id = @CarrierId
    FROM dbo.shipment_wip AS s
JOIN dbo.shipment_package_wip AS sp
ON s.shipment_id = sp.shipment_id
    INNER JOIN elt_stage.dhl_bill AS dhl
    ON sp.tracking_number = dhl.domestic_tracking_number
WHERE dhl.created_date > @MinCreatedDate;

UPDATE sp
SET sp.carrier_pickup_date = TRY_CAST(dhl.shipment_date AS datetime2),
    sp.shipping_method_id = sm.shipping_method_id,
    sp.billed_weight_oz    = CASE
                                 WHEN dhl.billed_weight_unit IN ('LB')
                                     THEN dhl.billed_weight * CAST(16 AS decimal(18,6))
                                 WHEN dhl.billed_weight_unit IN ('OZ')
                                     THEN dhl.billed_weight
                                 ELSE dhl.billed_weight
        END
    FROM dbo.shipment_package_wip AS sp
INNER JOIN elt_stage.dhl_bill AS dhl
ON sp.tracking_number = dhl.domestic_tracking_number
    LEFT JOIN dbo.shipping_method AS sm
    ON sm.method_name = dhl.shipping_method
WHERE dhl.created_date > @MinCreatedDate;

-- 3) update dimensions
-- todo: Need mapping from Jarred
/*;UPDATE sp
 SET
     sp.billed_length_in    = TRY_CAST(dhl.dim_length AS decimal(10,2)),
     sp.billed_width_in     = TRY_CAST(dhl.dim_width AS decimal(10,2)),
     sp.billed_height_in    = TRY_CAST(dhl.dim_height AS decimal(10,2))
 FROM dbo.shipment_package_wip AS sp
INNER JOIN elt_stage.dhl_bill AS dhl
 ON sp.tracking_number = dhl.tracking_number and dhl.dim_length > 0
 WHERE dhl.created_date > @MinCreatedDate;*/

-- 4) Insert into carrier bill
INSERT INTO dbo.carrier_bill (
    carrier_id,
    bill_number,
    bill_date,
    total_amount,
    num_shipments
)
SELECT
    @CarrierId                               AS carrier_id,
    cost.invoice_number                      AS bill_number,
    cost.invoice_date                        AS bill_date,
    cost.total_cost                          AS total_amount,
    ISNULL(cnt.shipment_count, 0)            AS num_shipments
FROM (
         -- Aggregate strictly by invoice_number (pick a single date per invoice)
         SELECT
             dhl.invoice_number,
             MAX(dhl.invoice_date) AS invoice_date,  -- or MIN(...) if you prefer
             SUM(
                     ISNULL(dhl.transportation_cost,0)
                         + ISNULL(dhl.fuel_surcharge,0)
                         + ISNULL(dhl.non_qualified_dim_charges,0)
                         + ISNULL(dhl.delivery_area_surcharge,0)
             ) AS total_cost
         FROM elt_stage.dhl_bill AS dhl
         GROUP BY dhl.invoice_number
     ) AS cost
         LEFT JOIN (
    -- Count shipments per invoice_number (normalize tracking numbers to avoid OR in join)
    SELECT
        dhl.invoice_number,
        COUNT(DISTINCT s.shipment_id) AS shipment_count
    FROM elt_stage.dhl_bill AS dhl
        CROSS APPLY (VALUES (dhl.overall_tracking_number),
                         (dhl.domestic_tracking_number)) AS t(tracking_number)
    JOIN dbo.shipment_package_wip AS sp
    ON sp.tracking_number = t.tracking_number
        JOIN dbo.shipment_wip AS s
        ON s.shipment_id = sp.shipment_id
    GROUP BY dhl.invoice_number
) AS cnt
                   ON cnt.invoice_number = cost.invoice_number
-- Prevent inserting duplicates if the invoice already exists
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.carrier_bill AS cb
    WHERE cb.bill_number = cost.invoice_number
);


-- 5) Insert into shipment_charges
;WITH base AS (
    SELECT
        sp.shipment_id,
        sp.shipment_package_id,
        cb.carrier_bill_id,
        dhl.overall_tracking_number,
        dhl.domestic_tracking_number,
        dhl.fuel_surcharge,
        dhl.delivery_area_surcharge,
        dhl.non_qualified_dim_charges
    FROM elt_stage.dhl_bill AS dhl
             LEFT JOIN dbo.carrier_bill AS cb
                       ON cb.bill_number = dhl.invoice_number
     CROSS APPLY (VALUES (dhl.overall_tracking_number),
                         (dhl.domestic_tracking_number)) AS t(tracking_number)
    INNER JOIN dbo.shipment_package_wip AS sp
        ON sp.tracking_number = t.tracking_number
    WHERE cb.carrier_bill_id IS NOT NULL
)
INSERT INTO dbo.shipment_charges (
    shipment_id, shipment_package_id, carrier_bill_id, charge_type_id, amount
)
SELECT DISTINCT
    b.shipment_id,
    b.shipment_package_id,
    b.carrier_bill_id,
    ct.charge_type_id,
    CAST(x.amount AS decimal(18,2)) AS amount
FROM base AS b
    CROSS APPLY (VALUES
                 (N'Fuel Surcharge',              b.fuel_surcharge)
                      , (N'Delivery Area Surcharge',     b.delivery_area_surcharge)
                      , (N'Non-Qualified Dim',   b.non_qualified_dim_charges)
                ) AS x(charge_name, amount)
JOIN dbo.charge_types AS ct
  ON ct.charge_name = x.charge_name
 AND ct.carrier_id  = @CarrierId
WHERE x.amount IS NOT NULL
  AND x.amount <> 0
  AND NOT EXISTS (   -- optional de-dupe
        SELECT 1
        FROM dbo.shipment_charges sc
        WHERE sc.shipment_id         = b.shipment_id
          AND sc.shipment_package_id = b.shipment_package_id
          AND sc.carrier_bill_id     = b.carrier_bill_id
          AND sc.charge_type_id      = ct.charge_type_id
);

-- 6) Upsert run tracker
MERGE elt_stage.etl_run_tracker AS target
        USING (SELECT @ProcedureName AS procedure_name) AS source
            ON target.procedure_name = source.procedure_name
        WHEN MATCHED THEN
UPDATE SET last_run_time = SYSDATETIME()
    WHEN NOT MATCHED THEN
INSERT (procedure_name, last_run_time)
    VALUES (@ProcedureName, SYSDATETIME());

COMMIT;
END TRY
BEGIN CATCH
        IF @@TRANCOUNT > 0
ROLLBACK;

-- Log the error
INSERT INTO elt_stage.elt_error_log (
    procedure_name,
    error_message,
    error_number,
    error_severity,
    error_state,
    error_line,
    error_time
)
VALUES (
           OBJECT_NAME(@@PROCID),
           ERROR_MESSAGE(),
           ERROR_NUMBER(),
           ERROR_SEVERITY(),
           ERROR_STATE(),
           ERROR_LINE(),
           SYSDATETIME()
       );

-- Re-throw to preserve original error metadata
THROW;
END CATCH
END;
