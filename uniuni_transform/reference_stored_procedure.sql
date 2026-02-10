CREATE PROCEDURE elt_stage.usp_SyncUniUniBillingData
    AS

BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;  -- safer: auto-rollback on many runtime errors

DECLARE @ProcedureName SYSNAME = 'usp_SyncUniUniBillingData';
DECLARE @MinCreatedDate DATETIME2;
DECLARE @CarrierId INT;

BEGIN TRY

SELECT @MinCreatedDate = last_run_time
FROM elt_stage.etl_run_tracker
WHERE procedure_name = @ProcedureName;

SELECT @CarrierId = carrier_id
FROM dbo.carrier
WHERE carrier_name = 'UniUni';

IF @MinCreatedDate IS NULL
SET @MinCreatedDate = '2000-01-01';

BEGIN TRANSACTION;

-- 1. Enrich Shipment

UPDATE s
SET
    s.destination_zone = uni.[zone],
    s.carrier_id = @CarrierId
    FROM dbo.shipment_wip AS s
	JOIN dbo.shipment_package_wip AS sp
ON s.shipment_id = sp.shipment_id
    INNER JOIN elt_stage.uniuni_bill AS uni
    ON sp.tracking_number = uni.tracking_number
WHERE uni.created_at > @MinCreatedDate;

-- 2. Enrich Shipment Package

UPDATE sp
SET  sp.carrier_pickup_date = uni.induction_time,
     sp.shipping_method_id = sm.shipping_method_id,
     sp.billed_weight_oz = CASE
                               WHEN uni.dim_weight_uom = 'LBS'  -- handle variants
                                   THEN TRY_CAST(uni.dim_weight * 16.0 AS DECIMAL(18,2))
                               WHEN uni.dim_weight_uom =  'OZS'
                                   THEN uni.dim_weight
                               WHEN uni.dim_weight_uom IS NULL
                                   THEN NULL
                               ELSE NULL  -- Unknown unit, will be NULL
         END,
     sp.actual_weight_oz = CASE
                               WHEN uni.scaled_weight_uom = 'LBS'  -- handle variants
                                   THEN TRY_CAST(uni.scaled_weight * 16.0 AS DECIMAL(18,2))
                               WHEN uni.scaled_weight_uom =  'OZS'
                                   THEN uni.scaled_weight
                               WHEN uni.scaled_weight_uom IS NULL
                                   THEN NULL
                               ELSE NULL  -- Unknown unit, will be NULL
         END,
     sp.billed_length_in    = CASE
                                  WHEN uni.package_dim_uom = 'CM'  -- handle variants
                                      THEN TRY_CAST(uni.dim_length * 0.393701 AS DECIMAL(18,2))
                                  WHEN uni.package_dim_uom =  'IN'
                                      THEN uni.dim_length
                                  WHEN uni.package_dim_uom IS NULL
                                      THEN NULL
                                  ELSE NULL  -- Unknown unit, will be NULL
         END,
     sp.billed_width_in     = CASE
                                  WHEN uni.package_dim_uom = 'CM'  -- handle variants
                                      THEN TRY_CAST(uni.dim_width * 0.393701 AS DECIMAL(18,2))
                                  WHEN uni.package_dim_uom =  'IN'
                                      THEN uni.dim_width
                                  WHEN uni.package_dim_uom IS NULL
                                      THEN NULL
                                  ELSE NULL  -- Unknown unit, will be NULL
         END,
     sp.billed_height_in    = CASE
                                  WHEN uni.package_dim_uom = 'CM'  -- handle variants
                                      THEN TRY_CAST(uni.dim_height * 0.393701 AS DECIMAL(18,2))
                                  WHEN uni.package_dim_uom =  'IN'
                                      THEN uni.dim_height
                                  WHEN uni.package_dim_uom IS NULL
                                      THEN NULL
                                  ELSE NULL  -- Unknown unit, will be NULL
         END
    FROM dbo.shipment_package_wip AS sp
INNER JOIN elt_stage.uniuni_bill AS uni
ON sp.tracking_number = uni.tracking_number
    LEFT JOIN dbo.shipping_method AS sm
    ON sm.method_name = uni.service_type
WHERE uni.created_at > @MinCreatedDate;

-- 3. Aggregate invoice into Carrier Bill

INSERT INTO dbo.carrier_bill (
    carrier_id, bill_number, bill_date, total_amount, num_shipments
)
SELECT
    @CarrierId,
    CAST(uni.invoice_number AS VARCHAR(40)),
    CAST(uni.invoice_time  AS DATE),
    SUM(uni.base_fee+uni.discount_fee+uni.discount_percentage+uni.signature_fee+uni.pickup_fee+uni.over_dimension_fee+uni.over_max_size_fee+uni.over_weight_fee+uni.fuel_surcharge+uni.peak_season_surcharge+uni.delivery_area_surcharge+uni.truck_fee+uni.relabel_fee+uni.miscellaneous_fee+uni.credit_card_surcharge),
    COUNT(uni.tracking_number)
FROM elt_stage.uniuni_bill AS uni
WHERE uni.created_at > @MinCreatedDate
GROUP BY
    uni.invoice_number, CAST(uni.invoice_time AS DATE)
HAVING NOT EXISTS (
    SELECT 1
    FROM dbo.carrier_bill AS cb
    WHERE cb.bill_number = CAST(uni.invoice_number AS VARCHAR(40))
);

-- 4. insert into shipment_charges
INSERT INTO dbo.shipment_charges (shipment_id, shipment_package_id, charge_type_id, amount)
SELECT sp.shipment_id, sp.shipment_package_id, ct.charge_type_id, charges.amount
FROM elt_stage.uniuni_bill uni
    CROSS APPLY (VALUES ('Base Rate', uni.base_fee),
                        ('Discount Fee', uni.discount_fee),
                        ('Discount Percentage', uni.discount_percentage),
                        ('Signature Fee', uni.signature_fee),
                        ('Pick Up Fee', uni.pickup_fee),
                        ('Over Dimension Fee', uni.over_dimension_fee),
                        ('Over Max Size Fee', uni.over_max_size_fee),
                        ('Over Weight Fee', uni.over_weight_fee),
                        ('Fuel Surcharge', uni.fuel_surcharge),
                        ('Peak Season Surcharge', uni.peak_season_surcharge),
                        ('Delivery Area Surcharge', uni.delivery_area_surcharge),
                        ('Truck Fee', uni.truck_fee),
                        ('Relabel Fee', uni.relabel_fee),
                        ('Miscellaneous Fee', uni.miscellaneous_fee),
                        ('Credit Card Surcharge', uni.credit_card_surcharge)) AS charges(description, amount)
INNER JOIN dbo.charge_types ct ON ct.charge_name = charges.description AND ct.carrier_id = @CarrierId
INNER JOIN dbo.shipment_package_wip sp ON sp.tracking_number = uni.tracking_number
WHERE uni.created_at > @MinCreatedDate AND charges.amount > 0
AND NOT EXISTS (
SELECT 1 FROM dbo.shipment_charges sc
WHERE sc.shipment_id = sp.shipment_id
  AND sc.shipment_package_id = sp.shipment_package_id
  AND sc.charge_type_id = ct.charge_type_id
)

-- 5. Update ETL run tracker with current timestamp
MERGE INTO elt_stage.etl_run_tracker AS target
USING (SELECT @ProcedureName AS proc_name, SYSDATETIME() AS run_time) AS source
ON target.procedure_name = source.proc_name
WHEN MATCHED THEN
    UPDATE SET last_run_time = source.run_time
WHEN NOT MATCHED THEN
    INSERT (procedure_name, last_run_time) VALUES (source.proc_name, source.run_time);

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
