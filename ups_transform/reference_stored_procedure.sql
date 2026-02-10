/* 
This is a narrow table format bill so no need to unpivot the data.

For adding data to shipment_attributes table only use the rows where: 
 ups.charge_category_code = 'SHP'
 AND ups.charge_classification_code = 'FRT'
*/

CREATE   PROCEDURE [elt_stage].[usp_SyncUSPSEasyPost]
    AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;  -- safer: auto-rollback on many runtime errors


/*******************************************************************************
Schema:         dbo
Name:           [elt_stage].[usp_SyncFedExBillingData]
Description:    Step-2 for populating and enriching data into Gold tables after the Backend loads usps_bill

Source Tables:  - usps_bill
Target Table:   - Shipment_wip, shipment_package_wip, shipment_charges,carrier_bill

 ********** carrier_cost_ledge insert query missing **********

Versioning:
Date        Author          Reason
----------  --------------  ----------------------------------------------------
01-29-2026	Rajat			Cost correction for Carrier_bill and shipment_charges

*******************************************************************************/

DECLARE @ProcedureName SYSNAME = 'usp_SyncUSPSEasyPost';
DECLARE @MinCreatedDate DATETIME2;
DECLARE @CarrierId INT;

BEGIN TRY
-- 1) Get last successful run time
SELECT @MinCreatedDate = last_run_time
FROM elt_stage.etl_run_tracker
WHERE procedure_name = @ProcedureName;

SELECT @CarrierId = carrier_id
FROM dbo.carrier
WHERE carrier_name = 'USPS - Easy Post';

IF @MinCreatedDate IS NULL
SET @MinCreatedDate = '2000-01-01';  -- fallback default

BEGIN TRANSACTION;
-- 1. enrich the shipment table
UPDATE s
SET
    s.destination_zone = usps.usps_zone,
    s.carrier_id = @CarrierId
    FROM dbo.shipment_wip AS s
	JOIN dbo.shipment_package_wip AS sp
ON s.shipment_id = sp.shipment_id
    INNER JOIN elt_stage.usps_easy_post_bill AS usps
    ON sp.tracking_number = usps.tracking_code
WHERE usps.created_at > @MinCreatedDate;

-- 2. enrich the shipment_package
UPDATE sp
SET  sp.carrier_pickup_date = usps.postage_label_created_at,
     sp.shipping_method_id = sm.shipping_method_id,
     sp.billed_weight_oz = usps.weight,
     sp.billed_length_in    = TRY_CAST(usps.[length] AS decimal(10,2)),
     sp.billed_width_in     = TRY_CAST(usps.width AS decimal(10,2)),
     sp.billed_height_in    = TRY_CAST(usps.height AS decimal(10,2))
FROM dbo.shipment_package_wip AS sp
INNER JOIN elt_stage.usps_easy_post_bill AS usps
ON sp.tracking_number = usps.tracking_code
    LEFT JOIN dbo.shipping_method AS sm
    ON sm.method_name = usps.service
WHERE usps.created_at > @MinCreatedDate;

-- 3. insert in the carrier_bill
INSERT INTO dbo.carrier_bill (
    carrier_id, bill_number, bill_date, total_amount, num_shipments
)
SELECT
    @CarrierId,
    usps.invoice_number,
    CAST(usps.bill_date  AS DATE),
    SUM(usps.label_fee + usps.postage_fee + usps.carbon_offset_fee + usps.insurance_fee), --removed rate from addition clause 
    COUNT(usps.tracking_code)
FROM elt_stage.usps_easy_post_bill AS usps
WHERE usps.created_at > @MinCreatedDate
GROUP BY
    usps.invoice_number, CAST(usps.bill_date AS DATE)
HAVING NOT EXISTS (
    SELECT 1
    FROM dbo.carrier_bill AS cb
    WHERE cb.bill_number = usps.invoice_number
);

-- 4. insert into shipment_charges
INSERT INTO dbo.shipment_charges (shipment_id, shipment_package_id, charge_type_id, amount)
SELECT sp.shipment_id, sp.shipment_package_id, ct.charge_type_id, charges.amount
FROM elt_stage.usps_easy_post_bill usps
    CROSS APPLY (VALUES ('Base Rate', usps.rate),
                        ('Label Fee', usps.label_fee),
                        ('Unknown Charges', CAST(usps.postage_fee as float) - CAST(usps.rate as float)), 
                        ('Carbon Offset Fee', usps.carbon_offset_fee),
                        ('Insurance Fee', usps.insurance_fee)) AS charges(description, amount)
INNER JOIN dbo.charge_types ct ON ct.charge_name = charges.description AND ct.carrier_id = @CarrierId
INNER JOIN dbo.shipment_package_wip sp ON sp.tracking_number = usps.tracking_code
WHERE usps.created_at > @MinCreatedDate AND charges.amount <> 0 						-- Changed > 0 to <> 0 to handel -ve charges
AND NOT EXISTS (
SELECT 1 FROM dbo.shipment_charges sc
WHERE sc.shipment_id = sp.shipment_id
  AND sc.shipment_package_id = sp.shipment_package_id
  AND sc.charge_type_id = ct.charge_type_id
)

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
