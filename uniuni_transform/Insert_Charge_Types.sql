SET NOCOUNT ON;

DECLARE @ChargeTypesAdded INT;

/*
================================================================================
Insert UniUni Charge Types
================================================================================
Inserts all 18 UniUni charge types into charge_types table.
Matches charge names from reference stored procedure.
Only inserts charge types that don't already exist for this carrier.

Charge Type Flags:
- freight: 1 = freight charge, 0 = non-freight
- dt: 1 = DT (Duties & Taxes), 0 = non-DT
- markup: 1 = subject to markup, 0 = no markup
- category: Base, Discount, Accessorial, Surcharge, Adjustment
================================================================================
*/

INSERT INTO dbo.charge_types (
    carrier_id,
    charge_name,
    freight,
    dt,
    markup,
    category
)
SELECT
    @Carrier_id AS carrier_id,
    charge_name,
    freight,
    dt,
    markup,
    category
FROM (
    VALUES  
        ('Base Rate', 1, 0, 1, 'Base'),
        ('Billed Fee', 1, 0, 0, 'Base'),
        ('Discount Fee', 0, 1, 1, 'Discount'),
        ('Signature Fee', 0, 1, 1, 'Accessorial'),
        ('Pick Up Fee', 0, 1, 1, 'Accessorial'),
        ('Over Dimension Fee', 0, 1, 1, 'Surcharge'),
        ('Over Max Size Fee', 0, 1, 1, 'Surcharge'),
        ('Over Weight Fee', 0, 1, 1, 'Surcharge'),
        ('Fuel Surcharge', 0, 1, 1, 'Surcharge'),
        ('Peak Season Surcharge', 0, 1, 1, 'Surcharge'),
        ('Delivery Area Surcharge', 0, 1, 1, 'Surcharge'),
        ('Delivery Area Surcharge Extend', 0, 1, 1, 'Surcharge'),
        ('Truck Fee', 0, 1, 1, 'Accessorial'),
        ('Relabel Fee', 0, 1, 1, 'Accessorial'),
        ('Miscellaneous Fee', 0, 1, 1, 'Accessorial'),
        ('Credit Card Surcharge', 0, 1, 1, 'Surcharge'),
        ('Credit', 0, 1, 1, 'Adjustment'),
        ('Approved Claim', 0, 1, 1, 'Adjustment')
) AS charges(charge_name, freight, dt, markup, category)
WHERE
    NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types AS ct
        WHERE ct.charge_name = charges.charge_name
            AND ct.carrier_id = @Carrier_id
    );

SET @ChargeTypesAdded = @@ROWCOUNT;

-- Return result
SELECT 
    @ChargeTypesAdded AS ChargeTypesAdded,
    CASE 
        WHEN @ChargeTypesAdded > 0 THEN 'SUCCESS: ' + CAST(@ChargeTypesAdded AS VARCHAR(10)) + ' charge types inserted'
        ELSE 'INFO: All charge types already exist'
    END AS Message;
