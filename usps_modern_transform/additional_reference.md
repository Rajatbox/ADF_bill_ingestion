Source: ShipHero label-level API export (USPS Modern only — no row-level filter needed)
One row per shipment. 44 columns, clean snake_case headers.

Invoice number = USPS_Modern_{MIN(shipment_created_date date)}_{MAX(shipment_created_date date)}
Bill date      = MAX(CAST(shipment_created_date AS DATE))
One carrier_bill per file.

Ship date      = label_created_date
Account number = carrier_account_id (present in file — no ADF injection needed)

Weight: dim_weight column → string with embedded unit e.g. "1.5562 lb"
        Parse numeric with LEFT(..., CHARINDEX(' ',...) - 1), unit always LB → convert to OZ (× 16)

Dimensions: dim_height / dim_width / dim_length → string e.g. "1.0000 inch"
            Parse numeric with LEFT(..., CHARINDEX(' ',...) - 1), unit always inch → no conversion

Charge type   = "Freight charge" (charge_category_id = 15, Transportation)
Charge amount = cost column (DECIMAL)
