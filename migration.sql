CREATE TABLE billing.delta_eliteworks_bill (
    time_utc VARCHAR(50) NULL,
    shipment_id VARCHAR(255) NULL,
    user_account VARCHAR(255) NULL,
    tracking_number VARCHAR(255) NULL,
    status VARCHAR(50) NULL,
    carrier VARCHAR(50) NULL,
    service VARCHAR(255) NULL,
    reference VARCHAR(255) NULL,
    shipment_weight_oz VARCHAR(50) NULL,
    shipment_dryice_weight_oz VARCHAR(50) NULL,
    package_type VARCHAR(50) NULL,
    package_length_in VARCHAR(50) NULL,
    package_width_in VARCHAR(50) NULL,
    package_height_in VARCHAR(50) NULL,
    from_name VARCHAR(255) NULL,
    from_company VARCHAR(255) NULL,
    from_street VARCHAR(255) NULL,
    from_apt_suite VARCHAR(50) NULL,
    from_city VARCHAR(100) NULL,
    from_state VARCHAR(50) NULL,
    from_postal VARCHAR(50) NULL,
    from_country VARCHAR(10) NULL,
    to_name VARCHAR(255) NULL,
    to_company VARCHAR(255) NULL,
    to_street VARCHAR(255) NULL,
    to_apt_suite VARCHAR(50) NULL,
    to_city VARCHAR(100) NULL,
    to_state VARCHAR(50) NULL,
    to_postal VARCHAR(50) NULL,
    to_country VARCHAR(10) NULL,
    first_scan VARCHAR(50) NULL,
    delivered VARCHAR(50) NULL,
    delivered_days VARCHAR(50) NULL,
    delivered_business_days VARCHAR(50) NULL,
    zone VARCHAR(50) NULL,
    charged VARCHAR(50) NULL,
    store_markup VARCHAR(50) NULL,
    platform_charged_with_corrections VARCHAR(50) NULL,
    commercial VARCHAR(50) NULL,
    order_reference VARCHAR(255) NULL,
    order_date VARCHAR(50) NULL
);

-- FLAVORCLOUD DELTA TABLE
CREATE TABLE billing.delta_flavorcloud_bill (
    [Invoice Number] VARCHAR(100) NULL,
    [Date] VARCHAR(50) NULL,
    [Order Number] VARCHAR(100) NULL,
    [Shipment Number] VARCHAR(255) NULL,
    [Service Level] VARCHAR(50) NULL,
    [Terms Of Trade] VARCHAR(50) NULL,
    [Origin Location] VARCHAR(255) NULL,
    [Destination Country] VARCHAR(10) NULL,
    [Ship To Address Zip] VARCHAR(50) NULL,
    [Carrier] VARCHAR(100) NULL,
    [Weight Unit] VARCHAR(10) NULL,
    [Total Weight] VARCHAR(50) NULL,
    [Dimension Unit] VARCHAR(10) NULL,
    [Length] VARCHAR(50) NULL,
    [Width] VARCHAR(50) NULL,
    [Height] VARCHAR(50) NULL,
    [Commissions (USD)] VARCHAR(50) NULL,
    [Duties (USD)] VARCHAR(50) NULL,
    [Taxes (USD)] VARCHAR(50) NULL,
    [Fees (USD)] VARCHAR(50) NULL,
    [LandedCost (Duty + Taxes + Fees) (USD)] VARCHAR(50) NULL,
    [Insurance (USD)] VARCHAR(50) NULL,
    [Shipping Charges (USD)] VARCHAR(50) NULL,
    [Order Value (USD)] VARCHAR(50) NULL,
    [Shipment Total Charges (USD)] VARCHAR(50) NULL,
    [Shipment Date] VARCHAR(50) NULL,
    [Payment Terms] VARCHAR(50) NULL,
    [Due Date] VARCHAR(50) NULL
);

CREATE TABLE billing.eliteworks_bill (
    id INT IDENTITY(1,1) NOT NULL,
    carrier_bill_id INT NULL,
    invoice_number NVARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    tracking_number NVARCHAR(255) NOT NULL,
    shipment_date DATETIME NULL,
    service_method NVARCHAR(255) NULL,
    zone NVARCHAR(50) NULL,
    charged_amount DECIMAL(18,2) NULL,
    store_markup DECIMAL(18,2) NULL,
    platform_charged DECIMAL(18,2) NULL,
    billed_weight_oz DECIMAL(18,2) NULL,
    package_length_in DECIMAL(18,2) NULL,
    package_width_in DECIMAL(18,2) NULL,
    package_height_in DECIMAL(18,2) NULL,
    from_postal NVARCHAR(50) NULL,
    to_postal NVARCHAR(50) NULL,
    to_city NVARCHAR(100) NULL,
    to_state NVARCHAR(50) NULL,
    to_country NVARCHAR(10) NULL,
    shipment_status NVARCHAR(50) NULL,
    created_date DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    
    CONSTRAINT PK_eliteworks_bill PRIMARY KEY (id),
    CONSTRAINT FK_eliteworks_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
        REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_carrier_bill_id
ON billing.eliteworks_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_created_date
ON billing.eliteworks_bill (created_date);

-- Composite index for tracking number lookups
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_tracking_number_invoice
ON billing.eliteworks_bill (tracking_number, invoice_number, invoice_date);

-- FLAVORCLOUD BILL TABLE (Normalized carrier bill line items)
CREATE TABLE billing.flavorcloud_bill (
    id INT IDENTITY(1,1) NOT NULL,
    carrier_bill_id INT NULL,
    invoice_number NVARCHAR(100) NOT NULL,
    invoice_date DATE NOT NULL,
    order_number NVARCHAR(100) NULL,
    tracking_number NVARCHAR(255) NOT NULL,
    service_level NVARCHAR(50) NULL,
    terms_of_trade NVARCHAR(50) NULL,
    origin_location NVARCHAR(255) NULL,
    destination_country NVARCHAR(10) NULL,
    ship_to_zip NVARCHAR(50) NULL,
    carrier_name NVARCHAR(100) NULL,
    total_weight DECIMAL(18,6) NULL,
    weight_unit NVARCHAR(10) NULL,
    length DECIMAL(18,2) NULL,
    width DECIMAL(18,2) NULL,
    height DECIMAL(18,2) NULL,
    dimension_unit NVARCHAR(10) NULL,
    commissions DECIMAL(18,2) NULL,
    duties DECIMAL(18,2) NULL,
    taxes DECIMAL(18,2) NULL,
    fees DECIMAL(18,2) NULL,
    landed_cost DECIMAL(18,2) NULL,
    insurance DECIMAL(18,2) NULL,
    shipping_charges DECIMAL(18,2) NULL,
    order_value DECIMAL(18,2) NULL,
    shipment_total_charges DECIMAL(18,2) NULL,
    shipment_date DATE NULL,
    payment_terms NVARCHAR(50) NULL,
    due_date DATE NULL,
    created_date DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    
    CONSTRAINT PK_flavorcloud_bill PRIMARY KEY (id),
    CONSTRAINT FK_flavorcloud_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
        REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_flavorcloud_bill_carrier_bill_id
ON billing.flavorcloud_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_flavorcloud_bill_created_date
ON billing.flavorcloud_bill (created_date);

-- Composite index for tracking number lookups
CREATE NONCLUSTERED INDEX IX_flavorcloud_bill_tracking_number_invoice
ON billing.flavorcloud_bill (tracking_number, invoice_number, invoice_date);