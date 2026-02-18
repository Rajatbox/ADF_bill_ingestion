CREATE TABLE billing.file_ingestion_tracker (
	file_id int IDENTITY(1,1) NOT NULL,
	file_name varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	carrier_id int NOT NULL,
	created_at datetime2 DEFAULT sysdatetime() NOT NULL,
	completed_at datetime2 NULL,  -- NULL = in progress/failed/retry, NOT NULL = completed
	CONSTRAINT PK_file_ingestion_tracker PRIMARY KEY (file_id),
	CONSTRAINT FK_file_ingestion_tracker_carrier FOREIGN KEY (carrier_id) 
		REFERENCES dbo.carrier(carrier_id),
	CONSTRAINT UQ_file_ingestion_tracker_file_carrier UNIQUE (file_name, carrier_id)
);

-- Index for completion queries
CREATE NONCLUSTERED INDEX IX_file_ingestion_tracker_completed
ON billing.file_ingestion_tracker (carrier_id, completed_at);


-----------Carrier Bill Table-----------

-- Add file_id column to track which file each invoice came from
ALTER TABLE billing.carrier_bill 
ADD file_id INT NULL;

-- Foreign key to file_ingestion_tracker
ALTER TABLE billing.carrier_bill 
ADD CONSTRAINT FK_carrier_bill_file_ingestion 
FOREIGN KEY (file_id) REFERENCES billing.file_ingestion_tracker(file_id);

-- Index for file-based filtering (one file can have MANY invoices)
CREATE NONCLUSTERED INDEX IX_carrier_bill_file_id
ON billing.carrier_bill (file_id);


-----------FedEx Charges View-----------

-- Update view to include file_id for file-based filtering
DROP VIEW IF EXISTS billing.vw_FedExCharges;
GO

CREATE VIEW billing.vw_FedExCharges
AS
SELECT
    fb.carrier_bill_id,
    fb.invoice_number,
    fb.express_or_ground_tracking_id,
    fb.created_date,
    cb.file_id,  -- For file-based filtering in downstream scripts
    v.charge_type,
    v.charge_amount
FROM billing.fedex_bill fb
JOIN billing.carrier_bill cb ON cb.carrier_bill_id = fb.carrier_bill_id
OUTER APPLY (
    VALUES
        ('Transportation Charge', fb.[Transportation Charge Amount]),
        (fb.[Tracking ID Charge Description], fb.[Tracking ID Charge Amount]),
        (fb.[Tracking ID Charge Description_1], fb.[Tracking ID Charge Amount_1]),
        (fb.[Tracking ID Charge Description_2], fb.[Tracking ID Charge Amount_2]),
        (fb.[Tracking ID Charge Description_3], fb.[Tracking ID Charge Amount_3]),
        (fb.[Tracking ID Charge Description_4], fb.[Tracking ID Charge Amount_4]),
        (fb.[Tracking ID Charge Description_5], fb.[Tracking ID Charge Amount_5]),
        (fb.[Tracking ID Charge Description_6], fb.[Tracking ID Charge Amount_6]),
        (fb.[Tracking ID Charge Description_7], fb.[Tracking ID Charge Amount_7]),
        (fb.[Tracking ID Charge Description_8], fb.[Tracking ID Charge Amount_8]),
        (fb.[Tracking ID Charge Description_9], fb.[Tracking ID Charge Amount_9]),
        (fb.[Tracking ID Charge Description_10], fb.[Tracking ID Charge Amount_10]),
        (fb.[Tracking ID Charge Description_11], fb.[Tracking ID Charge Amount_11]),
        (fb.[Tracking ID Charge Description_12], fb.[Tracking ID Charge Amount_12]),
        (fb.[Tracking ID Charge Description_13], fb.[Tracking ID Charge Amount_13]),
        (fb.[Tracking ID Charge Description_14], fb.[Tracking ID Charge Amount_14]),
        (fb.[Tracking ID Charge Description_15], fb.[Tracking ID Charge Amount_15]),
        (fb.[Tracking ID Charge Description_16], fb.[Tracking ID Charge Amount_16]),
        (fb.[Tracking ID Charge Description_17], fb.[Tracking ID Charge Amount_17]),
        (fb.[Tracking ID Charge Description_18], fb.[Tracking ID Charge Amount_18]),
        (fb.[Tracking ID Charge Description_19], fb.[Tracking ID Charge Amount_19]),
        (fb.[Tracking ID Charge Description_20], fb.[Tracking ID Charge Amount_20]),
        (fb.[Tracking ID Charge Description_21], fb.[Tracking ID Charge Amount_21]),
        (fb.[Tracking ID Charge Description_22], fb.[Tracking ID Charge Amount_22]),
        (fb.[Tracking ID Charge Description_23], fb.[Tracking ID Charge Amount_23]),
        (fb.[Tracking ID Charge Description_24], fb.[Tracking ID Charge Amount_24]),
        (fb.[Tracking ID Charge Description_25], fb.[Tracking ID Charge Amount_25]),
        (fb.[Tracking ID Charge Description_26], fb.[Tracking ID Charge Amount_26]),
        (fb.[Tracking ID Charge Description_27], fb.[Tracking ID Charge Amount_27]),
        (fb.[Tracking ID Charge Description_28], fb.[Tracking ID Charge Amount_28]),
        (fb.[Tracking ID Charge Description_29], fb.[Tracking ID Charge Amount_29]),
        (fb.[Tracking ID Charge Description_30], fb.[Tracking ID Charge Amount_30]),
        (fb.[Tracking ID Charge Description_31], fb.[Tracking ID Charge Amount_31]),
        (fb.[Tracking ID Charge Description_32], fb.[Tracking ID Charge Amount_32]),
        (fb.[Tracking ID Charge Description_33], fb.[Tracking ID Charge Amount_33]),
        (fb.[Tracking ID Charge Description_34], fb.[Tracking ID Charge Amount_34]),
        (fb.[Tracking ID Charge Description_35], fb.[Tracking ID Charge Amount_35]),
        (fb.[Tracking ID Charge Description_36], fb.[Tracking ID Charge Amount_36]),
        (fb.[Tracking ID Charge Description_37], fb.[Tracking ID Charge Amount_37]),
        (fb.[Tracking ID Charge Description_38], fb.[Tracking ID Charge Amount_38]),
        (fb.[Tracking ID Charge Description_39], fb.[Tracking ID Charge Amount_39]),
        (fb.[Tracking ID Charge Description_40], fb.[Tracking ID Charge Amount_40]),
        (fb.[Tracking ID Charge Description_41], fb.[Tracking ID Charge Amount_41]),
        (fb.[Tracking ID Charge Description_42], fb.[Tracking ID Charge Amount_42]),
        (fb.[Tracking ID Charge Description_43], fb.[Tracking ID Charge Amount_43]),
        (fb.[Tracking ID Charge Description_44], fb.[Tracking ID Charge Amount_44]),
        (fb.[Tracking ID Charge Description_45], fb.[Tracking ID Charge Amount_45]),
        (fb.[Tracking ID Charge Description_46], fb.[Tracking ID Charge Amount_46]),
        (fb.[Tracking ID Charge Description_47], fb.[Tracking ID Charge Amount_47]),
        (fb.[Tracking ID Charge Description_48], fb.[Tracking ID Charge Amount_48]),
        (fb.[Tracking ID Charge Description_49], fb.[Tracking ID Charge Amount_49]),
        (fb.[Tracking ID Charge Description_50], fb.[Tracking ID Charge Amount_50])
) v (charge_type, charge_amount)
WHERE NULLIF(v.charge_type, '') IS NOT NULL
  AND v.charge_amount IS NOT NULL
  AND v.charge_amount <> 0;
GO


