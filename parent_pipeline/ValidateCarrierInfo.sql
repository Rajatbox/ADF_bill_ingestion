/*
================================================================================
Validate Carrier & Initialize File Tracking (Combined)
================================================================================
Purpose: 
1. Validate account number matches file path
2. Check if file already processed (fail fast if yes)
3. Create file tracking record

Returns: carrier_id, file_id, validated_carrier_name
================================================================================
*/

SET NOCOUNT ON;

-- 1. Accept params from ADF
DECLARE @RawJson NVARCHAR(MAX) = '@{string(activity('LookupAccountInFile').output.firstRow)}';
DECLARE @InputCarrier NVARCHAR(100) = LOWER('@{variables('v_FileMetadata')[1]}');
DECLARE @ExpectedAccount NVARCHAR(100) = '@{variables('v_FileMetadata')[2]}';
DECLARE @FileName NVARCHAR(255) = '@{variables('v_FileMetadata')[3]}';

-- 2. Extract account from file
DECLARE @ActualAccountInFile NVARCHAR(100) = CASE 
    WHEN @InputCarrier = 'fedex' THEN JSON_VALUE(@RawJson, '$.Prop_1')
    WHEN @InputCarrier = 'dhl'   THEN JSON_VALUE(@RawJson, '$.Prop_1')
    WHEN @InputCarrier = 'ups'   THEN JSON_VALUE(@RawJson, '$.Prop_2')
    WHEN @InputCarrier = 'usps - easy post' THEN JSON_VALUE(@RawJson, '$.carrier_account_id')
    ELSE 'UNKNOWN_CARRIER'
END;

-- 3. Validate carrier is supported
IF @ActualAccountInFile = 'UNKNOWN_CARRIER'
BEGIN
    RAISERROR('Unsupported carrier: %s', 16, 1, @InputCarrier);
    RETURN;
END

-- 4. Validate account matches (exact match required)
IF NULLIF(@ActualAccountInFile, '') IS NULL OR @ActualAccountInFile <> @ExpectedAccount
BEGIN
    RAISERROR('Account mismatch. Carrier: %s | Expected: %s | Found: %s', 16, 1, 
              @InputCarrier, @ExpectedAccount, @ActualAccountInFile);
    RETURN;
END

-- 5. Get carrier_id
DECLARE @Carrier_id INT;
SELECT @Carrier_id = carrier_id 
FROM dbo.carrier 
WHERE LOWER(carrier_name) = @InputCarrier;

-- 6. Get or create file record (idempotent)
IF NOT EXISTS (
    SELECT 1 FROM billing.file_ingestion_tracker 
    WHERE file_name = @FileName AND carrier_id = @Carrier_id
)
BEGIN
    INSERT INTO billing.file_ingestion_tracker (file_name, carrier_id, created_at)
    VALUES (@FileName, @Carrier_id, SYSDATETIME());
END

-- 7. Return record with routing logic
-- If completed_at IS NOT NULL → 'Skip' (ADF Switch routes to Fail activity)
-- If completed_at IS NULL → carrier name (ADF Switch routes to child pipeline)
SELECT 
    file_id,
    carrier_id,
    CASE 
        WHEN completed_at IS NOT NULL THEN 'Skip'
        ELSE @InputCarrier
    END AS validated_carrier_name
FROM billing.file_ingestion_tracker
WHERE file_name = @FileName AND carrier_id = @Carrier_id;


