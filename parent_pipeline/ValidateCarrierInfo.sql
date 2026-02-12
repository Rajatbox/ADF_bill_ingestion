-- 1. Accept the entire row as a JSON string from ADF
DECLARE @RawJson NVARCHAR(MAX) = '@{string(activity('LookupAccountInFile').output.firstRow)}';
DECLARE @InputCarrier NVARCHAR(100) = LOWER('@{variables('v_FileMetadata')[1]}');
DECLARE @ExpectedAccount NVARCHAR(100) = '@{variables('v_FileMetadata')[2]}';

-- 2. Extract values using JSON_VALUE (Returns NULL if key is missing, no crashing!)
DECLARE @ActualAccountInFile NVARCHAR(100) = CASE 
    WHEN @InputCarrier = 'fedex' THEN JSON_VALUE(@RawJson, '$.Prop_1')
    WHEN @InputCarrier = 'dhl'   THEN JSON_VALUE(@RawJson, '$.Prop_1')
    WHEN @InputCarrier = 'ups'   THEN JSON_VALUE(@RawJson, '$.Prop_2')
    WHEN @InputCarrier = 'usps - easy post' THEN JSON_VALUE(@RawJson, '$.carrier_account_id')
    ELSE 'UNKNOWN_CARRIER'
END;

-- 3. Validation Logic
IF (
    (@InputCarrier = 'usps - easy post' AND @ActualAccountInFile NOT LIKE '%' + @ExpectedAccount + '%')
    OR 
    (@InputCarrier <> 'usps - easy post' AND (NULLIF(@ActualAccountInFile, '') IS NULL OR @ActualAccountInFile <> @ExpectedAccount))
    OR
    (@ActualAccountInFile = 'UNKNOWN_CARRIER')
)
BEGIN
    DECLARE @ErrMsg NVARCHAR(250) = FORMATMESSAGE('Validation Failed. Carrier: %s | Expected: %s | Found: %s', 
                                    @InputCarrier, @ExpectedAccount, COALESCE(@ActualAccountInFile, 'MISSING_COLUMN'));
    RAISERROR(@ErrMsg, 16, 1);
    RETURN;
END

-- 4. Final Metadata Fetch
SELECT
    c.carrier_id,
    COALESCE(cit.last_run_time, '2000-01-01') AS last_run_time,
    @InputCarrier AS validated_carrier_name -- Useful for the Switch later
FROM
    dbo.carrier c
LEFT JOIN 
    billing.carrier_ingestion_tracker cit ON c.carrier_name = cit.carrier_name
WHERE
    LOWER(c.carrier_name) = @InputCarrier;