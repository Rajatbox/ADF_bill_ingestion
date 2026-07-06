/*
================================================================================
Rollback Script: Delete all traces of a file from the database
================================================================================
Carriers: EasyPost, Eliteworks, Vanlo

Input: @File_id — everything else resolved via JOIN to carrier_bill
================================================================================
*/

  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @File_id INT = /* enter file_id */;

  BEGIN TRANSACTION;
  BEGIN TRY

      -- 1. carrier_cost_ledger
      DELETE ccl
      FROM dbo.carrier_cost_ledger ccl
      JOIN billing.carrier_bill cb ON cb.carrier_bill_id = ccl.carrier_bill_id
      WHERE cb.file_id = @File_id;

      -- 2. shipment_charges + capture attribute IDs before deleting
      SELECT DISTINCT sc.shipment_attribute_id
      INTO #attr_ids
      FROM billing.shipment_charges sc
      JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
      WHERE cb.file_id = @File_id;

      DELETE sc
      FROM billing.shipment_charges sc
      JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
      WHERE cb.file_id = @File_id;

      -- 3. shipment_attributes (only orphans)
      DELETE sa
      FROM billing.shipment_attributes sa
      JOIN #attr_ids a ON a.shipment_attribute_id = sa.id
      WHERE NOT EXISTS (
          SELECT 1 FROM billing.shipment_charges sc WHERE sc.shipment_attribute_id = sa.id
      )
      AND NOT EXISTS (
          SELECT 1 FROM dbo.carrier_cost_ledger ccl WHERE ccl.shipment_attribute_id = sa.id
      );

      DROP TABLE #attr_ids;

      -- 4. Dynamic carrier-specific bill table delete
      DECLARE @carrier_id   INT;
      DECLARE @carrier_name NVARCHAR(255);
      DECLARE @sql          NVARCHAR(MAX);

      SELECT @carrier_id = carrier_id
      FROM billing.file_ingestion_tracker
      WHERE file_id = @File_id;

      SELECT @carrier_name = carrier_name
      FROM dbo.carrier
      WHERE carrier_id = @carrier_id;

      SET @sql = N'
          DELETE cb_tbl
          FROM billing.' + QUOTENAME(@carrier_name + '_bill') + N' AS cb_tbl
          JOIN billing.carrier_bill cb ON cb.carrier_bill_id = cb_tbl.carrier_bill_id
          WHERE cb.file_id = @File_id;
      ';

      EXEC sp_executesql @sql, N'@File_id INT', @File_id = @File_id;

      -- 5. carrier_bill
      DELETE FROM billing.carrier_bill WHERE file_id = @File_id;

      -- 6. file_ingestion_tracker
      DELETE FROM billing.file_ingestion_tracker WHERE file_id = @File_id;

      COMMIT TRANSACTION;
      SELECT 'SUCCESS' AS Status;

  END TRY
  BEGIN CATCH
      IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
      SELECT 'ERROR' AS Status, ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
      THROW;
  END CATCH;
