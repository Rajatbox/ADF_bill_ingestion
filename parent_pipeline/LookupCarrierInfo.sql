/*
================================================================================
Lookup Script: Carrier Info & Last Run Time
================================================================================
Purpose: Retrieves carrier_id and last successful run timestamp for incremental
         processing. Used as the first step in ADF pipeline to set parameters.

ADF Pipeline Variables Required:
  INPUT:
    - v_FileMetadata: Array containing carrier name at index [1]
                      Example: variables('v_FileMetadata')[1] = 'FedEx'
  
  OUTPUT (Query Results):
    - carrier_id: INT - Carrier identifier for downstream queries
    - last_run_time: DATETIME2 - Last successful ingestion timestamp
                                  Defaults to '2000-01-01' if first run

Tables:
  - dbo.carrier (carrier master)
  - Test.carrier_ingestion_tracker (run history)

Execution Order: FIRST in pipeline (provides parameters for all subsequent steps)
================================================================================
*/

SELECT
    carrier_id,
    COALESCE(last_run_time, '2000-01-01') AS last_run_time
FROM
dbo.carrier c
LEFT JOIN 
Test.carrier_ingestion_tracker cit
ON
    c.carrier_name = cit.carrier_name
WHERE
    LOWER(c.carrier_name) = LOWER('@{variables(''v_FileMetadata'')[1]}')

