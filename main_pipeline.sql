-- データ加工処理のプロシージャ(生データに近いBronze)
CREATE OR REPLACE PROCEDURE BRONZE.SNOWVILL.UNSTRUCTURED_PIPELINE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CREATE TEMPORARY TABLE staging_stream AS
    SELECT * FROM BRONZE.SNOWVILL.DEMO_ST WHERE metadata$action = 'INSERT';

  
  INSERT INTO BRONZE.SNOWVILL.parse_tb
  SELECT  
      REPLACE(relative_path, 'document/', '') AS file_name,
      size,
      last_modified,
      AI_PARSE_DOCUMENT(TO_FILE('@BRONZE.SNOWVILL.DEMO_STG', relative_path), {'mode': 'OCR' , 'page_split': true}) AS json_data
  FROM 
      staging_stream
  WHERE
      relative_path LIKE 'document/%';


  INSERT INTO BRONZE.SNOWVILL.extract_tb
  SELECT 
      REPLACE(relative_path, 'contract/', '') AS file_name,
      AI_EXTRACT(
          file => TO_FILE('@BRONZE.SNOWVILL.DEMO_STG', relative_path),
          responseFormat => {
                  'schema': {
                      'type': 'object',
                      'properties': {
                          'start_date': {
                              'description': '契約開始日は？',
                              'type': 'string'
                          },
                          'contract_term': {
                              'description': '契約期間は？〇か月間で答えて',
                              'type': 'string'
                          },
                          'remuneration': {
                              'description': '固定報酬は？（税別）は消して',
                              'type': 'string'
                          },
                          'acceptance': {
                              'description': '検収の納入日は？〇営業日以内で答えて',
                              'type': 'string'
                          },
                          'nad_term': {
                              'description': '機密保持の義務期間は？',
                              'type': 'string'
                          },
                          'guarantee': {
                              'description': '保証は納入後何日間？',
                              'type': 'string'
                          }
                      }
                  }
              }
          ) AS json_data
  FROM 
      staging_stream
  WHERE
      relative_path LIKE 'contract/%';


  ALTER DYNAMIC TABLE SILVER.SNOWVILL.flatten_tb REFRESH;
  ALTER DYNAMIC TABLE SILVER.SNOWVILL.structured_tb REFRESH;

  DROP TABLE staging_stream;

  ALTER CORTEX SEARCH SERVICE GOLD.SNOWVILL.mintsuyo_search REFRESH;
  
  RETURN TRUE;
END;
$$;


-- ストリーム検知のタスク実行
CREATE OR REPLACE TASK BRONZE.SNOWVILL.STREAM_TRIGGER_TK
  WAREHOUSE = SNOWSIGHT_WH
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.SNOWVILL.DEMO_ST')
  AS
  CALL
    BRONZE.SNOWVILL.UNSTRUCTURED_PIPELINE();

-- タスク起動
ALTER TASK BRONZE.SNOWVILL.STREAM_TRIGGER_TK RESUME;