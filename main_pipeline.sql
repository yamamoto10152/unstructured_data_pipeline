
-- 1. ステージの設置
CREATE OR REPLACE STAGE SNOWVILL.MINTSUYO.DEMO_STG
  encryption = (type = 'snowflake_sse') 
  DIRECTORY = (ENABLE = TRUE);

-- SELECT * FROM DIRECTORY(@YAMAMOTO_DB.DEMO_SC.DEMO_STG);

-- 2. ストリームの設置
CREATE OR REPLACE STREAM SNOWVILL.MINTSUYO.DEMO_ST 
ON DIRECTORY(@SNOWVILL.MINTSUYO.DEMO_STG);


-- 3. パイプラインの実行プロシージャ
CREATE OR REPLACE PROCEDURE SNOWVILL.MINTSUYO.UNSTRUCTURED_PIPELINE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CREATE OR REPLACE TEMPORARY TABLE staging_stream AS
    SELECT * FROM SNOWVILL.MINTSUYO.DEMO_ST WHERE metadata$action = 'INSERT';
    
  CREATE OR REPLACE TEMPORARY TABLE staging_json AS
    SELECT
      relative_path AS file_name,
      AI_EXTRACT(
        file => TO_FILE('@SNOWVILL.MINTSUYO.DEMO_STG', relative_path),
          responseFormat => {
            'schema': {
              'type': 'object',
              'properties': {
                        'building': {
                            'description': '設置物件の名前は？',
                            'type': 'string'
                        },
                        'company': {
                            'description': '会社の名前は？',
                            'type': 'string'
                        },
                        'check_date': {
                            'description': '点検時期は1年のうち何月？',
                            'type': 'string'
                        },
                        'total': {
                            'description': '点検料金の総額は？',
                            'type': 'string'
                        },
                        'subscribe': {
                            'description': '第5条の４に書かれている文章のアンダーライン（下線）が引かれているテキストのみ抽出',
                            'type': 'string'
                        },
                        'term': {
                            'description': '契約の有効期間は？',
                            'type': 'string'
                        }
                    }
                }
            }
        ) AS json_data
    FROM 
        staging_stream;

    INSERT INTO yamamoto.ug_handson.bronze_json 
    SELECT * FROM staging_json;
    

    CREATE OR REPLACE TEMPORARY TABLE staging_silver AS
    SELECT
    file_name,
    json_data:error AS result,
    json_data:response:building::STRING AS building,
    json_data:response:company::STRING AS company,
    json_data:response:check_date::STRING AS check_date,
    json_data:response:total::STRING AS total,
    json_data:response:subscribe::STRING AS subscribe,
    json_data:response:term::STRING AS term
    FROM staging_json;


    INSERT INTO yamamoto.ug_handson.silver_json
    SELECT * FROM staging_silver;



    INSERT INTO yamamoto.ug_handson.silver_error
    SELECT 
        file_name, 
        result
    FROM yamamoto.ug_handson.silver_json
    WHERE 
        result != 'null' 
        OR building != 'None' 
        OR company != 'None' 
        OR check_date != 'None' 
        OR total != 'None' 
        OR subscribe != 'None'
        OR term != 'None';

    DROP TABLE staging_stream;
    
    RETURN TRUE;
END;
$$;


-- 4. ストリーム検知のタスク実行
CREATE OR REPLACE TASK triggered_task_stream
  WAREHOUSE = YAMAMOTO_WH
  WHEN SYSTEM$STREAM_HAS_DATA('ug_handson_stream')
  AS
  CALL
  DATA_PROCESSING_PIPELINE();



-- 5. ファイル削除