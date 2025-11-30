-- 3. Cortex AI SQLを使用して非構造化データをSilver層へ格納する。(半構造化)
CREATE OR REPLACE TABLE SILVER.SNOWVILL.extract_tb
AS
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
    DIRECTORY(@BRONZE.SNOWVILL.DEMO_STG)
WHERE
    relative_path LIKE 'contract/%';

-- パイプライン内のINSERT
INSERT INTO SILVER.SNOWVILL.extract_tb
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

-- 確認
SELECT * FROM SILVER.SNOWVILL.extract_tb;
SELECT * FROM BRONZE.SNOWVILL.DEMO_ST;



-- 4. Silver層へ格納したデータを構造化テーブルに変換してGold層へ格納する。
CREATE OR REPLACE DYNAMIC TABLE GOLD.SNOWVILL.structured_tb 
WAREHOUSE = 'SNOWSIGHT_WH'
TARGET_LAG = DOWNSTREAM
REFRESH_MODE = INCREMENTAL
INITIALIZE = on_create
AS
SELECT
    file_name,
    json_data:error AS result,
    TO_DATE(json_data:response:start_date::STRING, 'YYYY"年"MM"月"DD"日"') AS start_date,
    json_data:response:contract_term::INTEGER AS contract_term,
    json_data:response:remuneration::STRING AS remuneration,
    json_data:response:acceptance::INTEGER AS acceptance,
    json_data:response:nad_term::STRING AS nad_term,
    json_data:response:guarantee::INTEGER AS guarantee
FROM 
    SILVER.SNOWVILL.extract_tb;

-- パイプライン内のREFRESH
ALTER DYNAMIC TABLE GOLD.SNOWVILL.structured_tb REFRESH;

-- 確認
SELECT * FROM GOLD.SNOWVILL.structured_tb;
