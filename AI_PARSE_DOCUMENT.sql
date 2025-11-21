-- 1. AI_PARSE_DOCUMENTで社内ドキュメントを半構造化データに加工する。
CREATE OR REPLACE TABLE snowvill.mintsuyo.parse_tb
AS
SELECT  
    REPLACE(relative_path, 'document/', '') AS file_name,
    size,
    last_modified,
    AI_PARSE_DOCUMENT(TO_FILE('@SNOWVILL.MINTSUYO.DEMO_STG', relative_path), {'mode': 'OCR' , 'page_split': true}) AS json_data
FROM 
    DIRECTORY(@SNOWVILL.MINTSUYO.DEMO_STG)
WHERE
    relative_path LIKE 'document/%';


IN

SELECT * FROM snowvill.mintsuyo.parse_tb;



-- 2. 半構造化データをCortex Searchで使える形に加工する。
CREATE OR REPLACE DYNAMIC TABLE snowvill.mintsuyo.flatten_tb
WAREHOUSE = 'SNOWSIGHT_WH'
TARGET_LAG = DOWNSTREAM
REFRESH_MODE = INCREMENTAL
INITIALIZE = on_create
AS
SELECT
    file_name,
    size,
    last_modified,
    json_data:metadata:"pageCount"::number AS pagecount,
    index,
    value:content::varchar AS content
FROM 
    snowvill.mintsuyo.parse_tb, LATERAL FLATTEN(INPUT => snowvill.mintsuyo.parse_tb.json_data, path=>'pages') AS pages;

SELECT * FROM snowvill.mintsuyo.flatten_tb;



-- 3. Cortex Searchを作成する
-- CREATE OR REPLACE CORTEX SEARCH SERVICE snowvill.mintsuyo.mintsuyo_search
-- ON content
-- ATTRIBUTES (FILE_NAME, LAST_MODIFIED, PAGECOUNT)
-- WAREHOUSE = 'SNOWSIGHT_WH'
-- TARGET_LAG = '365 days'
-- AS (
--     SELECT 
--         content,
--         file_name,
--         last_modified,
--         pagecount
--     FROM 
--         snowvill.mintsuyo.flatten_tb
-- );
