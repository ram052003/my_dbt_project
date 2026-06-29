{{
  config(
    materialized='incremental',
    alias='VERINT_ACTV_MAP',
    schema='',
    pre_hook="",
    post_hook="",
    incremental_strategy='merge',
    unique_key=['ACTV_MAP_ID','EFF_DTTM'],
    merge_update_columns=['END_DTTM','AUD_UPD_BY_NM','AUD_UPD_DTTM','CURR_IND']
  )
}}

WITH VERINT_ACTV_MAPOut AS (
    SELECT
        ACTV_MAP_ID,
        EFF_DTTM,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY
    FROM {{ ref('VERINT_ACTV_MAP') }}
    WHERE CURR_IND = 'Y'
),

CURRENT_TIMESTAMPOut AS (
    SELECT
        TO_VARCHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYY-MM-DD HH24:MI:SS.FF3') AS UPDT_END_DTTM,
        TO_VARCHAR(DATEADD(SECOND, 1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())), 'YYYY-MM-DD HH24:MI:SS.FF3') AS EFF_DTTM,
        TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS.FF3') AS AUD_CRE_DTTM
),

ACTIVITYMAPPINGOut AS (
    SELECT
        ID AS ACTV_MAP_ID,
        ACTIVITYID AS ACTV_ID,
        MAPPEDACTIVITYID AS MAPPED_ACTV_ID,
        MODIFIEDBY AS MOD_BY
    FROM {{ ref('ACTIVITYMAPPING') }}
),

LKP_CRCOut AS (
    SELECT
        ACTV_MAP_ID,
        EFF_DTTM,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY,
        MD5(CONCAT(
            COALESCE(ACTV_ID::VARCHAR, ''),
            '|',
            COALESCE(MAPPED_ACTV_ID::VARCHAR, ''),
            '|',
            COALESCE(MOD_BY::VARCHAR, '')
        )) AS CRC
    FROM VERINT_ACTV_MAPOut
),

tAddCRCRow_3_Lookup_LastMatchOut AS (
    SELECT
        DISTINCT ACTV_MAP_ID,
        LAST_VALUE(EFF_DTTM) OVER (PARTITION BY ACTV_MAP_ID ORDER BY ACTV_MAP_ID) AS EFF_DTTM,
        LAST_VALUE(ACTV_ID) OVER (PARTITION BY ACTV_MAP_ID ORDER BY ACTV_MAP_ID) AS ACTV_ID,
        LAST_VALUE(MAPPED_ACTV_ID) OVER (PARTITION BY ACTV_MAP_ID ORDER BY ACTV_MAP_ID) AS MAPPED_ACTV_ID,
        LAST_VALUE(MOD_BY) OVER (PARTITION BY ACTV_MAP_ID ORDER BY ACTV_MAP_ID) AS MOD_BY,
        LAST_VALUE(CRC) OVER (PARTITION BY ACTV_MAP_ID ORDER BY ACTV_MAP_ID) AS CRC
    FROM LKP_CRCOut
),

SRC_CRCOut AS (
    SELECT
        ACTV_MAP_ID,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY,
        MD5(CONCAT(
            COALESCE(ACTV_ID::VARCHAR, ''),
            '|',
            COALESCE(MAPPED_ACTV_ID::VARCHAR, ''),
            '|',
            COALESCE(MOD_BY::VARCHAR, '')
        )) AS CRC
    FROM ACTIVITYMAPPINGOut
),

tMap_1Out AS (
    SELECT
        lnk_src.CRC AS CRC_1,
        lnk_ref.CRC AS CRC_2,
        lnk_date.EFF_DTTM AS EFF_DTTM_1,
        lnk_ref.EFF_DTTM AS EFF_DTTM_2,
        lnk_src.ACTV_ID AS ACTV_ID,
        lnk_date.AUD_CRE_DTTM AS AUD_CRE_DTTM,
        lnk_src.MOD_BY AS MOD_BY,
        lnk_src.MAPPED_ACTV_ID AS MAPPED_ACTV_ID,
        lnk_src.ACTV_MAP_ID AS ACTV_MAP_ID,
        lnk_date.UPDT_END_DTTM AS UPDT_END_DTTM
    FROM SRC_CRCOut AS lnk_src
    CROSS JOIN CURRENT_TIMESTAMPOut AS lnk_date
    INNER JOIN tAddCRCRow_3_Lookup_LastMatchOut AS lnk_ref
        ON lnk_src.ACTV_MAP_ID = lnk_ref.ACTV_MAP_ID
),

lnk_updateOut AS (
    SELECT
        ACTV_MAP_ID,
        EFF_DTTM_1 AS EFF_DTTM,
        UPDT_END_DTTM AS END_DTTM,
        'ETL-UPDATE' AS AUD_UPD_BY_NM,
        AUD_CRE_DTTM AS AUD_UPD_DTTM,
        'N' AS CURR_IND
    FROM tMap_1Out
    WHERE CRC_1 <> CRC_2
)

SELECT
    ACTV_MAP_ID,
    EFF_DTTM,
    END_DTTM,
    AUD_UPD_BY_NM,
    AUD_UPD_DTTM,
    CURR_IND
FROM lnk_updateOut