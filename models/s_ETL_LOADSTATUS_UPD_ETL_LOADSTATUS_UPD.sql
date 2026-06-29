{{
    config(
        materialized='incremental',
        alias='tMSSqlRow_1',
        schema='',
        pre_hook="UPDATE ETL_LOADSTATUS SET END_TIME = CURRENT_TIMESTAMP, STATUS = 'COMPLETED', AUD_UPD_BY = 'ETL-Update', AUD_UPD_DTTM = CURRENT_TIMESTAMP, SRC_RECORDSCOUNT = {{ var('tMSSqlInput_1_NB_LINE') }}, LOAD_DURATION = NULL, INS_TGT_RECORDSCOUNT = {{ var('tMSSqlOutput_2_NB_LINE') }}, UPD_TGT_RECORDSCOUNT = {{ var('tMSSqlOutput_1_NB_LINE') }}, DEL_TGT_RECORDSCOUNT = NULL WHERE STATUS = 'PROCESSING' AND PROCESS = 'SRVC_WH' AND TGT_PLATFORM = 'CR1SDWSQLPRD001' AND TGT_TBL = 'VERINT_EMP' AND ETL_BATCH_ID = {{ var('ETL_BATCH_ID') }}",
        post_hook="",
        incremental_strategy='append'
    )
}}

WITH ETL_LOADSTATUS_UPD_sourceOut AS (
    SELECT
        *
    FROM
        ETL_LOADSTATUS
    WHERE
        STATUS = 'COMPLETED'
        AND PROCESS = 'SRVC_WH'
        AND TGT_PLATFORM = 'CR1SDWSQLPRD001'
        AND TGT_TBL = 'VERINT_EMP'
        AND ETL_BATCH_ID = {{ var('ETL_BATCH_ID') }}
),

df_ExpOut AS (
    SELECT
        *
    FROM
        ETL_LOADSTATUS_UPD_sourceOut AS ETL_LOADSTATUS_UPD_sourceOut
)

SELECT
    *
FROM
    df_ExpOut AS df_ExpOut