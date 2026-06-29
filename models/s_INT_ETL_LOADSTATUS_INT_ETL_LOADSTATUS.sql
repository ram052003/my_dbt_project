{{
    config(
        materialized='incremental',
        alias='ETL_LOADSTATUS',
        schema='',
        pre_hook="INSERT INTO {{ this }} VALUES ( 'SRVC_WH', CURRENT_TIMESTAMP(), 'DATA_GOVERNANCE', 'LOAD_VERINT_EMP_SWH', CURRENT_TIMESTAMP(), null, null, null, null, null, null, 'CR1SDWSQLPRD001', 'SRVC_WH', 'VERINT_EMP', 'TALEND', 'PROCESSING', 'ETL-Insert', CURRENT_TIMESTAMP(), null, null, 'SQL', {{ ETL_BATCH_ID }}, 'BPMAINDB' )",
        post_hook="",
        incremental_strategy='append'
    )
}}

WITH INT_ETL_LOADSTATUS_sourceOut AS (
    SELECT *
    FROM {{ ref('INT_ETL_LOADSTATUS_source') }}
),

df_ExpOut AS (
    SELECT *
    FROM INT_ETL_LOADSTATUS_sourceOut
)

SELECT *
FROM df_ExpOut