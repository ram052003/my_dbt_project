{{
    config(
        materialized='incremental',
        alias='ETL_LOADSTATUS',
        schema='',
        pre_hook="INSERT INTO {{ this }} VALUES ( 'SRVC_WH', GETDATE() AS ds__QueryNode_0, 'DATA_GOVERNANCE', 'LOAD_VERINT_ACTV_MAP_SWH', GETDATE() AS ds__QueryNode_0, null, null, null, null, null, null, 'CR1SDWSQLPRD001', 'SRVC_WH', 'VERINT_ACTV_MAP', 'TALEND', 'PROCESSING', 'ETL-Insert', GETDATE() AS ds__QueryNode_0, null, null, 'SQL', {{ ETL_BATCH_ID }}, 'BPMAINDB')",
        post_hook="",
        incremental_strategy='append'
    )
}}

WITH INT_ETL_LOADSTATUS_sourceOut AS (
    INSERT INTO {{ this }}
    VALUES (
        'SRVC_WH',
        CURRENT_TIMESTAMP() AS ds__QueryNode_0,
        'DATA_GOVERNANCE',
        'LOAD_VERINT_ACTV_MAP_SWH',
        CURRENT_TIMESTAMP() AS ds__QueryNode_0,
        null,
        null,
        null,
        null,
        null,
        null,
        'CR1SDWSQLPRD001',
        'SRVC_WH',
        'VERINT_ACTV_MAP',
        'TALEND',
        'PROCESSING',
        'ETL-Insert',
        CURRENT_TIMESTAMP() AS ds__QueryNode_0,
        null,
        null,
        'SQL',
        {{ ETL_BATCH_ID }},
        'BPMAINDB'
    )
    RETURNING *
),

df_ExpOut AS (
    SELECT *
    FROM INT_ETL_LOADSTATUS_sourceOut
)

SELECT *
FROM df_ExpOut