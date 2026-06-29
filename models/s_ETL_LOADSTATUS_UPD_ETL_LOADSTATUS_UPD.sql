{{ 
    config(
        materialized='incremental',
        alias='tMSSqlRow_1',
        incremental_strategy='append'
    ) 
}}

WITH updated_rows AS (
    UPDATE ETL_LOADSTATUS
    SET
        END_TIME = CURRENT_TIMESTAMP(),
        STATUS = 'COMPLETED',
        AUD_UPD_BY = 'ETL-Update',
        AUD_UPD_DTTM = CURRENT_TIMESTAMP(),
        SRC_RECORDSCOUNT = {{ var('src_recordscount') }},
        LOAD_DURATION = NULL,
        INS_TGT_RECORDSCOUNT = {{ var('ins_tgt_recordscount') }},
        UPD_TGT_RECORDSCOUNT = {{ var('upd_tgt_recordscount') }},
        DEL_TGT_RECORDSCOUNT = NULL
    WHERE
        STATUS = 'PROCESSING'
        AND PROCESS = 'SRVC_WH'
        AND TGT_PLATFORM = 'CR1SDWSQLPRD001'
        AND TGT_TBL = 'VERINT_ACTV_MAP'
        AND ETL_BATCH_ID = {{ var('etl_batch_id') }}
    RETURNING *
)

SELECT
    *
FROM
    updated_rows;