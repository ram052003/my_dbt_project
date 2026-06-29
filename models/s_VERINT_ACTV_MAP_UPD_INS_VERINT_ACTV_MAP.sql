{{
    config(
        materialized='incremental',
        alias='VERINT_ACTV_MAP',
        schema='',
        pre_hook="",
        post_hook="",
        incremental_strategy='append'
    )
}}

with VERINT_ACTV_MAP_UPD_INSOut as (
    select
        ACTV_MAP_ID,
        SOR_CD,
        EFF_DTTM,
        END_DTTM,
        UNQ_KEY_TXT,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY,
        AUD_CRE_BY_NM,
        AUD_CRE_DTTM,
        ETL_BATCH_ID,
        CURR_IND
    from {{ source('tFileInputDelimited_2', var('TEMP_DIR') ~ 'VERINT_ACTV_MAP_UPD_INS') }}
),

VERINT_ACTV_MAP_INSOut as (
    select
        ACTV_MAP_ID,
        SOR_CD,
        EFF_DTTM,
        END_DTTM,
        UNQ_KEY_TXT,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY,
        AUD_CRE_BY_NM,
        AUD_CRE_DTTM,
        ETL_BATCH_ID,
        CURR_IND
    from {{ source('tFileInputDelimited_3', var('TEMP_DIR') ~ 'VERINT_ACTV_MAP_INS') }}
),

tUnite_1Out as (
    select
        ACTV_MAP_ID,
        SOR_CD,
        EFF_DTTM,
        END_DTTM,
        UNQ_KEY_TXT,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY,
        AUD_CRE_BY_NM,
        AUD_CRE_DTTM,
        ETL_BATCH_ID,
        CURR_IND
    from VERINT_ACTV_MAP_UPD_INSOut

    union all

    select
        ACTV_MAP_ID,
        SOR_CD,
        EFF_DTTM,
        END_DTTM,
        UNQ_KEY_TXT,
        ACTV_ID,
        MAPPED_ACTV_ID,
        MOD_BY,
        AUD_CRE_BY_NM,
        AUD_CRE_DTTM,
        ETL_BATCH_ID,
        CURR_IND
    from VERINT_ACTV_MAP_INSOut
),

tMap_2Out as (
    select
        row6.ACTV_MAP_ID as ACTV_MAP_ID,
        row6.SOR_CD as SOR_CD,
        row6.EFF_DTTM as EFF_DTTM,
        row6.END_DTTM as END_DTTM,
        row6.UNQ_KEY_TXT as UNQ_KEY_TXT,
        row6.ACTV_ID as ACTV_ID,
        row6.MAPPED_ACTV_ID as MAPPED_ACTV_ID,
        nullif(row6.MOD_BY, '') as MOD_BY,
        row6.AUD_CRE_BY_NM as AUD_CRE_BY_NM,
        row6.AUD_CRE_DTTM as AUD_CRE_DTTM,
        row6.ETL_BATCH_ID as ETL_BATCH_ID,
        row6.CURR_IND as CURR_IND
    from tUnite_1Out as row6
)

select
    ACTV_MAP_ID,
    SOR_CD,
    EFF_DTTM,
    END_DTTM,
    UNQ_KEY_TXT,
    ACTV_ID,
    MAPPED_ACTV_ID,
    MOD_BY,
    AUD_CRE_BY_NM,
    AUD_CRE_DTTM,
    ETL_BATCH_ID,
    CURR_IND
from tMap_2Out