{{
  config(
    materialized='incremental',
    alias='VERINT_EMP',
    schema='',
    pre_hook="",
    post_hook="",
    incremental_strategy='merge',
    unique_key=['EMP_ID','EFF_DTTM'],
    merge_update_columns=['END_DTTM','AUD_UPD_BY_NM','AUD_UPD_DTTM','CURR_IND']
  )
}}

WITH CURRENT_TIMESTAMPOut AS (
    SELECT
        TO_VARCHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYY-MM-DD HH24:MI:SS.FF3') AS UPDT_END_DTTM,
        TO_VARCHAR(DATEADD(SECOND, 1, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())), 'YYYY-MM-DD HH24:MI:SS.FF3') AS EFF_DTTM,
        TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS.FF3') AS AUD_CRE_DTTM
),

EMPLOYEEAMOut AS (
    SELECT
        a.ID AS EMP_ID,
        a.EMPLOYEETYPEID AS EMP_TYPE_ID,
        a.ASSIGNEDPOINTS AS ASGN_PNT,
        a.EMPLOYEENUMBER AS EMP_NBR,
        a.STARTTIME AS EMP_STRT_DTTM,
        a.ENDTIME AS EMP_END_DTTM,
        a.CHANGECOUNTER AS CHG_CNTR,
        a.MODIFIEDBY AS MOD_BY,
        a.ISSUPERVISOR AS SPVSR_FLG,
        a.ISTEAMLEAD AS TEAM_LEAD_FLG,
        a.PREFERREDSTART AS PREF_STRT_FLG,
        b.FIRSTNAME AS FRST_NM,
        b.LASTNAME AS LAST_NM,
        b.MIDDLEINITIAL AS MDL_INITL,
        b.SUFFIX AS SUFX,
        b.BIRTHDATE AS BRTH_DT,
        c.USERNAME AS USR_NM,
        c.STATUS AS USR_STS,
        c.MODIFIEDBY AS PROFILE_MOD_BY,
        c.LASTMODIFIEDAT AS PROFILE_MOD_DTTM,
        c.FAILEDLOGINCOUNT AS FAIL_LOGIN_CNT,
        c.LASTLOGINTIME AS LAST_LOGIN_DTTM
    FROM dbo.EMPLOYEEAM AS a
    JOIN dbo.PERSON AS b
        ON a.PERSONID = b.ID
    LEFT JOIN dbo.BPUSER AS c
        ON a.ID = c.EMPLOYEEID
),

VERINT_EMPOut AS (
    SELECT
        EMP_ID,
        SOR_CD,
        EFF_DTTM,
        END_DTTM,
        UNQ_KEY_TXT,
        EMP_TYPE_ID,
        ASGN_PNT,
        EMP_NBR,
        EMP_STRT_DTTM,
        EMP_END_DTTM,
        CHG_CNTR,
        MOD_BY,
        SPVSR_FLG,
        TEAM_LEAD_FLG,
        PREF_STRT_FLG,
        FRST_NM,
        LAST_NM,
        MDL_INITL,
        SUFX,
        BRTH_DT,
        USR_NM,
        USR_STS,
        PROFILE_MOD_BY,
        PROFILE_MOD_DTTM,
        FAIL_LOGIN_CNT,
        LAST_LOGIN_DTTM
    FROM dbo.VERINT_EMP
    WHERE CURR_IND = 'Y'
),

LKP_CRCOut AS (
    SELECT
        EMP_ID,
        SOR_CD,
        EFF_DTTM,
        END_DTTM,
        UNQ_KEY_TXT,
        EMP_TYPE_ID,
        ASGN_PNT,
        EMP_NBR,
        EMP_STRT_DTTM,
        EMP_END_DTTM,
        CHG_CNTR,
        MOD_BY,
        SPVSR_FLG,
        TEAM_LEAD_FLG,
        PREF_STRT_FLG,
        FRST_NM,
        LAST_NM,
        MDL_INITL,
        SUFX,
        BRTH_DT,
        USR_NM,
        USR_STS,
        PROFILE_MOD_BY,
        PROFILE_MOD_DTTM,
        FAIL_LOGIN_CNT,
        LAST_LOGIN_DTTM,
        MD5(
            CONCAT(
                COALESCE(EMP_TYPE_ID::VARCHAR, ''),
                '|',
                COALESCE(ASGN_PNT::VARCHAR, ''),
                '|',
                COALESCE(EMP_NBR::VARCHAR, ''),
                '|',
                COALESCE(EMP_STRT_DTTM::VARCHAR, ''),
                '|',
                COALESCE(EMP_END_DTTM::VARCHAR, ''),
                '|',
                COALESCE(CHG_CNTR::VARCHAR, ''),
                '|',
                COALESCE(MOD_BY::VARCHAR, ''),
                '|',
                COALESCE(SPVSR_FLG::VARCHAR, ''),
                '|',
                COALESCE(TEAM_LEAD_FLG::VARCHAR, ''),
                '|',
                COALESCE(PREF_STRT_FLG::VARCHAR, ''),
                '|',
                COALESCE(FRST_NM::VARCHAR, ''),
                '|',
                COALESCE(LAST_NM::VARCHAR, ''),
                '|',
                COALESCE(MDL_INITL::VARCHAR, ''),
                '|',
                COALESCE(SUFX::VARCHAR, ''),
                '|',
                COALESCE(BRTH_DT::VARCHAR, ''),
                '|',
                COALESCE(USR_NM::VARCHAR, ''),
                '|',
                COALESCE(USR_STS::VARCHAR, ''),
                '|',
                COALESCE(PROFILE_MOD_BY::VARCHAR, ''),
                '|',
                COALESCE(PROFILE_MOD_DTTM::VARCHAR, ''),
                '|',
                COALESCE(FAIL_LOGIN_CNT::VARCHAR, ''),
                '|',
                COALESCE(LAST_LOGIN_DTTM::VARCHAR, '')
            )
        ) AS CRC
    FROM VERINT_EMPOut
),

tAddCRCRow_3_Lookup_LastMatchOut AS (
    SELECT DISTINCT
        EMP_ID,
        LAST_VALUE(SOR_CD) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS SOR_CD,
        LAST_VALUE(EFF_DTTM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS EFF_DTTM,
        LAST_VALUE(END_DTTM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS END_DTTM,
        LAST_VALUE(UNQ_KEY_TXT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS UNQ_KEY_TXT,
        LAST_VALUE(EMP_TYPE_ID) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS EMP_TYPE_ID,
        LAST_VALUE(ASGN_PNT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS ASGN_PNT,
        LAST_VALUE(EMP_NBR) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS EMP_NBR,
        LAST_VALUE(EMP_STRT_DTTM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS EMP_STRT_DTTM,
        LAST_VALUE(EMP_END_DTTM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS EMP_END_DTTM,
        LAST_VALUE(CHG_CNTR) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS CHG_CNTR,
        LAST_VALUE(MOD_BY) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS MOD_BY,
        LAST_VALUE(SPVSR_FLG) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS SPVSR_FLG,
        LAST_VALUE(TEAM_LEAD_FLG) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS TEAM_LEAD_FLG,
        LAST_VALUE(PREF_STRT_FLG) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS PREF_STRT_FLG,
        LAST_VALUE(FRST_NM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS FRST_NM,
        LAST_VALUE(LAST_NM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS LAST_NM,
        LAST_VALUE(MDL_INITL) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS MDL_INITL,
        LAST_VALUE(SUFX) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS SUFX,
        LAST_VALUE(BRTH_DT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS BRTH_DT,
        LAST_VALUE(USR_NM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS USR_NM,
        LAST_VALUE(USR_STS) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS USR_STS,
        LAST_VALUE(PROFILE_MOD_BY) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS PROFILE_MOD_BY,
        LAST_VALUE(PROFILE_MOD_DTTM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS PROFILE_MOD_DTTM,
        LAST_VALUE(FAIL_LOGIN_CNT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS FAIL_LOGIN_CNT,
        LAST_VALUE(LAST_LOGIN_DTTM) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS LAST_LOGIN_DTTM,
        LAST_VALUE(CRC) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID) AS CRC
    FROM LKP_CRCOut
),

SRC_CRCOut AS (
    SELECT
        EMP_ID,
        EMP_TYPE_ID,
        ASGN_PNT,
        EMP_NBR,
        EMP_STRT_DTTM,
        EMP_END_DTTM,
        CHG_CNTR,
        MOD_BY,
        SPVSR_FLG,
        TEAM_LEAD_FLG,
        PREF_STRT_FLG,
        FRST_NM,
        LAST_NM,
        MDL_INITL,
        SUFX,
        BRTH_DT,
        USR_NM,
        USR_STS,
        PROFILE_MOD_BY,
        PROFILE_MOD_DTTM,
        FAIL_LOGIN_CNT,
        LAST_LOGIN_DTTM,
        MD5(
            CONCAT(
                COALESCE(EMP_TYPE_ID::VARCHAR, ''),
                '|',
                COALESCE(ASGN_PNT::VARCHAR, ''),
                '|',
                COALESCE(EMP_NBR::VARCHAR, ''),
                '|',
                COALESCE(EMP_STRT_DTTM::VARCHAR, ''),
                '|',
                COALESCE(EMP_END_DTTM::VARCHAR, ''),
                '|',
                COALESCE(CHG_CNTR::VARCHAR, ''),
                '|',
                COALESCE(MOD_BY::VARCHAR, ''),
                '|',
                COALESCE(SPVSR_FLG::VARCHAR, ''),
                '|',
                COALESCE(TEAM_LEAD_FLG::VARCHAR, ''),
                '|',
                COALESCE(PREF_STRT_FLG::VARCHAR, ''),
                '|',
                COALESCE(FRST_NM::VARCHAR, ''),
                '|',
                COALESCE(LAST_NM::VARCHAR, ''),
                '|',
                COALESCE(MDL_INITL::VARCHAR, ''),
                '|',
                COALESCE(SUFX::VARCHAR, ''),
                '|',
                COALESCE(BRTH_DT::VARCHAR, ''),
                '|',
                COALESCE(USR_NM::VARCHAR, ''),
                '|',
                COALESCE(USR_STS::VARCHAR, ''),
                '|',
                COALESCE(PROFILE_MOD_BY::VARCHAR, ''),
                '|',
                COALESCE(PROFILE_MOD_DTTM::VARCHAR, ''),
                '|',
                COALESCE(FAIL_LOGIN_CNT::VARCHAR, ''),
                '|',
                COALESCE(LAST_LOGIN_DTTM::VARCHAR, '')
            )
        ) AS CRC
    FROM EMPLOYEEAMOut
),

tMap_1Out AS (
    SELECT
        src.EMP_NBR,
        src.ASGN_PNT,
        src.USR_STS,
        src.EMP_TYPE_ID,
        src.MOD_BY,
        src.FRST_NM,
        src.SPVSR_FLG,
        src.SUFX,
        ref.EMP_ID AS EMP_ID_2,
        src.EMP_ID AS EMP_ID_1,
        src.MDL_INITL,
        src.LAST_LOGIN_DTTM,
        src.FAIL_LOGIN_CNT,
        lnk_date.UPDT_END_DTTM,
        lnk_date.EFF_DTTM AS EFF_DTTM_1,
        src.BRTH_DT,
        ref.EFF_DTTM AS EFF_DTTM_2,
        src.TEAM_LEAD_FLG,
        src.PROFILE_MOD_BY,
        src.USR_NM,
        src.CRC AS CRC_1,
        ref.CRC AS CRC_2,
        src.EMP_STRT_DTTM,
        src.LAST_NM,
        src.PROFILE_MOD_DTTM,
        src.CHG_CNTR,
        lnk_date.AUD_CRE_DTTM,
        src.EMP_END_DTTM,
        src.PREF_STRT_FLG
    FROM SRC_CRCOut AS src
    CROSS JOIN CURRENT_TIMESTAMPOut AS lnk_date
    INNER JOIN tAddCRCRow_3_Lookup_LastMatchOut AS ref
        ON src.EMP_ID = ref.EMP_ID
),

lnk_updateOut AS (
    SELECT
        src.EMP_ID,
        src.EFF_DTTM,
        src.UPDT_END_DTTM AS END_DTTM,
        'ETL-UPDATE' AS AUD_UPD_BY_NM,
        src.AUD_CRE_DTTM AS AUD_UPD_DTTM,
        'N' AS CURR_IND
    FROM tMap_1Out AS src
    WHERE src.CRC_1 <> src.CRC_2
)

SELECT
    EMP_ID,
    EFF_DTTM,
    END_DTTM,
    AUD_UPD_BY_NM,
    AUD_UPD_DTTM,
    CURR_IND
FROM lnk_updateOut