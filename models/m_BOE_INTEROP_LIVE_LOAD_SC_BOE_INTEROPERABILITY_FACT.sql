/ * Transformation Name ==>SC_BOE_INTEROPERABILITY_FACT ,Transformation Type ==>Target * /

{{
    config(
        materialized='incremental',
        alias='BOE_INTEROPERABILITY',
        schema='EMIR_SHARED',
        pre_hook="",
        post_hook="",
        incremental_strategy='append'
    )
}}

WITH SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACTOut AS (
    /* SubQuery from Source ==>SQ_SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACT */
    SELECT
        BUSINESS_DATE,
        EXCHANGE,
        INTEROP_ID,
        INTEROP_DESC,
        EXPOSURE_AMT,
        CURRENCY_MNEMONIC,
        Coll_Type,
        FinInstrmId,
        issr,
        Trpty_MktVal_AMT,
        Trpty_MktVal_CCY,
        MktVal_AMT,
        PstHrcutVal_AMT,
        PstHrcutVal_CCY,
        CollRqrmnt
    FROM (
        SELECT DISTINCT
            DATE_TRUNC('DAY', a.BUSINESS_DATE) AS BUSINESS_DATE,
            d.STRING_VALUE3 AS EXCHANGE,
            d.STRING_VALUE5 AS INTEROP_ID,
            d.STRING_VALUE4 AS INTEROP_DESC,
            a.EXPOSURE_AMT AS EXPOSURE_AMT,
            c.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC,
            NULL AS Coll_Type,
            NULL AS FinInstrmId,
            NULL AS issr,
            NULL AS Trpty_MktVal_AMT,
            NULL AS Trpty_MktVal_CCY,
            NULL AS MktVal_AMT,
            NULL AS PstHrcutVal_AMT,
            NULL AS PstHrcutVal_CCY,
            'MGIN' AS CollRqrmnt
        FROM (
            SELECT
                BUSINESS_DATE,
                COLLATERAL_REC_SHORT_CODE,
                EXPOSURE_AMT,
                BASKET_NUMBER,
                EXPOSURE_REF,
                EXPOSURE_CURR_ID
            FROM {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            WHERE BUSINESS_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              AND COLLATERAL_GIVER_SHORT_CODE = 'LCHGIVER'
        ) a
        INNER JOIN (
            SELECT
                INSTRUMENT_GROUP,
                ISIN,
                SECURITY_VALUE,
                EXPOSURE_REF
            FROM {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            WHERE BUSINESS_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              AND COLLATERAL_GIVER_SHORT_CODE IS NULL
        ) b
            ON a.EXPOSURE_REF = b.EXPOSURE_REF
        INNER JOIN dw_mart.currency_dim AS c
            ON c.ID = a.EXPOSURE_CURR_ID
        INNER JOIN application_variables AS d
            ON a.COLLATERAL_REC_SHORT_CODE = d.STRING_VALUE2
           AND d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
           AND 'Y' != (
               SELECT STRING_VALUE
               FROM APPLICATION_VARIABLES
               WHERE VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
           )
        UNION
        SELECT DISTINCT
            DATE_TRUNC('DAY', a.BUSINESS_DATE) AS BUSINESS_DATE,
            d.STRING_VALUE3 AS EXCHANGE,
            d.STRING_VALUE5 AS INTEROP_ID,
            d.STRING_VALUE4 AS INTEROP_DESC,
            NULL AS EXPOSURE_AMT,
            NULL AS CURRENCY_MNEMONIC,
            b.INSTRUMENT_GROUP AS Coll_Type,
            TO_VARCHAR(b.ISIN) AS FinInstrmId,
            '00000000000000000000' AS issr,
            MAX(a.EXPOSURE_AMT) AS Trpty_MktVal_AMT,
            c.CURRENCY_MNEMONIC AS Trpty_MktVal_CCY,
            SUM((1 || b.Margin_Percent) * b.SECURITY_VALUE) AS MktVal_AMT,
            SUM(b.SECURITY_VALUE) AS PstHrcutVal_AMT,
            c.CURRENCY_MNEMONIC AS PstHrcutVal_CCY,
            'MGIN' AS CollRqrmnt
        FROM (
            SELECT
                BUSINESS_DATE,
                COLLATERAL_REC_SHORT_CODE,
                EXPOSURE_AMT,
                BASKET_NUMBER,
                EXPOSURE_REF,
                EXPOSURE_CURR_ID
            FROM {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            WHERE BUSINESS_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              AND COLLATERAL_GIVER_SHORT_CODE = 'LCHGIVER'
        ) a
        INNER JOIN (
            SELECT
                INSTRUMENT_GROUP,
                ISIN,
                SECURITY_VALUE,
                EXPOSURE_REF,
                MARGIN_PERCENT
            FROM {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            WHERE BUSINESS_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              AND COLLATERAL_GIVER_SHORT_CODE IS NULL
        ) b
            ON a.EXPOSURE_REF = b.EXPOSURE_REF
        INNER JOIN dw_mart.currency_dim AS c
            ON c.ID = a.EXPOSURE_CURR_ID
        INNER JOIN application_variables AS d
            ON a.COLLATERAL_REC_SHORT_CODE = d.STRING_VALUE2
           AND d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
           AND 'Y' != (
               SELECT STRING_VALUE
               FROM APPLICATION_VARIABLES
               WHERE VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
           )
        GROUP BY
            DATE_TRUNC('DAY', a.BUSINESS_DATE),
            d.STRING_VALUE3,
            d.STRING_VALUE5,
            d.STRING_VALUE4,
            b.INSTRUMENT_GROUP,
            TO_VARCHAR(b.ISIN),
            c.CURRENCY_MNEMONIC
        UNION
        SELECT DISTINCT
            DATE_TRUNC('DAY', a.BUSINESS_DATE) AS BUSINESS_DATE,
            STRING_VALUE3 AS EXCHANGE,
            STRING_VALUE5 AS INTEROP_ID,
            STRING_VALUE4 AS INTEROP_DESC,
            TRANSACTION_AMT AS EXPOSURE_AMT,
            TRANSACTION_CCY AS CURRENCY_MNEMONIC,
            NULL AS Coll_Type,
            NULL AS FinInstrmId,
            NULL AS issr,
            NULL AS Trpty_MktVal_AMT,
            NULL AS Trpty_MktVal_CCY,
            NULL AS MktVal_AMT,
            NULL AS PstHrcutVal_AMT,
            NULL AS PstHrcutVal_CCY,
            'MGIN' AS CollRqrmnt
        FROM {{ source('reg_rep', 'RR_BNK_TRIPTYALC_INVST_FACT') }} AS a
        INNER JOIN application_variables AS d
            ON a.MNEMONIC = d.STRING_VALUE
        WHERE d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          AND BUSINESS_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
          AND 'Y' = (
              SELECT STRING_VALUE
              FROM APPLICATION_VARIABLES
              WHERE VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
          )
        UNION
        SELECT DISTINCT
            DATE_TRUNC('DAY', a.BUSINESS_DATE) AS BUSINESS_DATE,
            STRING_VALUE3 AS EXCHANGE,
            STRING_VALUE5 AS INTEROP_ID,
            STRING_VALUE4 AS INTEROP_DESC,
            NULL AS EXPOSURE_AMT,
            NULL AS CURRENCY_MNEMONIC,
            COLLATERAL_GROUP AS Coll_Type,
            TO_VARCHAR(a.ISIN) AS FinInstrmId,
            '00000000000000000000' AS issr,
            TRANSACTION_AMT AS Trpty_MktVal_AMT,
            TRANSACTION_CCY AS Trpty_MktVal_CCY,
            PRE_HC_MV_TX_CCY_AMT AS MktVal_AMT,
            POST_HC_MV_TX_CCY_AMT AS PstHrcutVal_AMT,
            TRANSACTION_CCY AS PstHrcutVal_CCY,
            'MGIN' AS CollRqrmnt
        FROM {{ source('reg_rep', 'RR_BNK_TRIPTYALC_INVST_FACT') }} AS a
        INNER JOIN application_variables AS d
            ON a.MNEMONIC = d.STRING_VALUE
        WHERE d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          AND BUSINESS_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
          AND 'Y' = (
              SELECT STRING_VALUE
              FROM APPLICATION_VARIABLES
              WHERE VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
          )
    )
),

SQ_SC_RR_POSITIONS_FACTOut AS (
    /* SubQuery from Source ==>SQ_SQ_SC_RR_POSITIONS_FACT */
    SELECT
        NOTIONAL_AMT,
        INTEROP_ID,
        INTEROP_DESC,
        CURRENCY_MNEMONIC
    FROM (
        SELECT
            SUM(DECODE(NVL(INSTRUMENTS_DIM.SECURITY_TYPE, 'NONTERM'), 'TERM', ABS(MTM_NOTIONAL), 0)) AS NOTIONAL_AMT,
            application_variables.STRING_VALUE5 AS INTEROP_ID,
            application_variables.STRING_VALUE4 AS INTEROP_DESC,
            CURRENCY_DIM.CURRENCY_MNEMONIC
        FROM {{ source('reg_rep', 'RR_POSITIONS_FACT') }}
        INNER JOIN INSTRUMENTS_DIM AS INSTRUMENTS_DIM
            ON RR_POSITIONS_FACT.INSTRUMENT_ID = INSTRUMENTS_DIM.ID
        INNER JOIN CURRENCY_DIM AS CURRENCY_DIM
            ON RR_POSITIONS_FACT.MARGIN_CURRENCY_ID = CURRENCY_DIM.ID
        INNER JOIN application_variables AS application_variables
            ON COUNTERPARTIES_DIM.CLEARING_MEMBER_MNEMONIC = APPLICATION_VARIABLES.STRING_VALUE5
        WHERE MEMBER_COUNTERPARTY_ID = COUNTERPARTIES_DIM.ID
          AND BUSINESS_DATE_ID = (
              SELECT DISTINCT ID
              FROM {{ source('dw_mart', 'CALENDAR_DIM') }}
              WHERE CAL_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
                AND TYPE = 'LTD_STD'
          )
          AND exchange_id IN (
              SELECT id
              FROM {{ source('dw_mart', 'exchange_dim') }}
              WHERE EXCHANGE_MNEMONIC = 'ECL'
          )
        GROUP BY
            application_variables.STRING_VALUE5,
            application_variables.STRING_VALUE4,
            CURRENCY_DIM.CURRENCY_MNEMONIC
    )
),

SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut AS (
    /* SubQuery from Source ==>SQ_SQ_SC_RR_TRADE_AND_TRANSACTION_FACT */
    SELECT
        BUSINESS_DATE_TIME,
        TrdsClrd,
        INTEROP_ID,
        INTEROP_DESC
    FROM (
        SELECT
            a.BUSINESS_DATE_TIME,
            ROUND(COUNT(a.ID) / 2) AS TrdsClrd,
            d.STRING_VALUE5 AS INTEROP_ID,
            d.STRING_VALUE4 AS INTEROP_DESC
        FROM {{ source('reg_rep', 'RR_TRADE_AND_TRANSACTION_FACT') }} AS a
        INNER JOIN COUNTERPARTIES_DIM AS b
            ON a.MEMBER_COUNTERPARTY_ID = b.ID
        INNER JOIN application_variables AS d
            ON b.CLEARING_MEMBER_MNEMONIC = d.STRING_VALUE5
        WHERE d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          AND a.exchange_id IN (
              SELECT id
              FROM {{ source('dw_mart', 'exchange_dim') }}
              WHERE EXCHANGE_MNEMONIC = 'ECL'
          )
          AND TRADE_ACTION_TYPE = 'NEW'
          AND BUSINESS_DATE_ID = (
              SELECT DISTINCT ID
              FROM {{ source('dw_mart', 'CALENDAR_DIM') }}
              WHERE CAL_DATE = TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
                AND TYPE = 'LTD_STD'
          )
        GROUP BY
            a.BUSINESS_DATE_TIME,
            d.STRING_VALUE5,
            d.STRING_VALUE4
    )
),

SQ_SC_RR_POSITIONS_FACT_master AS (
    SELECT
        SQ_SC_RR_POSITIONS_FACTOut.NOTIONAL_AMT AS NOTIONAL_AMT,
        SQ_SC_RR_POSITIONS_FACTOut.INTEROP_ID AS INTEROP_ID1,
        SQ_SC_RR_POSITIONS_FACTOut.INTEROP_DESC AS INTEROP_DESC1,
        SQ_SC_RR_POSITIONS_FACTOut.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC
    FROM SQ_SC_RR_POSITIONS_FACTOut AS SQ_SC_RR_POSITIONS_FACTOut
),

JNR_gross_CLRIDOut AS (
    SELECT
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.BUSINESS_DATE_TIME AS BUSINESS_DATE_TIME,
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.TrdsClrd AS TrdsClrd,
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.INTEROP_ID AS INTEROP_ID,
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.INTEROP_DESC AS INTEROP_DESC,
        SQ_SC_RR_POSITIONS_FACT_master.NOTIONAL_AMT AS NOTIONAL_AMT,
        SQ_SC_RR_POSITIONS_FACT_master.INTEROP_ID1 AS INTEROP_ID1,
        SQ_SC_RR_POSITIONS_FACT_master.INTEROP_DESC1 AS INTEROP_DESC1,
        SQ_SC_RR_POSITIONS_FACT_master.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC,
        ROW_NUMBER() OVER (ORDER BY 1) AS jkey
    FROM SQ_SC_RR_POSITIONS_FACT_master AS SQ_SC_RR_POSITIONS_FACT_master
    INNER JOIN SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut AS SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut
        ON SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.INTEROP_ID = SQ_SC_RR_POSITIONS_FACT_master.INTEROP_ID1
),

exp_TTL_SECURITYOut AS (
    SELECT
        BUSINESS_DATE AS BUSINESS_DATE,
        NULL AS Trdsclrd,
        EXCHANGE AS EXCHANGE,
        INTEROP_ID AS INTEROP_ID,
        INTEROP_DESC AS INTEROP_DESC,
        EXPOSURE_AMT AS EXPOSURE_AMT,
        CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC,
        Coll_Type AS Coll_Type,
        FinInstrmId AS FinInstrmId,
        issr AS issr,
        Trpty_MktVal_AMT AS Trpty_MktVal_AMT,
        Trpty_MktVal_CCY AS Trpty_MktVal_CCY,
        MktVal_AMT AS MktVal_AMT,
        NULL AS NOTIONAL_AMT,
        PstHrcutVal_AMT AS PstHrcutVal_AMT,
        PstHrcutVal_CCY AS PstHrcutVal_CCY,
        CollRqrmnt AS CollRqrmnt,
        NULL AS GROSS_NOTIONAL_CURRENCY
    FROM SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACTOut AS SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACTOut
),

DSR_exp_TTL_SECURITY1Out AS (
    SELECT
        JNR_gross_CLRIDOut.BUSINESS_DATE_TIME AS BUSINESS_DATE,
        JNR_gross_CLRIDOut.TrdsClrd AS Trdsclrd,
        JNR_gross_CLRIDOut.INTEROP_ID AS INTEROP_ID,
        JNR_gross_CLRIDOut.INTEROP_DESC AS INTEROP_DESC,
        JNR_gross_CLRIDOut.NOTIONAL_AMT AS NOTIONAL_AMT,
        JNR_gross_CLRIDOut.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC2
    FROM JNR_gross_CLRIDOut AS JNR_gross_CLRIDOut
),

exp_TTL_SECURITY1Out AS (
    SELECT
        BUSINESS_DATE AS BUSINESS_DATE,
        Trdsclrd AS Trdsclrd,
        'ECL' AS EXCHANGE,
        INTEROP_ID AS INTEROP_ID,
        INTEROP_DESC AS INTEROP_DESC,
        NULL AS EXPOSURE_AMT,
        NULL AS CURRENCY_MNEMONIC,
        NULL AS Coll_Type,
        NULL AS FinInstrmId,
        '00000000000000000000' AS issr,
        NULL AS Trpty_MktVal_AMT,
        NULL AS Trpty_MktVal_CCY,
        NULL AS MktVal_AMT,
        NOTIONAL_AMT AS NOTIONAL_AMT,
        NULL AS PstHrcutVal_AMT,
        CURRENCY_MNEMONIC2 AS CURRENCY_MNEMONIC2,
        NULL AS CURRENCY_MNEMONIC1,
        NULL AS PstHrcutVal_CCY,
        'Margin' AS ColRqmnt
    FROM DSR_exp_TTL_SECURITY1Out AS DSR_exp_TTL_SECURITY1Out
),

Union_ALL_DATEOut AS (
    SELECT
        BUSINESS_DATE,
        Trdsclrd,
        EXCHANGE,
        INTEROP_ID,
        INTEROP_DESC,
        EXPOSURE_AMT,
        CURRENCY_MNEMONIC,
        Coll_Type,
        FinInstrmId,
        issr,
        Trpty_MktVal_AMT,
        Trpty_MktVal_CCY,
        MktVal_AMT,
        NOTIONAL_AMT,
        PstHrcutVal_AMT,
        PstHrcutVal_CCY,
        CollRqrmnt,
        GROSS_NOTIONAL_CURRENCY
    FROM (
        SELECT
            exp_TTL_SECURITYOut.BUSINESS_DATE AS BUSINESS_DATE1,
            exp_TTL_SECURITYOut.Trdsclrd AS Trdsclrd1,
            exp_TTL_SECURITYOut.EXCHANGE AS EXCHANGE1,
            exp_TTL_SECURITYOut.INTEROP_ID AS INTEROP_ID1,
            exp_TTL_SECURITYOut.INTEROP_DESC AS INTEROP_DESC1,
            exp_TTL_SECURITYOut.EXPOSURE_AMT AS EXPOSURE_AMT1,
            exp_TTL_SECURITYOut.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC1,
            exp_TTL_SECURITYOut.Coll_Type AS Coll_Type1,
            exp_TTL_SECURITYOut.FinInstrmId AS FinInstrmId1,
            exp_TTL_SECURITYOut.issr AS issr1,
            exp_TTL_SECURITYOut.Trpty_MktVal_AMT AS Trpty_MktVal_AMT1,
            exp_TTL_SECURITYOut.Trpty_MktVal_CCY AS Trpty_MktVal_CCY1,
            exp_TTL_SECURITYOut.MktVal_AMT AS MktVal_AMT1,
            exp_TTL_SECURITYOut.NOTIONAL_AMT AS NOTIONAL_AMT1,
            exp_TTL_SECURITYOut.PstHrcutVal_AMT AS PstHrcutVal_AMT1,
            exp_TTL_SECURITYOut.PstHrcutVal_CCY AS PstHrcutVal_CCY1,
            exp_TTL_SECURITYOut.CollRqrmnt AS CollRqrmnt1,
            exp_TTL_SECURITYOut.GROSS_NOTIONAL_CURRENCY AS GROSS_NOTIONAL_CURRENCY2
        FROM exp_TTL_SECURITYOut AS exp_TTL_SECURITYOut
        UNION ALL
        SELECT
            exp_TTL_SECURITY1Out.BUSINESS_DATE AS BUSINESS_DATE2,
            exp_TTL_SECURITY1Out.Trdsclrd AS Trdsclrd2,
            exp_TTL_SECURITY1Out.EXCHANGE AS EXCHANGE2,
            exp_TTL_SECURITY1Out.INTEROP_ID AS INTEROP_ID2,
            exp_TTL_SECURITY1Out.INTEROP_DESC AS INTEROP_DESC2,
            exp_TTL_SECURITY1Out.EXPOSURE_AMT AS EXPOSURE_AMT2,
            exp_TTL_SECURITY1Out.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC2,
            exp_TTL_SECURITY1Out.Coll_Type AS Coll_Type2,
            exp_TTL_SECURITY1Out.FinInstrmId AS FinInstrmId2,
            exp_TTL_SECURITY1Out.issr AS issr2,
            exp_TTL_SECURITY1Out.Trpty_MktVal_AMT AS Trpty_MktVal_AMT2,
            exp_TTL_SECURITY1Out.Trpty_MktVal_CCY AS Trpty_MktVal_CCY2,
            exp_TTL_SECURITY1Out.MktVal_AMT AS MktVal_AMT2,
            exp_TTL_SECURITY1Out.NOTIONAL_AMT AS NOTIONAL_AMT2,
            exp_TTL_SECURITY1Out.PstHrcutVal_AMT AS PstHrcutVal_AMT2,
            exp_TTL_SECURITY1Out.CURRENCY_MNEMONIC1 AS PstHrcutVal_CCY2,
            exp_TTL_SECURITY1Out.PstHrcutVal_CCY AS CollRqmnt2,
            exp_TTL_SECURITY1Out.CURRENCY_MNEMONIC2 AS GROSS_NOTIONAL_CURRENCY3
        FROM exp_TTL_SECURITY1Out AS exp_TTL_SECURITY1Out
    )
),

SC_SEQ_INTEROP1Out AS (
    SELECT
        Union_ALL_DATEOut.INTEROP_ID AS INTEROP_ID,
        Union_ALL_DATEOut.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC,
        Union_ALL_DATEOut.EXCHANGE AS EXCHANGE,
        Union_ALL_DATEOut.GROSS_NOTIONAL_CURRENCY AS GROSS_NOTIONAL_CURRENCY,
        Union_ALL_DATEOut.Trdsclrd AS Trdsclrd,
        Union_ALL_DATEOut.FinInstrmId AS FinInstrmId,
        Union_ALL_DATEOut.Trpty_MktVal_CCY AS Trpty_MktVal_CCY,
        Union_ALL_DATEOut.PstHrcutVal_AMT AS PstHrcutVal_AMT,
        Union_ALL_DATEOut.MktVal_AMT AS MktVal_AMT,
        Union_ALL_DATEOut.issr AS issr,
        Union_ALL_DATEOut.Coll_Type AS Coll_Type,
        Union_ALL_DATEOut.NOTIONAL_AMT AS NOTIONAL_AMT,
        Union_ALL_DATEOut.PstHrcutVal_CCY AS PstHrcutVal_CCY,
        Union_ALL_DATEOut.Trpty_MktVal_AMT AS Trpty_MktVal_AMT,
        Union_ALL_DATEOut.EXPOSURE_AMT AS EXPOSURE_AMT,
        Union_ALL_DATEOut.INTEROP_DESC AS INTEROP_DESC,
        Union_ALL_DATEOut.CollRqrmnt AS CollRqrmnt
    FROM Union_ALL_DATEOut AS Union_ALL_DATEOut
),

Target_Table_Name_SK_Max_Value AS (
    SELECT
        MAX(GENERATED_SEQ) AS max_generated_seq
    FROM {{ this }}
),

DSR_EXP_FINAL1Out AS (
    SELECT
        (SELECT max_generated_seq FROM Target_Table_Name_SK_Max_Value) + ROW_NUMBER() OVER () AS GENERATED_SEQ,
        SC_SEQ_INTEROP1Out.Trdsclrd AS Trdsclrd,
        SC_SEQ_INTEROP1Out.EXCHANGE AS EXCHANGE,
        SC_SEQ_INTEROP1Out.INTEROP_ID AS INTEROP_ID,
        SC_SEQ_INTEROP1Out.INTEROP_DESC AS INTEROP_DESC,
        SC_SEQ_INTEROP1Out.EXPOSURE_AMT AS EXPOSURE_AMT,
        SC_SEQ_INTEROP1Out.CURRENCY_MNEMONIC AS CURRENCY_MNEMONIC,
        SC_SEQ_INTEROP1Out.Coll_Type AS Coll_Type,
        SC_SEQ_INTEROP1Out.FinInstrmId AS FinInstrmId,
        SC_SEQ_INTEROP1Out.issr AS issr,
        SC_SEQ_INTEROP1Out.Trpty_MktVal_AMT AS Trpty_MktVal_AMT,
        SC_SEQ_INTEROP1Out.Trpty_MktVal_CCY AS Trpty_MktVal_CCY,
        SC_SEQ_INTEROP1Out.MktVal_AMT AS MktVal_AMT,
        SC_SEQ_INTEROP1Out.NOTIONAL_AMT AS NOTIONAL_AMT,
        SC_SEQ_INTEROP1Out.PstHrcutVal_AMT AS PstHrcutVal_AMT,
        SC_SEQ_INTEROP1Out.PstHrcutVal_CCY AS PstHrcutVal_CCY,
        SC_SEQ_INTEROP1Out.CollRqrmnt AS CollRqrmnt,
        SC_SEQ_INTEROP1Out.GROSS_NOTIONAL_CURRENCY AS GROSS_NOTIONAL_CURRENCY
    FROM SC_SEQ_INTEROP1Out AS SC_SEQ_INTEROP1Out
),

EXP_FINAL1Out AS (
    SELECT
        GENERATED_SEQ AS GENERATED_SEQ,
        LPAD(GENERATED_SEQ, 9, '0') AS v_SEQ_LPAD_TO_9,
        DATEDIFF(
            SECOND,
            TO_TIMESTAMP('01/01/1970 00:00:00', 'DD-MM-YYYY HH24:MI:SS'),
            CURRENT_TIMESTAMP()
        ) AS v_EPOC_TIME,
        v_EPOC_TIME || v_SEQ_LPAD_TO_9 AS v_CONCATE_ID,
        v_CONCATE_ID AS ID,
        '{{ var("BATCH_ID") }}' AS BATCH_ID,
        dsf_1.ID AS BATCH_DETAILS_ID,
        DATE_TRUNC('DAY', TO_DATE(LTRIM(RTRIM('{{ var("BATCH_BUSINESS_DATE") }}')), 'YYYYMMDD')) AS v_BATCH_BUSINESS_DATE,
        v_BATCH_BUSINESS_DATE AS BATCH_BUSINESS_DATE,
        dsf_2.ID AS BUSINESS_DATE_ID,
        v_TOT_REC_COUNT + 1 AS v_TOT_REC_COUNT,
        TO_DATE('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD') AS BUSINESS_DATE,
        Trdsclrd,
        EXCHANGE,
        INTEROP_ID,
        INTEROP_DESC,
        EXPOSURE_AMT,
        CURRENCY_MNEMONIC,
        Coll_Type,
        FinInstrmId,
        issr,
        Trpty_MktVal_AMT,
        Trpty_MktVal_CCY,
        MktVal_AMT,
        NOTIONAL_AMT,
        PstHrcutVal_AMT,
        PstHrcutVal_CCY,
        CollRqrmnt,
        NULL AS SUBMIT_FLAG,
        GROSS_NOTIONAL_CURRENCY,
        0 AS ERROR_INDICATOR
    FROM DSR_EXP_FINAL1Out AS DSR_EXP_FINAL1Out
    LEFT JOIN (
        SELECT ID, BATCH_ID, TARGET_TABLE_NAME, FILE_NAME
        FROM (
            SELECT ID, BATCH_ID, TARGET_TABLE_NAME, FILE_NAME,
                   ROW_NUMBER() OVER (PARTITION BY BATCH_ID, TARGET_TABLE_NAME, FILE_NAME ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT ID, BATCH_ID, TARGET_TABLE_NAME, FILE_NAME,
                       ROW_NUMBER() OVER (PARTITION BY BATCH_ID, TARGET_TABLE_NAME, FILE_NAME ORDER BY BATCH_ID, TARGET_TABLE_NAME, FILE_NAME NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        BATCH_DETAILS.ID AS ID,
                        BATCH_DETAILS.BATCH_ID AS BATCH_ID,
                        BATCH_DETAILS.TARGET_TABLE_NAME AS TARGET_TABLE_NAME,
                        BATCH_DETAILS.FILE_NAME AS FILE_NAME,
                        BATCH_DETAILS.STATUS AS STATUS
                    FROM (
                        SELECT
                            id,
                            TARGET_TABLE_NAME,
                            BATCH_ID,
                            STATUS,
                            ACTIVE_FLAG,
                            BATCH_DETAILS_START_TIME,
                            FILE_NAME,
                            RANK() OVER (PARTITION BY batch_id, TARGET_TABLE_NAME, FILE_NAME ORDER BY BATCH_DETAILS_START_TIME DESC) AS RANK
                        FROM BATCH_DETAILS
                    ) BATCH_DETAILS
                    WHERE RANK = 1
                      AND STATUS = 'I'
                      AND ACTIVE_FLAG = 1
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.BATCH_ID = '{{ var("BATCH_ID") }}'
       AND dsf_1.TARGET_TABLE_NAME = '{{ var("TARGET_NAME") }}'
       AND dsf_1.FILE_NAME = '{{ var("SOURCE_NAME") }}'
    LEFT JOIN (
        SELECT ID, CAL_DATE, TYPE
        FROM (
            SELECT ID, CAL_DATE, TYPE,
                   ROW_NUMBER() OVER (PARTITION BY CAL_DATE, TYPE ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT ID, CAL_DATE, TYPE,
                       ROW_NUMBER() OVER (PARTITION BY CAL_DATE, TYPE ORDER BY CAL_DATE, TYPE NULLS LAST) AS rnk_fst
                FROM CALENDAR_DIM
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.CAL_DATE = 'LTD_STD'
       AND dsf_2.TYPE = v_BATCH_BUSINESS_DATE
),

DSR_SC_BOE_INTEROPERABILITY_FACTOut AS (
    SELECT
        EXP_FINAL1Out.ID AS ID,
        EXP_FINAL1Out.BATCH_ID AS BATCH_ID,
        EXP_FINAL1Out.BUSINESS_DATE AS BUSINESS_DATE,
        EXP_FINAL1Out.BUSINESS_DATE_ID AS BUSINESS_DATE_ID,
        EXP_FINAL1Out.BATCH_DETAILS_ID AS BATCH_DETAILS_ID,
        EXP_FINAL1Out.EXCHANGE AS EXCHANGE,
        EXP_FINAL1Out.INTEROP_ID AS INTEROP_ID,
        EXP_FINAL1Out.INTEROP_DESC AS INTEROP_DESC,
        EXP_FINAL1Out.EXPOSURE_AMT AS TtlInitlMrgn_AMT,
        EXP_FINAL1Out.CURRENCY_MNEMONIC AS TtlInitlMrgn_CCY,
        EXP_FINAL1Out.Trdsclrd AS TrdsClrd,
        EXP_FINAL1Out.NOTIONAL_AMT AS GrssNtnlAmt_AMT,
        EXP_FINAL1Out.GROSS_NOTIONAL_CURRENCY AS GrssNtnlAmt_CCY,
        EXP_FINAL1Out.PstHrcutVal_AMT AS PstHrcutVal_AMT,
        EXP_FINAL1Out.PstHrcutVal_CCY AS PstHrcutVal_CCY,
        EXP_FINAL1Out.Trpty_MktVal_AMT AS Trpty_MktVal_AMT,
        EXP_FINAL1Out.Trpty_MktVal_CCY AS Trpty_MktVal_CCY,
        EXP_FINAL1Out.Coll_Type AS Coll_Type,
        EXP_FINAL1Out.FinInstrmId AS FinInstrmId,
        EXP_FINAL1Out.issr AS issr,
        EXP_FINAL1Out.MktVal_AMT AS MktVal_AMT,
        EXP_FINAL1Out.Trpty_MktVal_CCY AS MktVal_CCY,
        EXP_FINAL1Out.CollRqrmnt AS CollRqrmnt,
        EXP_FINAL1Out.SUBMIT_FLAG AS SUBMIT_FLAG,
        EXP_FINAL1Out.ERROR_INDICATOR AS ERROR_INDICATOR
    FROM EXP_FINAL1Out AS EXP_FINAL1Out
)

SELECT
    ID AS ID,
    BATCH_ID AS BATCH_ID,
    BUSINESS_DATE AS BUSINESS_DATE,
    BUSINESS_DATE_ID AS BUSINESS_DATE_ID,
    BATCH_DETAILS_ID AS BATCH_DETAILS_ID,
    EXCHANGE AS EXCHANGE,
    INTEROP_ID AS INTEROP_ID,
    INTEROP_DESC AS INTEROP_DESC,
    TtlInitlMrgn_AMT AS TtlInitlMrgn_AMT,
    TtlInitlMrgn_CCY AS TtlInitlMrgn_CCY,
    TrdsClrd AS TrdsClrd,
    GrssNtnlAmt_AMT AS GrssNtnlAmt_AMT,
    GrssNtnlAmt_CCY AS GrssNtnlAmt_CCY,
    PstHrcutVal_AMT AS PstHrcutVal_AMT,
    PstHrcutVal_CCY AS PstHrcutVal_CCY,
    Trpty_MktVal_AMT AS Trpty_MktVal_AMT,
    Trpty_MktVal_CCY AS Trpty_MktVal_CCY,
    Coll_Type AS Coll_Type,
    FinInstrmId AS FinInstrmId,
    issr AS issr,
    MktVal_AMT AS MktVal_AMT,
    MktVal_CCY AS MktVal_CCY,
    CollRqrmnt AS CollRqrmnt,
    SUBMIT_FLAG AS SUBMIT_FLAG,
    ERROR_INDICATOR AS ERROR_INDICATOR
FROM DSR_SC_BOE_INTEROPERABILITY_FACTOut AS DSR_SC_BOE_INTEROPERABILITY_FACTOut