/ * Transformation Name ==>SC_PROCESS_ERROR_DETAILS ,Transformation Type ==>Target * /

{{
    config(
        materialized='incremental',
        alias='PROCESS_ERROR_DETAILS',
        schema='LDSA_SHARED',
        pre_hook=[],
        post_hook=[],
        incremental_strategy='append'
    )
}}

WITH SQ_SC_LCHSA_ETD_MetricByAccount_CDR_Info_YYYYmmdd_YYYYmmddHH24missOut AS (
    SELECT
        REPORT_DATE,
        ACCOUNT_CODE,
        METRIC_NAME,
        CURRENCY,
        ACCOUNT_TYPE,
        METRIC_VALUE,
        CurrentlyProcessedFileName
    FROM {{ source('FlatFile_catalog_1', 'LCHSA_ETD_MetricByAccount_CDR_Info_YYYYmmdd_YYYYmmddHH24miss') }}
),

EXP_TRIMOut AS (
    SELECT
        CASE
            WHEN REPORT_DATE IS NOT NULL THEN TO_DATE(LTRIM(RTRIM(REPORT_DATE)), 'DD/MM/YYYY')
            ELSE RAISE_ERROR('REPORT_DATE is NULL from Source which is a mandatory field, hence aborting the session')
        END AS O_REPORT_DATE,
        LTRIM(RTRIM(ACCOUNT_CODE)) AS O_ACCOUNT_CODE,
        LTRIM(RTRIM(METRIC_NAME)) AS O_METRIC_NAME,
        LTRIM(RTRIM(CURRENCY)) AS O_CURRENCY,
        LTRIM(RTRIM(ACCOUNT_TYPE)) AS O_ACCOUNT_TYPE,
        LTRIM(RTRIM(METRIC_VALUE)) AS O_METRIC_VALUE,
        'LCH.SA' AS ENTITY_NAME,
        CurrentlyProcessedFileName AS CurrentlyProcessedFileName,
        CASE
            WHEN REGEXP_INSTR(CurrentlyProcessedFileName, 'DCL') > 0 THEN 'DCLSA'
            ELSE 'MATIF_MONEP'
        END AS DATA_SET_NAME
    FROM SQ_SC_LCHSA_ETD_MetricByAccount_CDR_Info_YYYYmmdd_YYYYmmddHH24missOut
),

DSR_EXP_PASS_THROUGHOut AS (
    SELECT
        O_REPORT_DATE AS BUSINESS_DATE,
        ENTITY_NAME,
        O_ACCOUNT_CODE AS ACCOUNT_CODE,
        O_METRIC_NAME AS METRIC_NAME,
        O_CURRENCY AS CURRENCY,
        O_ACCOUNT_TYPE AS ACCOUNT_TYPE,
        O_METRIC_VALUE AS METRIC_VALUE,
        DATA_SET_NAME
    FROM EXP_TRIMOut
),

Target_Table_Name_SK_Max_Value AS (
    SELECT
        MAX(GENERATED_SEQ) AS max_generated_seq
    FROM {{ this }}
),

DSR_SC_EXP_EPOC_GENERATOROut AS (
    SELECT
        (SELECT max_generated_seq FROM Target_Table_Name_SK_Max_Value) + ROW_NUMBER() OVER () AS GENERATED_SEQ
    FROM SC_SEQ_LD_SA_METRICS_BY_ACCOUNTOut
),

EXP_PASS_THROUGHOut AS (
    SELECT
        '{{ var("BATCH_ID") }}' AS BATCH_ID,
        dsf_1.ID AS BATCH_DETAILS_ID,
        BUSINESS_DATE,
        TO_TIMESTAMP('{{ var("BATCH_START_DATE") }}', 'DD-MON-YYYY HH24:MI:SS') AS BATCH_START_DATE,
        dsf_2.DIMENSION_KEY AS ENTITY_ID,
        ACCOUNT_CODE,
        METRIC_NAME,
        CURRENCY,
        ACCOUNT_TYPE,
        METRIC_VALUE,
        v_TOT_REC_COUNT + 1 AS v_TOT_REC_COUNT,
        DATA_SET_NAME
    FROM DSR_EXP_PASS_THROUGHOut
    LEFT JOIN (
        SELECT
            ID,
            BATCH_ID,
            TARGET_TABLE_NAME,
            FILE_NAME
        FROM (
            SELECT
                ID,
                BATCH_ID,
                TARGET_TABLE_NAME,
                FILE_NAME,
                ROW_NUMBER() OVER (PARTITION BY BATCH_ID, TARGET_TABLE_NAME, FILE_NAME ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ID,
                    BATCH_ID,
                    TARGET_TABLE_NAME,
                    FILE_NAME,
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
        ) lkp_inner
    ) lkp_outer
    WHERE rnk_lst = 1
) dsf_1
    ON dsf_1.BATCH_ID = '{{ var("BATCH_ID") }}'
   AND dsf_1.TARGET_TABLE_NAME = '{{ var("TARGET_NAME") }}'
   AND dsf_1.FILE_NAME = '{{ var("SOURCE_NAME_OVERRIDE") }}'
    LEFT JOIN (
        SELECT
            DIMENSION_KEY,
            NAME,
            EFFECTIVE_DATE,
            EXPIRATION_DATE
        FROM (
            SELECT
                DIMENSION_KEY,
                NAME,
                EFFECTIVE_DATE,
                EXPIRATION_DATE,
                ROW_NUMBER() OVER (PARTITION BY NAME, EFFECTIVE_DATE, EXPIRATION_DATE ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    DIMENSION_KEY,
                    NAME,
                    EFFECTIVE_DATE,
                    EXPIRATION_DATE,
                    ROW_NUMBER() OVER (PARTITION BY NAME, EFFECTIVE_DATE, EXPIRATION_DATE ORDER BY NAME, EFFECTIVE_DATE, EXPIRATION_DATE NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        ENTITY_DIM.DIMENSION_KEY AS DIMENSION_KEY,
                        ENTITY_DIM.NAME AS NAME,
                        ENTITY_DIM.EFFECTIVE_DATE AS EFFECTIVE_DATE,
                        NVL(ENTITY_DIM.EXPIRATION_DATE, TO_DATE('31-DEC-2099', 'DD-MON-YYYY')) AS EXPIRATION_DATE
                    FROM ENTITY_DIM
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
    ON dsf_2.NAME = ENTITY_NAME
   AND dsf_2.EFFECTIVE_DATE <= BUSINESS_DATE
   AND dsf_2.EXPIRATION_DATE >= BUSINESS_DATE
),

SC_EXP_EPOC_GENERATOROut AS (
    SELECT
        LPAD(GENERATED_SEQ, 9, '0') AS v_SEQ_LPAD_TO_9,
        DATEDIFF(
            SECOND,
            TO_TIMESTAMP('01/01/1970 00:00:00', 'DD-MM-YYYY HH24:MI:SS'),
            CURRENT_TIMESTAMP()
        ) AS v_EPOC_TIME,
        v_EPOC_TIME || v_SEQ_LPAD_TO_9 AS v_CONCATE_ID,
        v_CONCATE_ID AS ID,
        ROW_NUMBER() OVER (ORDER BY GENERATED_SEQ) AS jkey
    FROM DSR_SC_EXP_EPOC_GENERATOROut
),

EXP_FINALOut AS (
    SELECT
        BATCH_ID,
        BATCH_DETAILS_ID,
        BUSINESS_DATE,
        BATCH_START_DATE,
        ENTITY_ID,
        ACCOUNT_CODE,
        METRIC_NAME,
        CURRENCY,
        ACCOUNT_TYPE,
        METRIC_VALUE,
        '{{ var("PMWorkflowName", "") }}' AS WORKFLOW_NAME,
        '{{ var("PMMappingName", "") }}' AS MAPPING_NAME,
        '{{ var("TARGET_NAME") }}' AS TARGET_NAME,
        'E300_BLANK_FIELD_OR_LKUP_FAIL' AS VALIDATION_NAME,
        'NUMBER' AS DATA_TYPE_NUM,
        'STRING' AS DATA_TYPE_STRING,
        'DATE' AS DATA_TYPE_DATE,
        'Y' AS NUM_VAL_Y,
        'ENTITY_ID' AS ATTRIBUTE_NAME_ENTITY_ID,
        'ACCOUNT_CODE' AS ATTRIBUTE_NAME_ACCOUNT_CODE,
        'METRIC_NAME' AS ATTRIBUTE_NAME_METRIC_NAME,
        'CURRENCY' AS ATTRIBUTE_NAME_CURRENCY,
        'ACCOUNT_TYPE' AS ATTRIBUTE_NAME_ACCOUNT_TYPE,
        'METRIC_VALUE' AS ATTRIBUTE_NAME_METRIC_VALUE,
        DATA_SET_NAME
    FROM EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUEOut AS (
    SELECT
        EXP_FINALOut.VALIDATION_NAME AS in_VALIDATION_NAME,
        EXP_FINALOut.ATTRIBUTE_NAME_METRIC_VALUE AS in_ATTRIBUTE_NAME,
        EXP_FINALOut.DATA_TYPE_NUM AS in_DATA_TYPE,
        EXP_FINALOut.METRIC_VALUE AS in_ATTRIBUTE_TO_BE_VALIDATED_NUM
    FROM EXP_FINALOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_CURRENCYOut AS (
    SELECT
        EXP_FINALOut.VALIDATION_NAME AS in_VALIDATION_NAME,
        EXP_FINALOut.ATTRIBUTE_NAME_CURRENCY AS in_ATTRIBUTE_NAME,
        EXP_FINALOut.DATA_TYPE_STRING AS in_DATA_TYPE,
        EXP_FINALOut.CURRENCY AS in_ATTRIBUTE_TO_BE_VALIDATED_STR
    FROM EXP_FINALOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPEOut AS (
    SELECT
        EXP_FINALOut.VALIDATION_NAME AS in_VALIDATION_NAME,
        EXP_FINALOut.ATTRIBUTE_NAME_ACCOUNT_TYPE AS in_ATTRIBUTE_NAME,
        EXP_FINALOut.DATA_TYPE_STRING AS in_DATA_TYPE,
        EXP_FINALOut.ACCOUNT_TYPE AS in_ATTRIBUTE_TO_BE_VALIDATED_STR
    FROM EXP_FINALOut
),

DSR_EXP_LDSA_VALIDATION_METRIC_NAMEOut AS (
    SELECT
        EXP_FINALOut.VALIDATION_NAME AS in_VALIDATION_NAME,
        EXP_FINALOut.ATTRIBUTE_NAME_METRIC_NAME AS in_ATTRIBUTE_NAME,
        EXP_FINALOut.DATA_TYPE_STRING AS in_DATA_TYPE,
        EXP_FINALOut.METRIC_NAME AS in_ATTRIBUTE_TO_BE_VALIDATED_STR
    FROM EXP_FINALOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_ENTITY_IDOut AS (
    SELECT
        EXP_FINALOut.VALIDATION_NAME AS in_VALIDATION_NAME,
        EXP_FINALOut.ATTRIBUTE_NAME_ENTITY_ID AS in_ATTRIBUTE_NAME,
        EXP_FINALOut.DATA_TYPE_NUM AS in_DATA_TYPE,
        EXP_FINALOut.NUM_VAL_Y AS in_DIMENSION_KEY_FLAG,
        EXP_FINALOut.ENTITY_ID AS in_ATTRIBUTE_TO_BE_VALIDATED_NUM
    FROM EXP_FINALOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODEOut AS (
    SELECT
        EXP_FINALOut.VALIDATION_NAME AS in_VALIDATION_NAME,
        EXP_FINALOut.ATTRIBUTE_NAME_ACCOUNT_CODE AS in_ATTRIBUTE_NAME,
        EXP_FINALOut.DATA_TYPE_STRING AS in_DATA_TYPE,
        EXP_FINALOut.ACCOUNT_CODE AS in_ATTRIBUTE_TO_BE_VALIDATED_STR
    FROM EXP_FINALOut
),

EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUEOut AS (
    SELECT
        '{{ var("TARGET_NAME") }}' AS TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL AS v_VALIDATION_ACTIVE,
        CASE WHEN dsf_1.ACTIVE_Y_N_STR_VL IS NULL THEN 'N' ELSE dsf_1.ACTIVE_Y_N_STR_VL END AS v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N AS v_VALIDATION_ON_TABLE_ACTIVE,
        CASE WHEN dsf_2.ACTIVE_Y_N IS NULL THEN 'N' ELSE dsf_2.ACTIVE_Y_N END AS v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        CASE
            WHEN in_DATA_TYPE = 'DATE' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_DATE IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_DATE,
        CASE
            WHEN in_DATA_TYPE = 'STRING' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_STR IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_STR,
        CASE
            WHEN in_DATA_TYPE = 'NUMBER' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE
                         WHEN in_ATTRIBUTE_TO_BE_VALIDATED_NUM IS NULL
                              OR (CASE WHEN in_DIMENSION_KEY_FLAG IS NULL THEN 'N' ELSE in_DIMENSION_KEY_FLAG END) = 'Y'
                                 AND in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2
                         THEN 'F'
                         ELSE 'P'
                     END
            ELSE 'P'
        END AS v_NULL_CHECK_NUM,
        CASE
            WHEN v_NULL_CHECK_DATE = 'F' OR v_NULL_CHECK_STR = 'F' OR v_NULL_CHECK_NUM = 'F' THEN 'F'
            ELSE 'P'
        END AS v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL AS o_VALIDATION_PASS_FAIL,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'DATE'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            ELSE NULL
        END AS v_ERROR_MESSAGE_DATE,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'STRING'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            ELSE NULL
        END AS v_ERROR_MESSAGE_STR,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'NUMBER'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            ELSE NULL
        END AS v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM AS o_COMBINED_ERROR_MESSAGE,
        CASE WHEN v_VALIDATION_PASS_FAIL = 'F' THEN 1 ELSE 0 END AS o_ERROR_SEVERITY,
        ROW_NUMBER() OVER () AS jkey
    FROM DSR_EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUEOut
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        APPLICATION_VARIABLES.ACTIVE_Y_N AS ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE AS STRING_VALUE2
                    FROM APPLICATION_VARIABLES
                    WHERE VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      AND ACTIVE_Y_N = 'Y'
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM APPLICATION_VARIABLES
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       AND dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       AND dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_CURRENCYOut AS (
    SELECT
        '{{ var("TARGET_NAME") }}' AS TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL AS v_VALIDATION_ACTIVE,
        CASE WHEN dsf_1.ACTIVE_Y_N_STR_VL IS NULL THEN 'N' ELSE dsf_1.ACTIVE_Y_N_STR_VL END AS v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N AS v_VALIDATION_ON_TABLE_ACTIVE,
        CASE WHEN dsf_2.ACTIVE_Y_N IS NULL THEN 'N' ELSE dsf_2.ACTIVE_Y_N END AS v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        CASE
            WHEN in_DATA_TYPE = 'DATE' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_DATE IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_DATE,
        CASE
            WHEN in_DATA_TYPE = 'STRING' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_STR IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_STR,
        CASE
            WHEN in_DATA_TYPE = 'NUMBER' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE
                         WHEN in_ATTRIBUTE_TO_BE_VALIDATED_NUM IS NULL
                              OR (CASE WHEN in_DIMENSION_KEY_FLAG IS NULL THEN 'N' ELSE in_DIMENSION_KEY_FLAG END) = 'Y'
                                 AND in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2
                         THEN 'F'
                         ELSE 'P'
                     END
            ELSE 'P'
        END AS v_NULL_CHECK_NUM,
        CASE
            WHEN v_NULL_CHECK_DATE = 'F' OR v_NULL_CHECK_STR = 'F' OR v_NULL_CHECK_NUM = 'F' THEN 'F'
            ELSE 'P'
        END AS v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL AS o_VALIDATION_PASS_FAIL,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'DATE'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            ELSE NULL
        END AS v_ERROR_MESSAGE_DATE,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'STRING'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            ELSE NULL
        END AS v_ERROR_MESSAGE_STR,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'NUMBER'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            ELSE NULL
        END AS v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM AS o_COMBINED_ERROR_MESSAGE,
        CASE WHEN v_VALIDATION_PASS_FAIL = 'F' THEN 1 ELSE 0 END AS o_ERROR_SEVERITY,
        ROW_NUMBER() OVER () AS jkey
    FROM DSR_EXP_LDSA_VALIDATION_CHECKER_CURRENCYOut
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        APPLICATION_VARIABLES.ACTIVE_Y_N AS ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE AS STRING_VALUE2
                    FROM APPLICATION_VARIABLES
                    WHERE VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      AND ACTIVE_Y_N = 'Y'
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM APPLICATION_VARIABLES
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       AND dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       AND dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPEOut AS (
    SELECT
        '{{ var("TARGET_NAME") }}' AS TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL AS v_VALIDATION_ACTIVE,
        CASE WHEN dsf_1.ACTIVE_Y_N_STR_VL IS NULL THEN 'N' ELSE dsf_1.ACTIVE_Y_N_STR_VL END AS v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N AS v_VALIDATION_ON_TABLE_ACTIVE,
        CASE WHEN dsf_2.ACTIVE_Y_N IS NULL THEN 'N' ELSE dsf_2.ACTIVE_Y_N END AS v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        CASE
            WHEN in_DATA_TYPE = 'DATE' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_DATE IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_DATE,
        CASE
            WHEN in_DATA_TYPE = 'STRING' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_STR IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_STR,
        CASE
            WHEN in_DATA_TYPE = 'NUMBER' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE
                         WHEN in_ATTRIBUTE_TO_BE_VALIDATED_NUM IS NULL
                              OR (CASE WHEN in_DIMENSION_KEY_FLAG IS NULL THEN 'N' ELSE in_DIMENSION_KEY_FLAG END) = 'Y'
                                 AND in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2
                         THEN 'F'
                         ELSE 'P'
                     END
            ELSE 'P'
        END AS v_NULL_CHECK_NUM,
        CASE
            WHEN v_NULL_CHECK_DATE = 'F' OR v_NULL_CHECK_STR = 'F' OR v_NULL_CHECK_NUM = 'F' THEN 'F'
            ELSE 'P'
        END AS v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL AS o_VALIDATION_PASS_FAIL,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'DATE'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            ELSE NULL
        END AS v_ERROR_MESSAGE_DATE,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'STRING'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            ELSE NULL
        END AS v_ERROR_MESSAGE_STR,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'NUMBER'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            ELSE NULL
        END AS v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM AS o_COMBINED_ERROR_MESSAGE,
        CASE WHEN v_VALIDATION_PASS_FAIL = 'F' THEN 1 ELSE 0 END AS o_ERROR_SEVERITY,
        ROW_NUMBER() OVER () AS jkey
    FROM DSR_EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPEOut
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        APPLICATION_VARIABLES.ACTIVE_Y_N AS ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE AS STRING_VALUE2
                    FROM APPLICATION_VARIABLES
                    WHERE VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      AND ACTIVE_Y_N = 'Y'
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM APPLICATION_VARIABLES
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       AND dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       AND dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_METRIC_NAMEOut AS (
    SELECT
        '{{ var("TARGET_NAME") }}' AS TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL AS v_VALIDATION_ACTIVE,
        CASE WHEN dsf_1.ACTIVE_Y_N_STR_VL IS NULL THEN 'N' ELSE dsf_1.ACTIVE_Y_N_STR_VL END AS v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N AS v_VALIDATION_ON_TABLE_ACTIVE,
        CASE WHEN dsf_2.ACTIVE_Y_N IS NULL THEN 'N' ELSE dsf_2.ACTIVE_Y_N END AS v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        CASE
            WHEN in_DATA_TYPE = 'DATE' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_DATE IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_DATE,
        CASE
            WHEN in_DATA_TYPE = 'STRING' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_STR IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_STR,
        CASE
            WHEN in_DATA_TYPE = 'NUMBER' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE
                         WHEN in_ATTRIBUTE_TO_BE_VALIDATED_NUM IS NULL
                              OR (CASE WHEN in_DIMENSION_KEY_FLAG IS NULL THEN 'N' ELSE in_DIMENSION_KEY_FLAG END) = 'Y'
                                 AND in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2
                         THEN 'F'
                         ELSE 'P'
                     END
            ELSE 'P'
        END AS v_NULL_CHECK_NUM,
        CASE
            WHEN v_NULL_CHECK_DATE = 'F' OR v_NULL_CHECK_STR = 'F' OR v_NULL_CHECK_NUM = 'F' THEN 'F'
            ELSE 'P'
        END AS v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL AS o_VALIDATION_PASS_FAIL,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'DATE'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            ELSE NULL
        END AS v_ERROR_MESSAGE_DATE,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'STRING'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            ELSE NULL
        END AS v_ERROR_MESSAGE_STR,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'NUMBER'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            ELSE NULL
        END AS v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM AS o_COMBINED_ERROR_MESSAGE,
        CASE WHEN v_VALIDATION_PASS_FAIL = 'F' THEN 1 ELSE 0 END AS o_ERROR_SEVERITY,
        ROW_NUMBER() OVER () AS jkey
    FROM DSR_EXP_LDSA_VALIDATION_METRIC_NAMEOut
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        APPLICATION_VARIABLES.ACTIVE_Y_N AS ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE AS STRING_VALUE2
                    FROM APPLICATION_VARIABLES
                    WHERE VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      AND ACTIVE_Y_N = 'Y'
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM APPLICATION_VARIABLES
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       AND dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       AND dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_ENTITY_IDOut AS (
    SELECT
        '{{ var("TARGET_NAME") }}' AS TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL AS v_VALIDATION_ACTIVE,
        CASE WHEN dsf_1.ACTIVE_Y_N_STR_VL IS NULL THEN 'N' ELSE dsf_1.ACTIVE_Y_N_STR_VL END AS v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N AS v_VALIDATION_ON_TABLE_ACTIVE,
        CASE WHEN dsf_2.ACTIVE_Y_N IS NULL THEN 'N' ELSE dsf_2.ACTIVE_Y_N END AS v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        CASE
            WHEN in_DATA_TYPE = 'DATE' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_DATE IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_DATE,
        CASE
            WHEN in_DATA_TYPE = 'STRING' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_STR IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_STR,
        CASE
            WHEN in_DATA_TYPE = 'NUMBER' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE
                         WHEN in_ATTRIBUTE_TO_BE_VALIDATED_NUM IS NULL
                              OR (CASE WHEN in_DIMENSION_KEY_FLAG IS NULL THEN 'N' ELSE in_DIMENSION_KEY_FLAG END) = 'Y'
                                 AND in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2
                         THEN 'F'
                         ELSE 'P'
                     END
            ELSE 'P'
        END AS v_NULL_CHECK_NUM,
        CASE
            WHEN v_NULL_CHECK_DATE = 'F' OR v_NULL_CHECK_STR = 'F' OR v_NULL_CHECK_NUM = 'F' THEN 'F'
            ELSE 'P'
        END AS v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL AS o_VALIDATION_PASS_FAIL,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'DATE'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            ELSE NULL
        END AS v_ERROR_MESSAGE_DATE,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'STRING'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            ELSE NULL
        END AS v_ERROR_MESSAGE_STR,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'NUMBER'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            ELSE NULL
        END AS v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM AS o_COMBINED_ERROR_MESSAGE,
        CASE WHEN v_VALIDATION_PASS_FAIL = 'F' THEN 1 ELSE 0 END AS o_ERROR_SEVERITY,
        ROW_NUMBER() OVER () AS jkey
    FROM DSR_EXP_LDSA_VALIDATION_CHECKER_ENTITY_IDOut
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        APPLICATION_VARIABLES.ACTIVE_Y_N AS ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE AS STRING_VALUE2
                    FROM APPLICATION_VARIABLES
                    WHERE VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      AND ACTIVE_Y_N = 'Y'
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM APPLICATION_VARIABLES
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       AND dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       AND dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODEOut AS (
    SELECT
        '{{ var("TARGET_NAME") }}' AS TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL AS v_VALIDATION_ACTIVE,
        CASE WHEN dsf_1.ACTIVE_Y_N_STR_VL IS NULL THEN 'N' ELSE dsf_1.ACTIVE_Y_N_STR_VL END AS v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N AS v_VALIDATION_ON_TABLE_ACTIVE,
        CASE WHEN dsf_2.ACTIVE_Y_N IS NULL THEN 'N' ELSE dsf_2.ACTIVE_Y_N END AS v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        CASE
            WHEN in_DATA_TYPE = 'DATE' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_DATE IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_DATE,
        CASE
            WHEN in_DATA_TYPE = 'STRING' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE WHEN in_ATTRIBUTE_TO_BE_VALIDATED_STR IS NULL THEN 'F' ELSE 'P' END
            ELSE 'P'
        END AS v_NULL_CHECK_STR,
        CASE
            WHEN in_DATA_TYPE = 'NUMBER' AND v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' AND v_VALIDATION_ACTIVE_NVL = 'Y'
                THEN CASE
                         WHEN in_ATTRIBUTE_TO_BE_VALIDATED_NUM IS NULL
                              OR (CASE WHEN in_DIMENSION_KEY_FLAG IS NULL THEN 'N' ELSE in_DIMENSION_KEY_FLAG END) = 'Y'
                                 AND in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2
                         THEN 'F'
                         ELSE 'P'
                     END
            ELSE 'P'
        END AS v_NULL_CHECK_NUM,
        CASE
            WHEN v_NULL_CHECK_DATE = 'F' OR v_NULL_CHECK_STR = 'F' OR v_NULL_CHECK_NUM = 'F' THEN 'F'
            ELSE 'P'
        END AS v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL AS o_VALIDATION_PASS_FAIL,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'DATE'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            ELSE NULL
        END AS v_ERROR_MESSAGE_DATE,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'STRING'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            ELSE NULL
        END AS v_ERROR_MESSAGE_STR,
        CASE
            WHEN v_VALIDATION_PASS_FAIL = 'F' AND in_DATA_TYPE = 'NUMBER'
                THEN in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            ELSE NULL
        END AS v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM AS o_COMBINED_ERROR_MESSAGE,
        CASE WHEN v_VALIDATION_PASS_FAIL = 'F' THEN 1 ELSE 0 END AS o_ERROR_SEVERITY,
        ROW_NUMBER() OVER () AS jkey
    FROM DSR_EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODEOut
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY STRING_VALUE2 ORDER BY STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM (
                    SELECT
                        APPLICATION_VARIABLES.ACTIVE_Y_N AS ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE AS STRING_VALUE2
                    FROM APPLICATION_VARIABLES
                    WHERE VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      AND ACTIVE_Y_N = 'Y'
                )
            )
        )
        WHERE rnk_lst = 1
    ) dsf_1
        ON dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    LEFT JOIN (
        SELECT
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        FROM (
            SELECT
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY rnk_fst DESC NULLS LAST) AS rnk_lst
            FROM (
                SELECT
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    ROW_NUMBER() OVER (PARTITION BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 ORDER BY VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 NULLS LAST) AS rnk_fst
                FROM APPLICATION_VARIABLES
            )
        )
        WHERE rnk_lst = 1
    ) dsf_2
        ON dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       AND dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       AND dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_FINAL_master AS (
    SELECT
        BUSINESS_DATE,
        BATCH_ID,
        BATCH_DETAILS_ID,
        BATCH_START_DATE,
        WORKFLOW_NAME,
        MAPPING_NAME,
        TARGET_NAME,
        ACCOUNT_CODE,
        VALIDATION_NAME,
        ATTRIBUTE_NAME_ENTITY_ID AS ENTITY_ID_ATTRIBUTE_NAME,
        ATTRIBUTE_NAME_ACCOUNT_CODE AS ACCOUNT_CODE_ATTRIBUTE_NAME,
        ATTRIBUTE_NAME_METRIC_NAME AS METRIC_NAME_ATTRIBUTE_NAME,
        ATTRIBUTE_NAME_CURRENCY AS CURRENCY_ATTRIBUTE_NAME,
        ATTRIBUTE_NAME_ACCOUNT_TYPE AS ACCOUNT_TYPE_ATTRIBUTE_NAME,
        ATTRIBUTE_NAME_METRIC_VALUE AS METRIC_VALUE_ATTRIBUTE_NAME,
        ROW_NUMBER() OVER (ORDER BY ACCOUNT_CODE) AS jkey
    FROM EXP_FINALOut
),

EXP_LDSA_VALIDATION_CHECKER_ENTITY_ID_details AS (
    SELECT
        o_VALIDATION_PASS_FAIL AS ENTITY_ID_VALIDATION_PASS_FAIL,
        o_COMBINED_ERROR_MESSAGE AS ENTITY_ID_ERROR_MESSAGE,
        o_ERROR_SEVERITY AS ENTITY_ID_ERROR_SEVERITY,
        jkey
    FROM EXP_LDSA_VALIDATION_CHECKER_ENTITY_IDOut
),

EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUE_details AS (
    SELECT
        o_VALIDATION_PASS_FAIL AS METRIC_VALUE_VALIDATION_PASS_FAIL,
        o_COMBINED_ERROR_MESSAGE AS METRIC_VALUE_ERROR_MESSAGE,
        o_ERROR_SEVERITY AS METRIC_VALUE_ERROR_SEVERITY,
        jkey
    FROM EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUEOut
),

EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODE_details AS (
    SELECT
        o_VALIDATION_PASS_FAIL AS ACCOUNT_CODE_VALIDATION_PASS_FAIL,
        o_COMBINED_ERROR_MESSAGE AS ACCOUNT_CODE_ERROR_MESSAGE,
        o_ERROR_SEVERITY AS ACCOUNT_CODE_ERROR_SEVERITY,
        jkey
    FROM EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODEOut
),

EXP_LDSA_VALIDATION_CHECKER_CURRENCY_details AS (
    SELECT
        o_VALIDATION_PASS_FAIL AS CURRENCY_VALIDATION_PASS_FAIL,
        o_COMBINED_ERROR_MESSAGE AS CURRENCY_ERROR_MESSAGE,
        o_ERROR_SEVERITY AS CURRENCY_ERROR_SEVERITY,
        jkey
    FROM EXP_LDSA_VALIDATION_CHECKER_CURRENCYOut
),

EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPE_details AS (
    SELECT
        o_VALIDATION_PASS_FAIL AS ACCOUNT_TYPE_VALIDATION_PASS_FAIL,
        o_COMBINED_ERROR_MESSAGE AS ACCOUNT_TYPE_ERROR_MESSAGE,
        o_ERROR_SEVERITY AS ACCOUNT_TYPE_ERROR_SEVERITY,
        jkey
    FROM EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPEOut
),

EXP_LDSA_VALIDATION_METRIC_NAME_details AS (
    SELECT
        o_VALIDATION_pass_FAIL AS METRIC_NAME_VALIDATION_PASS_FAIL,
        o_COMBINED_ERROR_MESSAGE AS METRIC_NAME_ERROR_MESSAGE,
        o_ERROR_SEVERITY AS METRIC_NAME_ERROR_SEVERITY,
        jkey
    FROM EXP_LDSA_VALIDATION_METRIC_NAMEOut
),

dsJoiner_EXP_CONSOLIDATE_VALIDATIONS0Out AS (
    SELECT
        EXP_FINAL_master.BUSINESS_DATE,
        EXP_FINAL_master.BATCH_ID,
        EXP_FINAL_master.BATCH_DETAILS_ID,
        EXP_FINAL_master.BATCH_START_DATE,
        EXP_FINAL_master.WORKFLOW_NAME,
        EXP_FINAL_master.MAPPING_NAME,
        EXP_FINAL_master.TARGET_NAME,
        EXP_FINAL_master.ACCOUNT_CODE,
        EXP_FINAL_master.VALIDATION_NAME,
        SC_EXP_EPOC_GENERATOROut.ID,
        EXP_FINAL_master.ENTITY_ID_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_ENTITY_ID_details.ENTITY_ID_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_ENTITY_ID_details.ENTITY_ID_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_ENTITY_ID_details.ENTITY_ID_ERROR_SEVERITY,
        EXP_FINAL_master.ACCOUNT_CODE_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODE_details.ACCOUNT_CODE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODE_details.ACCOUNT_CODE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODE_details.ACCOUNT_CODE_ERROR_SEVERITY,
        EXP_FINAL_master.METRIC_NAME_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_METRIC_NAME_details.METRIC_NAME_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_METRIC_NAME_details.METRIC_NAME_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_METRIC_NAME_details.METRIC_NAME_ERROR_SEVERITY,
        EXP_FINAL_master.CURRENCY_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_CURRENCY_details.CURRENCY_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_CURRENCY_details.CURRENCY_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_CURRENCY_details.CURRENCY_ERROR_SEVERITY,
        EXP_FINAL_master.ACCOUNT_TYPE_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPE_details.ACCOUNT_TYPE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPE_details.ACCOUNT_TYPE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPE_details.ACCOUNT_TYPE_ERROR_SEVERITY,
        EXP_FINAL_master.METRIC_VALUE_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUE_details.METRIC_VALUE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUE_details.METRIC_VALUE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUE_details.METRIC_VALUE_ERROR_SEVERITY,
        ROW_NUMBER() OVER (ORDER BY 1) AS jkey
    FROM EXP_FINAL_master
    INNER JOIN EXP_LDSA_VALIDATION_CHECKER_ENTITY_ID_details
        ON EXP_FINAL_master.jkey = EXP_LDSA_VALIDATION_CHECKER_ENTITY_ID_details.jkey
    INNER JOIN EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUE_details
        ON EXP_FINAL_master.jkey = EXP_LDSA_VALIDATION_CHECKER_METRIC_VALUE_details.jkey
    INNER JOIN EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODE_details
        ON EXP_FINAL_master.jkey = EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_CODE_details.jkey
    INNER JOIN EXP_LDSA_VALIDATION_CHECKER_CURRENCY_details
        ON EXP_FINAL_master.jkey = EXP_LDSA_VALIDATION_CHECKER_CURRENCY_details.jkey
    INNER JOIN SC_EXP_EPOC_GENERATOROut
        ON EXP_FINAL_master.jkey = SC_EXP_EPOC_GENERATOROut.jkey
    INNER JOIN EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPE_details
        ON EXP_FINAL_master.jkey = EXP_LDSA_VALIDATION_CHECKER_ACCOUNT_TYPE_details.jkey
    INNER JOIN EXP_LDSA_VALIDATION_METRIC_NAME_details
        ON EXP_FINAL_master.jkey = EXP_LDSA_VALIDATION_METRIC_NAME_details.jkey
),

EXP_CONSOLIDATE_VALIDATIONSOut AS (
    SELECT
        BUSINESS_DATE,
        BATCH_ID,
        BATCH_DETAILS_ID,
        BATCH_START_DATE,
        WORKFLOW_NAME,
        MAPPING_NAME,
        TARGET_NAME,
        '{{ var("FILE_NAME") }}' AS FILE_NAME,
        ACCOUNT_CODE,
        'FILE NAME:' || FILE_NAME || '/' || 'BUSINESS_DATE:' || BUSINESS_DATE || '/' || 'ACCOUNT_CODE:' || ACCOUNT_CODE || '/' AS v_ERROR_MESSAGE,
        VALIDATION_NAME,
        ID,
        ENTITY_ID_ATTRIBUTE_NAME,
        ENTITY_ID_VALIDATION_PASS_FAIL,
        ENTITY_ID_ERROR_MESSAGE,
        ENTITY_ID_ERROR_SEVERITY,
        v_ERROR_MESSAGE || ENTITY_ID_ERROR_MESSAGE AS O_ENTITY_ID_ERROR_MESSAGE,
        ACCOUNT_CODE_ATTRIBUTE_NAME,
        ACCOUNT_CODE_VALIDATION_PASS_FAIL,
        ACCOUNT_CODE_ERROR_MESSAGE,
        ACCOUNT_CODE_ERROR_SEVERITY,
        v_ERROR_MESSAGE || ACCOUNT_CODE_ERROR_MESSAGE AS O_ACCOUNT_CODE_ERROR_MESSAGE,
        METRIC_NAME_ATTRIBUTE_NAME,
        METRIC_NAME_VALIDATION_PASS_FAIL,
        METRIC_NAME_ERROR_MESSAGE,
        METRIC_NAME_ERROR_SEVERITY,
        v_ERROR_MESSAGE || METRIC_NAME_ERROR_MESSAGE AS O_METRIC_NAME_ERROR_MESSAGE,
        CURRENCY_ATTRIBUTE_NAME,
        CURRENCY_VALIDATION_PASS_FAIL,
        CURRENCY_ERROR_MESSAGE,
        CURRENCY_ERROR_SEVERITY,
        v_ERROR_MESSAGE || CURRENCY_ERROR_MESSAGE AS O_CURRENCY_ERROR_MESSAGE,
        ACCOUNT_TYPE_ATTRIBUTE_NAME,
        ACCOUNT_TYPE_VALIDATION_PASS_FAIL,
        ACCOUNT_TYPE_ERROR_MESSAGE,
        ACCOUNT_TYPE_ERROR_SEVERITY,
        v_ERROR_MESSAGE || ACCOUNT_TYPE_ERROR_MESSAGE AS O_ACCOUNT_TYPE_ERROR_MESSAGE,
        METRIC_VALUE_ATTRIBUTE_NAME,
        METRIC_VALUE_VALIDATION_PASS_FAIL,
        METRIC_VALUE_ERROR_MESSAGE,
        METRIC_VALUE_ERROR_SEVERITY,
        v_ERROR_MESSAGE || METRIC_VALUE_ERROR_MESSAGE AS O_METRIC_VALUE_ERROR_MESSAGE,
        CASE
            WHEN ENTITY_ID_VALIDATION_PASS_FAIL = 'F'
              OR ACCOUNT_CODE_VALIDATION_PASS_FAIL = 'F'
              OR METRIC_NAME_VALIDATION_PASS_FAIL = 'F'
              OR CURRENCY_VALIDATION_PASS_FAIL = 'F'
              OR ACCOUNT_TYPE_VALIDATION_PASS_FAIL = 'F'
              OR METRIC_VALUE_VALIDATION_PASS_FAIL = 'F'
            THEN 1
            ELSE 0
        END AS MD_ERROR_IND
    FROM dsJoiner_EXP_CONSOLIDATE_VALIDATIONS0Out
),

DSR_FIL_ERROR_LOGICOut AS (
    SELECT
        EXP_CONSOLIDATE_VALIDATIONSOut.ACCOUNT_TYPE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.ACCOUNT_TYPE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_ACCOUNT_TYPE_ERROR_MESSAGE AS ACCOUNT_TYPE_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.METRIC_VALUE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.METRIC_VALUE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_METRIC_VALUE_ERROR_MESSAGE AS METRIC_VALUE_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.BATCH_START_DATE,
        EXP_CONSOLIDATE_VALIDATIONSOut.WORKFLOW_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.MAPPING_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.TARGET_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.MD_ERROR_IND,
        EXP_CONSOLIDATE_VALIDATIONSOut.BUSINESS_DATE,
        EXP_CONSOLIDATE_VALIDATIONSOut.BATCH_ID,
        EXP_CONSOLIDATE_VALIDATIONSOut.BATCH_DETAILS_ID,
        EXP_CONSOLIDATE_VALIDATIONSOut.VALIDATION_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.ENTITY_ID_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_ENTITY_ID_ERROR_MESSAGE AS ENTITY_ID_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.ID,
        EXP_CONSOLIDATE_VALIDATIONSOut.ENTITY_ID_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.ACCOUNT_CODE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.ACCOUNT_CODE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_ACCOUNT_CODE_ERROR_MESSAGE AS ACCOUNT_CODE_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.METRIC_NAME_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.METRIC_NAME_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_METRIC_NAME_ERROR_MESSAGE AS METRIC_NAME_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.CURRENCY_ATTRIBUTE_NAME AS CURRENCY_CODE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.CURRENCY_ERROR_SEVERITY AS CURRENCY_CODE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_CURRENCY_ERROR_MESSAGE AS CURRENCY_CODE_ERROR_MESSAGE
    FROM EXP_CONSOLIDATE_VALIDATIONSOut
),

DSR_SC_mplt_DV_PROCESS_ERROR_DETAILSOut AS (
    SELECT
        FIL_ERROR_LOGICOut.VALIDATION_NAME AS INPUT_ERROR_CODE_6,
        FIL_ERROR_LOGICOut.METRIC_VALUE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_6,
        FIL_ERROR_LOGICOut.BATCH_DETAILS_ID AS INPUT_BATCH_DETAILS_ID,
        FIL_ERROR_LOGICOut.BATCH_START_DATE AS INPUT_BATCH_START_DATE,
        FIL_ERROR_LOGICOut.BUSINESS_DATE AS INPUT_BATCH_BUSINESS_DATE,
        FIL_ERROR_LOGICOut.BATCH_ID AS INPUT_BATCH_ID,
        FIL_ERROR_LOGICOut.WORKFLOW_NAME AS INPUT_WORKFLOW_NAME,
        FIL_ERROR_LOGICOut.MAPPING_NAME AS INPUT_MAPPING_NAME,
        FIL_ERROR_LOGICOut.TARGET_NAME AS INPUT_TARGET_NAME,
        FIL_ERROR_LOGICOut.ID AS INPUT_REFERENCE_RECORD_ID,
        FIL_ERROR_LOGICOut.VALIDATION_NAME AS INPUT_ERROR_CODE_1,
        FIL_ERROR_LOGICOut.ENTITY_ID_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_1,
        FIL_ERROR_LOGICOut.ENTITY_ID_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_1,
        FIL_ERROR_LOGICOut.ENTITY_ID_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_1,
        FIL_ERROR_LOGICOut.ACCOUNT_CODE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_2,
        FIL_ERROR_LOGICOut.ACCOUNT_CODE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_2,
        FIL_ERROR_LOGICOut.VALIDATION_NAME AS INPUT_ERROR_CODE_2,
        FIL_ERROR_LOGICOut.ACCOUNT_CODE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_2,
        FIL_ERROR_LOGICOut.VALIDATION_NAME AS INPUT_ERROR_CODE_3,
        FIL_ERROR_LOGICOut.METRIC_NAME_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_3,
        FIL_ERROR_LOGICOut.METRIC_NAME_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_3,
        FIL_ERROR_LOGICOut.METRIC_NAME_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_3,
        FIL_ERROR_LOGICOut.CURRENCY_CODE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_4,
        FIL_ERROR_LOGICOut.CURRENCY_CODE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_4,
        FIL_ERROR_LOGICOut.VALIDATION_NAME AS INPUT_ERROR_CODE_4,
        FIL_ERROR_LOGICOut.CURRENCY_CODE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_4,
        FIL_ERROR_LOGICOut.ACCOUNT_TYPE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_5,
        FIL_ERROR_LOGICOut.ACCOUNT_TYPE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_5,
        FIL_ERROR_LOGICOut.VALIDATION_NAME AS INPUT_ERROR_CODE_5,
        FIL_ERROR_LOGICOut.ACCOUNT_TYPE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_5,
        FIL_ERROR_LOGICOut.METRIC_VALUE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_6,
        FIL_ERROR_LOGICOut.METRIC_VALUE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_6
    FROM DSR_FIL_ERROR_LOGICOut
    WHERE MD_ERROR_IND = 1
),

SC_mplt_DV_PROCESS_ERROR_DETAILSOut AS (
    SELECT
        PROCESS_DATE,
        REFERENCE_BATCH_ID,
        REFERENCE_PROCESS,
        REFERENCE_SUB_PROCESS,
        REFERENCE_TARGET_NAME,
        REFERENCE_RECORD_ID,
        REFERENCE_BUSINESS_KEY,
        REFERENCE_ATTRIBUTE_NAME,
        REFERENCE_ATTRIBUTE_VALUE,
        ERROR_CODE,
        ERROR_SEVERITY,
        ADDITIONAL_DETAILS,
        REFERENCE_BATCH_DETAILS_ID,
        BATCH_START_DATE AS CREATED_TS,
        ID,
        VALIDATION_NAME AS INPUT_ERROR_CODE_6,
        METRIC_VALUE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_6,
        BATCH_DETAILS_ID AS INPUT_BATCH_DETAILS_ID,
        BATCH_START_DATE AS INPUT_BATCH_START_DATE,
        BUSINESS_DATE AS INPUT_BATCH_BUSINESS_DATE,
        BATCH_ID AS INPUT_BATCH_ID,
        WORKFLOW_NAME AS INPUT_WORKFLOW_NAME,
        MAPPING_NAME AS INPUT_MAPPING_NAME,
        TARGET_NAME AS INPUT_TARGET_NAME,
        ID AS INPUT_REFERENCE_RECORD_ID,
        VALIDATION_NAME AS INPUT_ERROR_CODE_1,
        ENTITY_ID_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_1,
        ENTITY_ID_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_1,
        ENTITY_ID_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_1,
        ACCOUNT_CODE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_2,
        ACCOUNT_CODE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_2,
        VALIDATION_NAME AS INPUT_ERROR_CODE_2,
        ACCOUNT_CODE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_2,
        VALIDATION_NAME AS INPUT_ERROR_CODE_3,
        METRIC_NAME_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_3,
        METRIC_NAME_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_3,
        METRIC_NAME_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_3,
        CURRENCY_CODE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_4,
        CURRENCY_CODE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_4,
        VALIDATION_NAME AS INPUT_ERROR_CODE_4,
        CURRENCY_CODE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_4,
        ACCOUNT_TYPE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_5,
        ACCOUNT_TYPE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_5,
        VALIDATION_NAME AS INPUT_ERROR_CODE_5,
        ACCOUNT_TYPE_ERROR_SEVERITY AS INPUT_ERROR_SEVERITY_5,
        METRIC_VALUE_ATTRIBUTE_NAME AS INPUT_ATTRIBUTE_NAME_6,
        METRIC_VALUE_ERROR_MESSAGE AS INPUT_ATTRIBUTE_VALUE_6
    FROM DSR_SC_mplt_DV_PROCESS_ERROR_DETAILSOut
),

DSR_SC_PROCESS_ERROR_DETAILSOut AS (
    SELECT
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.PROCESS_DATE,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_BATCH_ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_PROCESS,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_SUB_PROCESS,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_TARGET_NAME,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_RECORD_ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_BUSINESS_KEY,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_ATTRIBUTE_NAME,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_ATTRIBUTE_VALUE,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ERROR_CODE,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ERROR_SEVERITY,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ADDITIONAL_DETAILS,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_BATCH_DETAILS_ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.CREATED_TS
    FROM SC_mplt_DV_PROCESS_ERROR_DETAILSOut
)

SELECT
    ID,
    PROCESS_DATE,
    REFERENCE_BATCH_ID,
    REFERENCE_PROCESS,
    REFERENCE_SUB_PROCESS,
    REFERENCE_TARGET_NAME,
    REFERENCE_RECORD_ID,
    REFERENCE_BUSINESS_KEY,
    REFERENCE_ATTRIBUTE_NAME,
    REFERENCE_ATTRIBUTE_VALUE,
    ERROR_CODE,
    ERROR_SEVERITY,
    ADDITIONAL_DETAILS,
    REFERENCE_BATCH_DETAILS_ID,
    CREATED_TS
FROM DSR_SC_PROCESS_ERROR_DETAILSOut