{{
    config(
        materialized='incremental',
        alias='PROCESS_ERROR_DETAILS',
        schema='LDSA_SHARED',
        pre_hook="",
        post_hook="",
        incremental_strategy='append'
    )
}}

with SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut as (
    select
        REPORT_DATE,
        TMF_CODE,
        CMF_CODE,
        FINANICAL_ACCOUNT,
        COLLATERAL_ACCOUNT,
        MARGIN_ACCOUNT,
        POSITION_ACCOUNT,
        SEGREGATION_TYPE,
        PRODUCT_FAMILY,
        CurrentlyProcessedFileName
    from {{ source('FlatFile_catalog_1', 'LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24miss') }}
),

Target_Table_Name_SK_Max_Value as (
    select
        max(GENERATED_SEQ) as max_generated_seq
    from {{ this }}
),

DSR_SC_EXP_EPOC_GENERATOROut as (
    select
        (select max_generated_seq from Target_Table_Name_SK_Max_Value) + row_number() over (order by (select null)) as GENERATED_SEQ
    from SC_SEQ_LD_SA_TRADE_ACCOUNTOut as SC_SEQ_LD_SA_TRADE_ACCOUNTOut
),

DSR_EXP_TRIMOut as (
    select
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.REPORT_DATE as REPORT_DATE,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.TMF_CODE as TMF_CODE,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.CMF_CODE as CMF_CODE,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.FINANICAL_ACCOUNT as FINANCIAL_ACCOUNT,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.COLLATERAL_ACCOUNT as COLLATERAL_ACCOUNT,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.MARGIN_ACCOUNT as MARGIN_ACCOUNT,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.POSITION_ACCOUNT as POSITION_ACCOUNT,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.SEGREGATION_TYPE as SEGREGATION_TYPE,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.PRODUCT_FAMILY as PRODUCT_FAMILY,
        SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut.CurrentlyProcessedFileName as CurrentlyProcessedFileName
    from SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut as SQ_SC_LCHSA_ETD_TradeAccount_YYYYmmdd_YYYYmmddHH24missOut
),

SC_EXP_EPOC_GENERATOROut as (
    select
        lpad(GENERATED_SEQ, 9, '0') as v_SEQ_LPAD_TO_9,
        datediff(
            second,
            to_timestamp('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'),
            current_timestamp()
        ) as v_EPOC_TIME,
        v_EPOC_TIME || v_SEQ_LPAD_TO_9 as v_CONCATE_ID,
        v_CONCATE_ID as ID
    from DSR_SC_EXP_EPOC_GENERATOROut as DSR_SC_EXP_EPOC_GENERATOROut
),

EXP_TRIMOut as (
    select
        (case
            when REPORT_DATE is not null then
                to_date(ltrim(rtrim(REPORT_DATE)), 'DD/MM/YYYY')
            else
                null
         end) as O_REPORT_DATE,
        ltrim(rtrim(TMF_CODE)) as O_TMF_CODE,
        ltrim(rtrim(CMF_CODE)) as O_CMF_CODE,
        ltrim(rtrim(FINANCIAL_ACCOUNT)) as O_FINANCIAL_ACCOUNT,
        ltrim(rtrim(COLLATERAL_ACCOUNT)) as O_COLLATERAL_ACCOUNT,
        ltrim(rtrim(MARGIN_ACCOUNT)) as O_MARGIN_ACCOUNT,
        ltrim(rtrim(POSITION_ACCOUNT)) as O_POSITION_ACCOUNT,
        ltrim(rtrim(SEGREGATION_TYPE)) as O_SEGREGATION_TYPE,
        ltrim(rtrim(PRODUCT_FAMILY)) as O_PRODUCT_FAMILY,
        'LCH.SA' as ENTITY_NAME,
        CurrentlyProcessedFileName as CurrentlyProcessedFileName
    from DSR_EXP_TRIMOut as DSR_EXP_TRIMOut
),

DSR_EXP_PASS_THROUGHOut as (
    select
        EXP_TRIMOut.O_REPORT_DATE as REPORT_DATE,
        EXP_TRIMOut.O_TMF_CODE as TMF_CODE,
        EXP_TRIMOut.O_CMF_CODE as CMF_CODE,
        EXP_TRIMOut.O_POSITION_ACCOUNT as POSITION_ACCOUNT,
        EXP_TRIMOut.O_MARGIN_ACCOUNT as MARGIN_ACCOUNT,
        EXP_TRIMOut.O_FINANCIAL_ACCOUNT as FINANCIAL_ACCOUNT,
        EXP_TRIMOut.O_COLLATERAL_ACCOUNT as COLLATERAL_ACCOUNT,
        EXP_TRIMOut.O_SEGREGATION_TYPE as SEGREGATION_TYPE,
        EXP_TRIMOut.O_PRODUCT_FAMILY as PRODUCT_FAMILY,
        EXP_TRIMOut.ENTITY_NAME as ENTITY_NAME,
        EXP_TRIMOut.CurrentlyProcessedFileName as CurrentlyProcessedFileName
    from EXP_TRIMOut as EXP_TRIMOut
),

EXP_PASS_THROUGHOut as (
    select
        '{{ var("BATCH_ID") }}' as BATCH_ID,
        dsf_1.ID as BATCH_DETAILS_ID,
        to_timestamp('{{ var("BATCH_START_DATE") }}', 'DD-MON-YYYY HH24:MI:SS') as BATCH_START_DATE,
        REPORT_DATE as REPORT_DATE,
        '{{ var("PMWorkflowName", "") }}' as WORKFLOW_NAME,
        '{{ var("PMMappingName", "") }}' as MAPPING_NAME,
        '{{ var("TARGET_NAME") }}' as TARGET_NAME,
        TMF_CODE as TMF_CODE,
        CMF_CODE as CMF_CODE,
        POSITION_ACCOUNT as POSITION_ACCOUNT,
        MARGIN_ACCOUNT as MARGIN_ACCOUNT,
        FINANCIAL_ACCOUNT as FINANCIAL_ACCOUNT,
        COLLATERAL_ACCOUNT as COLLATERAL_ACCOUNT,
        SEGREGATION_TYPE as SEGREGATION_TYPE,
        PRODUCT_FAMILY as PRODUCT_FAMILY,
        ENTITY_NAME as ENTITY_NAME,
        dsf_2.DIMENSION_KEY as ENTITY_ID,
        v_TOT_REC_COUNT + 1 as v_TOT_REC_COUNT,
        '{{ var("TOT_REC_COUNT") }}' as TOT_REC_COUNT,
        'Y' as NUM_FLAG_Y,
        CurrentlyProcessedFileName as CurrentlyProcessedFileName,
        'STRING' as DATA_TYPE_STRING,
        'DATE' as DATA_TYPE_DATE,
        'NUMBER' as DATA_TYPE_NUMBER,
        'E300_BLANK_FIELD_OR_LKUP_FAIL' as VALIDATION_NAME,
        'ENTITY_ID' as ATTRIBUTE_NAME_1,
        'TMF_CODE' as ATTRIBUTE_NAME_2,
        'CMF_CODE' as ATTRIBUTE_NAME_3,
        'FINANCIAL_ACCOUNT' as ATTRIBUTE_NAME_4,
        'COLLATERAL_ACCOUNT' as ATTRIBUTE_NAME_5,
        'MARGIN_ACCOUNT' as ATTRIBUTE_NAME_6,
        'POSITION_ACCOUNT' as ATTRIBUTE_NAME_7,
        'SEGREGATION_TYPE' as ATTRIBUTE_NAME_8,
        'PRODUCT_FAMILY' as ATTRIBUTE_NAME_9
    from DSR_EXP_PASS_THROUGHOut as DSR_EXP_PASS_THROUGHOut
    left join (
        select
            ID,
            BATCH_ID,
            TARGET_TABLE_NAME,
            FILE_NAME
        from (
            select
                ID,
                BATCH_ID,
                TARGET_TABLE_NAME,
                FILE_NAME,
                row_number() over (partition by BATCH_ID, TARGET_TABLE_NAME, FILE_NAME order by rnk_fst desc nulls last) as rnk_lst
            from (
                select
                    ID,
                    BATCH_ID,
                    TARGET_TABLE_NAME,
                    FILE_NAME,
                    row_number() over (partition by BATCH_ID, TARGET_TABLE_NAME, FILE_NAME order by BATCH_ID, TARGET_TABLE_NAME, FILE_NAME nulls last) as rnk_fst
                from (
                    select
                        BATCH_DETAILS.ID as ID,
                        BATCH_DETAILS.BATCH_ID as BATCH_ID,
                        BATCH_DETAILS.TARGET_TABLE_NAME as TARGET_TABLE_NAME,
                        BATCH_DETAILS.FILE_NAME as FILE_NAME,
                        BATCH_DETAILS.STATUS as STATUS
                    from (
                        select
                            id,
                            TARGET_TABLE_NAME,
                            BATCH_ID,
                            STATUS,
                            ACTIVE_FLAG,
                            BATCH_DETAILS_START_TIME,
                            FILE_NAME,
                            rank() over (partition by batch_id, TARGET_TABLE_NAME, FILE_NAME order by BATCH_DETAILS_START_TIME desc) as RANK
                        from BATCH_DETAILS
                    ) BATCH_DETAILS
                    where RANK = 1
                      and STATUS = 'I'
                      and ACTIVE_FLAG = 1
                )
            ) lkp_inner
        ) lkp_outer
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.BATCH_ID = '{{ var("BATCH_ID") }}'
       and dsf_1.TARGET_TABLE_NAME = '{{ var("TARGET_NAME") }}'
       and dsf_1.FILE_NAME = '{{ var("FILE_NAME_OVERRIDE") }}'
    left join (
        select
            DIMENSION_KEY,
            NAME,
            EFFECTIVE_DATE,
            EXPIRATION_DATE
        from (
            select
                DIMENSION_KEY,
                NAME,
                EFFECTIVE_DATE,
                EXPIRATION_DATE,
                row_number() over (partition by NAME, EFFECTIVE_DATE, EXPIRATION_DATE order by rnk_fst desc nulls last) as rnk_lst
            from (
                select
                    DIMENSION_KEY,
                    NAME,
                    EFFECTIVE_DATE,
                    EXPIRATION_DATE,
                    row_number() over (partition by NAME, EFFECTIVE_DATE, EXPIRATION_DATE order by NAME, EFFECTIVE_DATE, EXPIRATION_DATE nulls last) as rnk_fst
                from (
                    select
                        ENTITY_DIM.DIMENSION_KEY as DIMENSION_KEY,
                        ENTITY_DIM.NAME as NAME,
                        ENTITY_DIM.EFFECTIVE_DATE as EFFECTIVE_DATE,
                        nvl(ENTITY_DIM.EXPIRATION_DATE, to_date('31-DEC-2099', 'DD-MON-YYYY')) as EXPIRATION_DATE
                    from ENTITY_DIM
                )
            ) lkp_inner
        ) lkp_outer
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.NAME = ENTITY_NAME
       and dsf_2.EFFECTIVE_DATE <= REPORT_DATE
       and dsf_2.EXPIRATION_DATE >= REPORT_DATE
),

DSR_EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_5 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.COLLATERAL_ACCOUNT as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_CMFOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_3 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.CMF_CODE as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_8 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.SEGREGATION_TYPE as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_4 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.FINANCIAL_ACCOUNT as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_TMFOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_2 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.TMF_CODE as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_6 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.MARGIN_ACCOUNT as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_9 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.PRODUCT_FAMILY as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_7 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_STRING as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.POSITION_ACCOUNT as in_ATTRIBUTE_TO_BE_VALIDATED_STR
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

DSR_EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut as (
    select
        EXP_PASS_THROUGHOut.VALIDATION_NAME as in_VALIDATION_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_1 as in_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.DATA_TYPE_NUMBER as in_DATA_TYPE,
        EXP_PASS_THROUGHOut.NUM_FLAG_Y as in_DIMENSION_KEY_FLAG,
        EXP_PASS_THROUGHOut.ENTITY_ID as in_ATTRIBUTE_TO_BE_VALIDATED_NUM
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut as DSR_EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut
    left join (
        select
            ACTIVE_Y_N_STR_VL,
            STRING_VALUE2
        from (
            select
                ACTIVE_Y_N_STR_VL,
                STRING_VALUE2,
                row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select
                    ACTIVE_Y_N_STR_VL,
                    STRING_VALUE2,
                    row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select
                        APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                        APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select
            ACTIVE_Y_N,
            VARIABLE_NAME,
            STRING_VALUE,
            STRING_VALUE2
        from (
            select
                ACTIVE_Y_N,
                VARIABLE_NAME,
                STRING_VALUE,
                STRING_VALUE2,
                row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select
                    ACTIVE_Y_N,
                    VARIABLE_NAME,
                    STRING_VALUE,
                    STRING_VALUE2,
                    row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_CMFOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_CMFOut as DSR_EXP_LDSA_VALIDATION_CHECKER_CMFOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut as DSR_EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut as DSR_EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_TMFOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_TMFOut as DSR_EXP_LDSA_VALIDATION_CHECKER_TMFOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut as DSR_EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut as DSR_EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_NUM
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut as DSR_EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut as (
    select
        '{{ var("TARGET_NAME") }}' as TARGET_TABLE_NAME,
        dsf_1.ACTIVE_Y_N_STR_VL as v_VALIDATION_ACTIVE,
        case when v_VALIDATION_ACTIVE is null then 'N' else v_VALIDATION_ACTIVE end as v_VALIDATION_ACTIVE_NVL,
        dsf_2.ACTIVE_Y_N as v_VALIDATION_ON_TABLE_ACTIVE,
        case when v_VALIDATION_ON_TABLE_ACTIVE is null then 'N' else v_VALIDATION_ON_TABLE_ACTIVE end as v_VALIDATION_ON_TABLE_ACTIVE_NVL,
        case
            when in_DATA_TYPE = 'DATE' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_DATE is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_DATE,
        case
            when in_DATA_TYPE = 'STRING' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case when in_ATTRIBUTE_TO_BE_VALIDATED_STR is null then 'F' else 'P' end
            else 'P'
        end as v_NULL_CHECK_STR,
        case
            when in_DATA_TYPE = 'NUMBER' and v_VALIDATION_ON_TABLE_ACTIVE_NVL = 'Y' and v_VALIDATION_ACTIVE_NVL = 'Y' then
                case
                    when in_ATTRIBUTE_TO_BE_VALIDATED_NUM is null
                         or (case when in_DIMENSION_KEY_FLAG is null then 'N' else in_DIMENSION_KEY_FLAG end) = 'Y'
                            and in_ATTRIBUTE_TO_BE_VALIDATED_NUM = -2 then 'F'
                    else 'P'
                end
            else 'P'
        end as v_NULL_CHECK_NUM,
        case
            when v_NULL_CHECK_DATE = 'F' or v_NULL_CHECK_STR = 'F' or v_NULL_CHECK_NUM = 'F' then 'F' else 'P'
        end as v_VALIDATION_PASS_FAIL,
        v_VALIDATION_PASS_FAIL as o_VALIDATION_PASS_FAIL,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'DATE' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_DATE
            else null
        end as v_ERROR_MESSAGE_DATE,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'STRING' then in_ATTRIBUTE_NAME || ':' || in_ATTRIBUTE_TO_BE_VALIDATED_STR
            else null
        end as v_ERROR_MESSAGE_STR,
        case
            when v_VALIDATION_PASS_FAIL = 'F' and in_DATA_TYPE = 'NUMBER' then in_ATTRIBUTE_NAME || ':' || in_attribute_to_be_validated_num
            else null
        end as v_ERROR_MESSAGE_NUM,
        v_ERROR_MESSAGE_DATE || v_ERROR_MESSAGE_STR || v_ERROR_MESSAGE_NUM as o_COMBINED_ERROR_MESSAGE,
        case when v_VALIDATION_PASS_FAIL = 'F' then 1 else 0 end as o_ERROR_SEVERITY
    from DSR_EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut as DSR_EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut
    left join (
        select ACTIVE_Y_N_STR_VL, STRING_VALUE2
        from (
            select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                   row_number() over (partition by STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N_STR_VL, STRING_VALUE2,
                       row_number() over (partition by STRING_VALUE2 order by STRING_VALUE2 nulls last) as rnk_fst
                from (
                    select APPLICATION_VARIABLES.ACTIVE_Y_N as ACTIVE_Y_N_STR_VL,
                           APPLICATION_VARIABLES.STRING_VALUE as STRING_VALUE2
                    from APPLICATION_VARIABLES
                    where VARIABLE_NAME = 'GENERIC_ERROR_MASTER'
                      and ACTIVE_Y_N = 'Y'
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.STRING_VALUE2 = in_VALIDATION_NAME
    left join (
        select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2
        from (
            select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                   row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by rnk_fst desc nulls last) as rnk_lst
            from (
                select ACTIVE_Y_N, VARIABLE_NAME, STRING_VALUE, STRING_VALUE2,
                       row_number() over (partition by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 order by VARIABLE_NAME, STRING_VALUE, STRING_VALUE2 nulls last) as rnk_fst
                from APPLICATION_VARIABLES
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.VARIABLE_NAME = in_VALIDATION_NAME
       and dsf_2.STRING_VALUE = TARGET_TABLE_NAME
       and dsf_2.STRING_VALUE2 = in_ATTRIBUTE_NAME
),

EXP_LDSA_VALIDATION_CHECKER_TMF_master as (
    select
        EXP_LDSA_VALIDATION_CHECKER_TMFOut.o_VALIDATION_PASS_FAIL as TMF_CODE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_TMFOut.o_COMBINED_ERROR_MESSAGE as TMF_CODE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_TMFOut.o_ERROR_SEVERITY as TMF_CODE_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_TMFOut as EXP_LDSA_VALIDATION_CHECKER_TMFOut
),

EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut.o_VALIDATION_PASS_FAIL as COLLATERAL_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut.o_COMBINED_ERROR_MESSAGE as COLLATERAL_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut.o_ERROR_SEVERITY as COLLATERAL_ACCOUNT_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut as EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNTOut
),

EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut.o_VALIDATION_PASS_FAIL as MARGIN_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut.o_COMBINED_ERROR_MESSAGE as MARGIN_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut.o_ERROR_SEVERITY as MARGIN_ACCOUNT_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut as EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNTOut
),

EXP_LDSA_VALIDATION_CHECKER_CMF_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_CMFOut.o_VALIDATION_PASS_FAIL as CMF_CODE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_CMFOut.o_COMBINED_ERROR_MESSAGE as CMF_CODE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_CMFOut.o_ERROR_SEVERITY as CMF_CODE_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_CMFOut as EXP_LDSA_VALIDATION_CHECKER_CMFOut
),

EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut.o_VALIDATION_PASS_FAIL as PRODUCT_FAMILY_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut.o_COMBINED_ERROR_MESSAGE as PRODUCT_FAMILY_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut.o_ERROR_SEVERITY as PRODUCT_FAMILY_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut as EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILYOut
),

EXP_PASS_THROUGH_details as (
    select
        EXP_PASS_THROUGHOut.BATCH_ID as BATCH_ID,
        EXP_PASS_THROUGHOut.BATCH_DETAILS_ID as BATCH_DETAILS_ID,
        EXP_PASS_THROUGHOut.BATCH_START_DATE as BATCH_START_DATE,
        EXP_PASS_THROUGHOut.REPORT_DATE as BUSINESS_DATE,
        EXP_PASS_THROUGHOut.WORKFLOW_NAME as WORKFLOW_NAME,
        EXP_PASS_THROUGHOut.MAPPING_NAME as MAPPING_NAME,
        EXP_PASS_THROUGHOut.TARGET_NAME as TARGET_NAME,
        EXP_PASS_THROUGHOut.VALIDATION_NAME as VALIDATION_NAME,
        EXP_PASS_THROUGHOut.TMF_CODE as TMF_CODE,
        EXP_PASS_THROUGHOut.CMF_CODE as CMF_CODE,
        EXP_PASS_THROUGHOut.POSITION_ACCOUNT as FINANICAL_ACCOUNT,
        EXP_PASS_THROUGHOut.COLLATERAL_ACCOUNT as COLLATERAL_ACCOUNT,
        EXP_PASS_THROUGHOut.MARGIN_ACCOUNT as MARGIN_ACCOUNT,
        EXP_PASS_THROUGHOut.FINANCIAL_ACCOUNT as POSITION_ACCOUNT,
        EXP_PASS_THROUGHOut.SEGREGATION_TYPE as SEGREGATION_TYPE,
        EXP_PASS_THROUGHOut.PRODUCT_FAMILY as ACCOUNT_STRUCTURE_TYPE,
        EXP_PASS_THROUGHOut.ENTITY_NAME as ENTITY_NAME,
        EXP_PASS_THROUGHOut.ENTITY_ID as ENTITY_ID,
        EXP_PASS_THROUGHOut.TOT_REC_COUNT as TOT_REC_COUNT,
        EXP_PASS_THROUGHOut.CurrentlyProcessedFileName as CurrentlyProcessedFileName,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_1 as ENTITY_ID_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_2 as TMF_CODE_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_3 as CMF_CODE_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_4 as FINANCIAL_ACCOUNT_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_5 as COLLATERAL_ACCOUNT_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_6 as MARGIN_ACCOUNT_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_7 as POSITION_ACCOUNT_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_8 as SEGREGATION_TYPE_ATTRIBUTE_NAME,
        EXP_PASS_THROUGHOut.ATTRIBUTE_NAME_9 as PRODUCT_FAMILY_ATTRIBUTE_NAME
    from EXP_PASS_THROUGHOut as EXP_PASS_THROUGHOut
),

EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut.o_VALIDATION_PASS_FAIL as POSITION_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut.o_COMBINED_ERROR_MESSAGE as POSITION_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut.o_ERROR_SEVERITY as POSITION_ACCOUNT_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut as EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNTOut
),

EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut.o_VALIDATION_PASS_FAIL as SEGREGATION_TYPE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut.o_COMBINED_ERROR_MESSAGE as SEGREGATION_TYPE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut.o_ERROR_SEVERITY as SEGREGATION_TYPE_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut as EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPEOut
),

EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut.o_VALIDATION_PASS_FAIL as ENTITY_ID_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut.o_COMBINED_ERROR_MESSAGE as ENTITY_ID_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut.o_ERROR_SEVERITY as ENTITY_ID_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut as EXP_LDSA_VALIDATION_CHECKER_ENTITYIDOut
),

EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details as (
    select
        EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut.o_VALIDATION_PASS_FAIL as FINANCIAL_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut.o_COMBINED_ERROR_MESSAGE as FINANCIAL_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut.o_ERROR_SEVERITY as FINANCIAL_ACCOUNT_ERROR_SEVERITY
    from EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut as EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNTOut
),

dsJoiner_EXP_CONSOLIDATE_VALIDATIONS0Out as (
    select
        EXP_PASS_THROUGH_details.BATCH_ID as BATCH_ID,
        EXP_PASS_THROUGH_details.BATCH_DETAILS_ID as BATCH_DETAILS_ID,
        EXP_PASS_THROUGH_details.BATCH_START_DATE as BATCH_START_DATE,
        EXP_PASS_THROUGH_details.BUSINESS_DATE as BUSINESS_DATE,
        EXP_PASS_THROUGH_details.WORKFLOW_NAME as WORKFLOW_NAME,
        EXP_PASS_THROUGH_details.MAPPING_NAME as MAPPING_NAME,
        EXP_PASS_THROUGH_details.TARGET_NAME as TARGET_NAME,
        EXP_PASS_THROUGH_details.VALIDATION_NAME as VALIDATION_NAME,
        EXP_PASS_THROUGH_details.TMF_CODE as TMF_CODE,
        EXP_PASS_THROUGH_details.CMF_CODE as CMF_CODE,
        EXP_PASS_THROUGH_details.FINANICAL_ACCOUNT as FINANICAL_ACCOUNT,
        EXP_PASS_THROUGH_details.COLLATERAL_ACCOUNT as COLLATERAL_ACCOUNT,
        EXP_PASS_THROUGH_details.MARGIN_ACCOUNT as MARGIN_ACCOUNT,
        EXP_PASS_THROUGH_details.POSITION_ACCOUNT as POSITION_ACCOUNT,
        EXP_PASS_THROUGH_details.SEGREGATION_TYPE as SEGREGATION_TYPE,
        EXP_PASS_THROUGH_details.ACCOUNT_STRUCTURE_TYPE as ACCOUNT_STRUCTURE_TYPE,
        EXP_PASS_THROUGH_details.ENTITY_NAME as ENTITY_NAME,
        EXP_PASS_THROUGH_details.ENTITY_ID as ENTITY_ID,
        EXP_PASS_THROUGH_details.TOT_REC_COUNT as TOT_REC_COUNT,
        EXP_PASS_THROUGH_details.CurrentlyProcessedFileName as CurrentlyProcessedFileName,
        EXP_PASS_THROUGH_details.ENTITY_ID_ATTRIBUTE_NAME as ENTITY_ID_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details.ENTITY_ID_VALIDATION_PASS_FAIL as ENTITY_ID_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details.ENTITY_ID_ERROR_MESSAGE as ENTITY_ID_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details.ENTITY_ID_ERROR_SEVERITY as ENTITY_ID_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.TMF_CODE_ATTRIBUTE_NAME as TMF_CODE_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_TMF_master.TMF_CODE_VALIDATION_PASS_FAIL as TMF_CODE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_TMF_master.TMF_CODE_ERROR_MESSAGE as TMF_CODE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_TMF_master.TMF_CODE_ERROR_SEVERITY as TMF_CODE_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.CMF_CODE_ATTRIBUTE_NAME as CMF_CODE_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_CMF_details.CMF_CODE_VALIDATION_PASS_FAIL as CMF_CODE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_CMF_details.CMF_CODE_ERROR_MESSAGE as CMF_CODE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_CMF_details.CMF_CODE_ERROR_SEVERITY as CMF_CODE_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.FINANCIAL_ACCOUNT_ATTRIBUTE_NAME as FINANCIAL_ACCOUNT_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details.FINANCIAL_ACCOUNT_VALIDATION_PASS_FAIL as FINANCIAL_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details.FINANCIAL_ACCOUNT_ERROR_MESSAGE as FINANCIAL_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details.FINANCIAL_ACCOUNT_ERROR_SEVERITY as FINANCIAL_ACCOUNT_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.COLLATERAL_ACCOUNT_ATTRIBUTE_NAME as COLLATERAL_ACCOUNT_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details.COLLATERAL_ACCOUNT_VALIDATION_PASS_FAIL as COLLATERAL_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details.COLLATERAL_ACCOUNT_ERROR_MESSAGE as COLLATERAL_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details.COLLATERAL_ACCOUNT_ERROR_SEVERITY as COLLATERAL_ACCOUNT_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.MARGIN_ACCOUNT_ATTRIBUTE_NAME as MARGIN_ACCOUNT_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details.MARGIN_ACCOUNT_VALIDATION_PASS_FAIL as MARGIN_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details.MARGIN_ACCOUNT_ERROR_MESSAGE as MARGIN_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details.MARGIN_ACCOUNT_ERROR_SEVERITY as MARGIN_ACCOUNT_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.POSITION_ACCOUNT_ATTRIBUTE_NAME as POSITION_ACCOUNT_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details.POSITION_ACCOUNT_VALIDATION_PASS_FAIL as POSITION_ACCOUNT_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details.POSITION_ACCOUNT_ERROR_MESSAGE as POSITION_ACCOUNT_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details.POSITION_ACCOUNT_ERROR_SEVERITY as POSITION_ACCOUNT_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.SEGREGATION_TYPE_ATTRIBUTE_NAME as SEGREGATION_TYPE_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details.SEGREGATION_TYPE_VALIDATION_PASS_FAIL as SEGREGATION_TYPE_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details.SEGREGATION_TYPE_ERROR_MESSAGE as SEGREGATION_TYPE_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details.SEGREGATION_TYPE_ERROR_SEVERITY as SEGREGATION_TYPE_ERROR_SEVERITY,
        EXP_PASS_THROUGH_details.PRODUCT_FAMILY_ATTRIBUTE_NAME as PRODUCT_FAMILY_ATTRIBUTE_NAME,
        EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details.PRODUCT_FAMILY_VALIDATION_PASS_FAIL as PRODUCT_FAMILY_VALIDATION_PASS_FAIL,
        EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details.PRODUCT_FAMILY_ERROR_MESSAGE as PRODUCT_FAMILY_ERROR_MESSAGE,
        EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details.PRODUCT_FAMILY_ERROR_SEVERITY as PRODUCT_FAMILY_ERROR_SEVERITY,
        SC_EXP_EPOC_GENERATOROut.ID as ID,
        row_number() over (order by (select null)) as jkey
    from EXP_LDSA_VALIDATION_CHECKER_TMF_master as EXP_LDSA_VALIDATION_CHECKER_TMF_master
    inner join EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details as EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_COLLATERAL_ACCOUNT_details.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details as EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_MARGIN_ACCOUNT_details.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_CMF_details as EXP_LDSA_VALIDATION_CHECKER_CMF_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_CMF_details.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details as EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_PRODUCT_FAMILY_details.jkey
    inner join EXP_PASS_THROUGH_details as EXP_PASS_THROUGH_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_PASS_THROUGH_details.jkey
    inner join SC_EXP_EPOC_GENERATOROut as SC_EXP_EPOC_GENERATOROut
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = SC_EXP_EPOC_GENERATOROut.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details as EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_POSITION_ACCOUNT_details.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details as EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_SEGREGATION_TYPE_details.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details as EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_ENTITYID_details.jkey
    inner join EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details as EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details
        on EXP_LDSA_VALIDATION_CHECKER_TMF_master.jkey = EXP_LDSA_VALIDATION_CHECKER_FINANICAL_ACCOUNT_details.jkey
),

EXP_CONSOLIDATE_VALIDATIONSOut as (
    select
        BATCH_ID as BATCH_ID,
        BATCH_DETAILS_ID as BATCH_DETAILS_ID,
        BATCH_START_DATE as BATCH_START_DATE,
        BUSINESS_DATE as BUSINESS_DATE,
        WORKFLOW_NAME as WORKFLOW_NAME,
        MAPPING_NAME as MAPPING_NAME,
        TARGET_NAME as TARGET_NAME,
        VALIDATION_NAME as VALIDATION_NAME,
        TMF_CODE as TMF_CODE,
        CMF_CODE as CMF_CODE,
        FINANICAL_ACCOUNT as FINANICAL_ACCOUNT,
        COLLATERAL_ACCOUNT as COLLATERAL_ACCOUNT,
        MARGIN_ACCOUNT as MARGIN_ACCOUNT,
        POSITION_ACCOUNT as POSITION_ACCOUNT,
        SEGREGATION_TYPE as SEGREGATION_TYPE,
        ACCOUNT_STRUCTURE_TYPE as ACCOUNT_STRUCTURE_TYPE,
        ENTITY_NAME as ENTITY_NAME,
        ENTITY_ID as ENTITY_ID,
        TOT_REC_COUNT as TOT_REC_COUNT,
        CurrentlyProcessedFileName as CurrentlyProcessedFileName,
        '{{ var("FILE_NAME") }}' as FILE_NAME,
        'FILE NAME:' || FILE_NAME || '/' || 'BUSINESS_DATE:' || BUSINESS_DATE || '/' || 'TRADE_ACCOUNT:' || FINANICAL_ACCOUNT || '/' as v_ERROR_MESSAGE,
        ENTITY_ID_ATTRIBUTE_NAME as ENTITY_ID_ATTRIBUTE_NAME,
        ENTITY_ID_VALIDATION_PASS_FAIL as ENTITY_ID_VALIDATION_PASS_FAIL,
        ENTITY_ID_ERROR_SEVERITY as ENTITY_ID_ERROR_SEVERITY,
        v_ERROR_MESSAGE || ENTITY_ID_ERROR_MESSAGE as o_ENTITY_ID_ERROR_MESSAGE,
        TMF_CODE_ATTRIBUTE_NAME as TMF_CODE_ATTRIBUTE_NAME,
        TMF_CODE_VALIDATION_PASS_FAIL as TMF_CODE_VALIDATION_PASS_FAIL,
        TMF_CODE_ERROR_SEVERITY as TMF_CODE_ERROR_SEVERITY,
        v_ERROR_MESSAGE || TMF_CODE_ERROR_MESSAGE as O_TMF_CODE_ERROR_MESSAGE,
        CMF_CODE_ATTRIBUTE_NAME as CMF_CODE_ATTRIBUTE_NAME,
        CMF_CODE_VALIDATION_PASS_FAIL as CMF_CODE_VALIDATION_PASS_FAIL,
        CMF_CODE_ERROR_SEVERITY as CMF_CODE_ERROR_SEVERITY,
        v_ERROR_MESSAGE || CMF_CODE_ERROR_MESSAGE as O_CMF_CODE_ERROR_MESSAGE,
        FINANCIAL_ACCOUNT_ATTRIBUTE_NAME as FINANCIAL_ACCOUNT_ATTRIBUTE_NAME,
        FINANCIAL_ACCOUNT_VALIDATION_PASS_FAIL as FINANCIAL_ACCOUNT_VALIDATION_PASS_FAIL,
        FINANCIAL_ACCOUNT_ERROR_MESSAGE as FINANCIAL_ACCOUNT_ERROR_MESSAGE,
        FINANCIAL_ACCOUNT_ERROR_SEVERITY as FINANCIAL_ACCOUNT_ERROR_SEVERITY,
        v_ERROR_MESSAGE || FINANCIAL_ACCOUNT_ERROR_MESSAGE as O_FINANCIAL_ACCOUNT_ERROR_MESSAGE,
        COLLATERAL_ACCOUNT_ATTRIBUTE_NAME as COLLATERAL_ACCOUNT_ATTRIBUTE_NAME,
        COLLATERAL_ACCOUNT_VALIDATION_PASS_FAIL as COLLATERAL_ACCOUNT_VALIDATION_PASS_FAIL,
        COLLATERAL_ACCOUNT_ERROR_MESSAGE as COLLATERAL_ACCOUNT_ERROR_MESSAGE,
        COLLATERAL_ACCOUNT_ERROR_SEVERITY as COLLATERAL_ACCOUNT_ERROR_SEVERITY,
        v_ERROR_MESSAGE || COLLATERAL_ACCOUNT_ERROR_MESSAGE as O_COLLATERAL_ACCOUNT_ERROR_MESSAGE,
        MARGIN_ACCOUNT_ATTRIBUTE_NAME as MARGIN_ACCOUNT_ATTRIBUTE_NAME,
        MARGIN_ACCOUNT_VALIDATION_PASS_FAIL as MARGIN_ACCOUNT_VALIDATION_PASS_FAIL,
        MARGIN_ACCOUNT_ERROR_SEVERITY as MARGIN_ACCOUNT_ERROR_SEVERITY,
        v_ERROR_MESSAGE || MARGIN_ACCOUNT_ERROR_MESSAGE as O_MARGIN_ACCOUNT_ERROR_MESSAGE,
        POSITION_ACCOUNT_ATTRIBUTE_NAME as POSITION_ACCOUNT_ATTRIBUTE_NAME,
        POSITION_ACCOUNT_VALIDATION_PASS_FAIL as POSITION_ACCOUNT_VALIDATION_PASS_FAIL,
        POSITION_ACCOUNT_ERROR_SEVERITY as POSITION_ACCOUNT_ERROR_SEVERITY,
        v_ERROR_MESSAGE || POSITION_ACCOUNT_ERROR_MESSAGE as O_POSITION_ACCOUNT_ERROR_MESSAGE,
        SEGREGATION_TYPE_ATTRIBUTE_NAME as SEGREGATION_TYPE_ATTRIBUTE_NAME,
        SEGREGATION_TYPE_VALIDATION_PASS_FAIL as SEGREGATION_TYPE_VALIDATION_PASS_FAIL,
        SEGREGATION_TYPE_ERROR_MESSAGE as SEGREGATION_TYPE_ERROR_MESSAGE,
        SEGREGATION_TYPE_ERROR_SEVERITY as SEGREGATION_TYPE_ERROR_SEVERITY,
        v_ERROR_MESSAGE || SEGREGATION_TYPE_ERROR_MESSAGE as O_SEGREGATION_TYPE_ERROR_MESSAGE,
        PRODUCT_FAMILY_ATTRIBUTE_NAME as PRODUCT_FAMILY_ATTRIBUTE_NAME,
        PRODUCT_FAMILY_VALIDATION_PASS_FAIL as PRODUCT_FAMILY_VALIDATION_PASS_FAIL,
        PRODUCT_FAMILY_ERROR_SEVERITY as PRODUCT_FAMILY_ERROR_SEVERITY,
        v_ERROR_MESSAGE || PRODUCT_FAMILY_ERROR_MESSAGE as O_PRODUCT_FAMILY_ERROR_MESSAGE,
        case
            when ENTITY_ID_VALIDATION_PASS_FAIL = 'F'
                 or TMF_CODE_VALIDATION_PASS_FAIL = 'F'
                 or CMF_CODE_VALIDATION_PASS_FAIL = 'F'
                 or FINANCIAL_ACCOUNT_VALIDATION_PASS_FAIL = 'F'
                 or COLLATERAL_ACCOUNT_VALIDATION_PASS_FAIL = 'F'
                 or MARGIN_ACCOUNT_VALIDATION_PASS_FAIL = 'F'
                 or POSITION_ACCOUNT_VALIDATION_PASS_FAIL = 'F'
                 or SEGREGATION_TYPE_VALIDATION_PASS_FAIL = 'F'
                 or PRODUCT_FAMILY_VALIDATION_PASS_FAIL = 'F'
            then 1 else 0
        end as MD_ERROR_IND,
        ID as ID,
        case
            when regexp_instr(CurrentlyProcessedFileName, 'DCL') > 0 then 'DCLSA' else 'MATIF_MONEP'
        end as DATA_SET_NAME
    from dsJoiner_EXP_CONSOLIDATE_VALIDATIONS0Out as dsJoiner_EXP_CONSOLIDATE_VALIDATIONS0Out
),

DSR_FIL_ERROR_LOGICOut as (
    select
        EXP_CONSOLIDATE_VALIDATIONSOut.BATCH_ID as BATCH_ID,
        EXP_CONSOLIDATE_VALIDATIONSOut.BATCH_DETAILS_ID as BATCH_DETAILS_ID,
        EXP_CONSOLIDATE_VALIDATIONSOut.BATCH_START_DATE as BATCH_START_DATE,
        EXP_CONSOLIDATE_VALIDATIONSOut.BUSINESS_DATE as BUSINESS_DATE,
        EXP_CONSOLIDATE_VALIDATIONSOut.WORKFLOW_NAME as WORKFLOW_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.MAPPING_NAME as MAPPING_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.TARGET_NAME as TARGET_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.VALIDATION_NAME as VALIDATION_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.ENTITY_ID_ATTRIBUTE_NAME as ENTITY_ID_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.ENTITY_ID_ERROR_SEVERITY as ENTITY_ID_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.o_ENTITY_ID_ERROR_MESSAGE as ENTITY_ID_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.CurrentlyProcessedFileName as CurrentlyProcessedFileName,
        EXP_CONSOLIDATE_VALIDATIONSOut.ID as ID,
        EXP_CONSOLIDATE_VALIDATIONSOut.MD_ERROR_IND as MD_ERROR_IND,
        EXP_CONSOLIDATE_VALIDATIONSOut.TMF_CODE_ATTRIBUTE_NAME as TMF_CODE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.TMF_CODE_ERROR_SEVERITY as TMF_CODE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_TMF_CODE_ERROR_MESSAGE as O_TMF_CODE_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.CMF_CODE_ATTRIBUTE_NAME as CMF_CODE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.CMF_CODE_ERROR_SEVERITY as CMF_CODE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_CMF_CODE_ERROR_MESSAGE as O_CMF_CODE_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.FINANCIAL_ACCOUNT_ATTRIBUTE_NAME as FINANCIAL_ACCOUNT_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.FINANCIAL_ACCOUNT_ERROR_SEVERITY as FINANCIAL_ACCOUNT_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_FINANCIAL_ACCOUNT_ERROR_MESSAGE as O_FINANCIAL_ACCOUNT_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.COLLATERAL_ACCOUNT_ATTRIBUTE_NAME as COLLATERAL_ACCOUNT_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.COLLATERAL_ACCOUNT_ERROR_SEVERITY as COLLATERAL_ACCOUNT_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_COLLATERAL_ACCOUNT_ERROR_MESSAGE as O_COLLATERAL_ACCOUNT_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.MARGIN_ACCOUNT_ATTRIBUTE_NAME as MARGIN_ACCOUNT_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.MARGIN_ACCOUNT_ERROR_SEVERITY as MARGIN_ACCOUNT_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_MARGIN_ACCOUNT_ERROR_MESSAGE as O_MARGIN_ACCOUNT_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.POSITION_ACCOUNT_ATTRIBUTE_NAME as POSITION_ACCOUNT_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.POSITION_ACCOUNT_ERROR_SEVERITY as POSITION_ACCOUNT_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_POSITION_ACCOUNT_ERROR_MESSAGE as O_POSITION_ACCOUNT_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.SEGREGATION_TYPE_ATTRIBUTE_NAME as SEGREGATION_TYPE_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.SEGREGATION_TYPE_ERROR_SEVERITY as SEGREGATION_TYPE_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_SEGREGATION_TYPE_ERROR_MESSAGE as O_SEGREGATION_TYPE_ERROR_MESSAGE,
        EXP_CONSOLIDATE_VALIDATIONSOut.PRODUCT_FAMILY_ATTRIBUTE_NAME as PRODUCT_FAMILY_ATTRIBUTE_NAME,
        EXP_CONSOLIDATE_VALIDATIONSOut.PRODUCT_FAMILY_ERROR_SEVERITY as PRODUCT_FAMILY_ERROR_SEVERITY,
        EXP_CONSOLIDATE_VALIDATIONSOut.O_PRODUCT_FAMILY_ERROR_MESSAGE as O_PRODUCT_FAMILY_ERROR_MESSAGE
    from EXP_CONSOLIDATE_VALIDATIONSOut as EXP_CONSOLIDATE_VALIDATIONSOut
),

DSR_SC_mplt_DV_PROCESS_ERROR_DETAILSOut as (
    select
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_4,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_5,
        FIL_ERROR_LOGICOut.O_CMF_CODE_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_3,
        FIL_ERROR_LOGICOut.FINANCIAL_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_4,
        FIL_ERROR_LOGICOut.O_FINANCIAL_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_4,
        FIL_ERROR_LOGICOut.FINANCIAL_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_4,
        FIL_ERROR_LOGICOut.COLLATERAL_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_5,
        FIL_ERROR_LOGICOut.COLLATERAL_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_5,
        FIL_error_LOGICOut.O_COLLATERAL_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_5,
        FIL_ERROR_LOGICOut.MARGIN_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_6,
        FIL_ERROR_LOGICOut.O_MARGIN_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_6,
        FIL_ERROR_LOGICOut.MARGIN_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_6,
        FIL_ERROR_LOGICOut.POSITION_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_7,
        FIL_ERROR_LOGICOut.POSITION_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_7,
        FIL_ERROR_LOGICOut.O_POSITION_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_7,
        FIL_ERROR_LOGICOut.SEGREGATION_TYPE_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_8,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_6,
        FIL_ERROR_LOGICOut.BUSINESS_DATE as INPUT_BATCH_BUSINESS_DATE,
        FIL_ERROR_LOGICOut.BATCH_ID as INPUT_BATCH_ID,
        FIL_ERROR_LOGICOut.WORKFLOW_NAME as INPUT_WORKFLOW_NAME,
        FIL_ERROR_LOGICOut.BATCH_DETAILS_ID as INPUT_BATCH_DETAILS_ID,
        FIL_ERROR_LOGICOut.BATCH_START_DATE as INPUT_BATCH_START_DATE,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_7,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_8,
        FIL_ERROR_LOGICOut.TMF_CODE_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_2,
        FIL_ERROR_LOGICOut.O_TMF_CODE_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_2,
        FIL_ERROR_LOGICOut.TMF_CODE_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_2,
        FIL_ERROR_LOGICOut.CMF_CODE_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_3,
        FIL_ERROR_LOGICOut.CMF_CODE_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_3,
        FIL_ERROR_LOGICOut.SEGREGATION_TYPE_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_8,
        FIL_ERROR_LOGICOut.O_SEGREGATION_TYPE_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_8,
        FIL_ERROR_LOGICOut.PRODUCT_FAMILY_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_9,
        FIL_ERROR_LOGICOut.O_PRODUCT_FAMILY_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_9,
        FIL_ERROR_LOGICOut.PRODUCT_FAMILY_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_9,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_9,
        FIL_ERROR_LOGICOut.MAPPING_NAME as INPUT_MAPPING_NAME,
        FIL_ERROR_LOGICOut.TARGET_NAME as INPUT_TARGET_NAME,
        FIL_ERROR_LOGICOut.ID as INPUT_REFERENCE_RECORD_ID,
        FIL_ERROR_LOGICOut.CurrentlyProcessedFileName as INPUT_FILE_NAME,
        FIL_ERROR_LOGICOut.ENTITY_ID_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_1,
        FIL_ERROR_LOGICOut.ENTITY_ID_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_1,
        FIL_ERROR_LOGICOut.ENTITY_ID_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_1,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_1,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_2,
        FIL_ERROR_LOGICOut.VALIDATION_NAME as INPUT_ERROR_CODE_3
    from DSR_FIL_ERROR_LOGICOut as DSR_FIL_ERROR_LOGICOut
    where MD_ERROR_IND = 1
),

SC_mplt_DV_PROCESS_ERROR_DETAILSOut as (
    select
        BATCH_START_DATE as CREATED_TS,
        ID as ID,
        PROCESS_DATE as PROCESS_DATE,
        REFERENCE_BATCH_ID as REFERENCE_BATCH_ID,
        REFERENCE_PROCESS as REFERENCE_PROCESS,
        REFERENCE_SUB_PROCESS as REFERENCE_SUB_PROCESS,
        REFERENCE_TARGET_NAME as REFERENCE_TARGET_NAME,
        REFERENCE_RECORD_ID as REFERENCE_RECORD_ID,
        REFERENCE_BUSINESS_KEY as REFERENCE_BUSINESS_KEY,
        REFERENCE_ATTRIBUTE_NAME as REFERENCE_ATTRIBUTE_NAME,
        REFERENCE_ATTRIBUTE_VALUE as REFERENCE_ATTRIBUTE_VALUE,
        ERROR_CODE as ERROR_CODE,
        ERROR_SEVERITY as ERROR_SEVERITY,
        ADDITIONAL_DETAILS as ADDITIONAL_DETAILS,
        REFERENCE_BATCH_DETAILS_ID as REFERENCE_BATCH_DETAILS_ID,
        VALIDATION_NAME as INPUT_ERROR_CODE_4,
        VALIDATION_NAME as INPUT_ERROR_CODE_5,
        O_CMF_CODE_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_3,
        FINANCIAL_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_4,
        O_FINANCIAL_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_4,
        FINANCIAL_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_4,
        COLLATERAL_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_5,
        COLLATERAL_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_5,
        O_COLLATERAL_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_5,
        MARGIN_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_6,
        O_MARGIN_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_6,
        MARGIN_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_6,
        POSITION_ACCOUNT_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_7,
        POSITION_ACCOUNT_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_7,
        O_POSITION_ACCOUNT_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_7,
        SEGREGATION_TYPE_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_8,
        VALIDATION_NAME as INPUT_ERROR_CODE_6,
        BUSINESS_DATE as INPUT_BATCH_BUSINESS_DATE,
        BATCH_ID as INPUT_BATCH_ID,
        WORKFLOW_NAME as INPUT_WORKFLOW_NAME,
        BATCH_DETAILS_ID as INPUT_BATCH_DETAILS_ID,
        BATCH_START_DATE as INPUT_BATCH_START_DATE,
        VALIDATION_NAME as INPUT_ERROR_CODE_7,
        VALIDATION_NAME as INPUT_ERROR_CODE_8,
        TMF_CODE_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_2,
        O_TMF_CODE_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_2,
        TMF_CODE_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_2,
        CMF_CODE_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_3,
        CMF_CODE_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_3,
        SEGREGATION_TYPE_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_8,
        O_SEGREGATION_TYPE_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_8,
        PRODUCT_FAMILY_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_9,
        O_PRODUCT_FAMILY_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_9,
        PRODUCT_FAMILY_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_9,
        VALIDATION_NAME as INPUT_ERROR_CODE_9,
        MAPPING_NAME as INPUT_MAPPING_NAME,
        TARGET_NAME as INPUT_TARGET_NAME,
        ID as INPUT_REFERENCE_RECORD_ID,
        CurrentlyProcessedFileName as INPUT_FILE_NAME,
        ENTITY_ID_ATTRIBUTE_NAME as INPUT_ATTRIBUTE_NAME_1,
        ENTITY_ID_ERROR_SEVERITY as INPUT_ERROR_SEVERITY_1,
        ENTITY_ID_ERROR_MESSAGE as INPUT_ATTRIBUTE_VALUE_1,
        VALIDATION_NAME as INPUT_ERROR_CODE_1,
        VALIDATION_NAME as INPUT_ERROR_CODE_2,
        VALIDATION_NAME as INPUT_ERROR_CODE_3
    from DSR_SC_mplt_DV_PROCESS_ERROR_DETAILSOut as DSR_SC_mplt_DV_PROCESS_ERROR_DETAILSOut
),

DSR_SC_PROCESS_ERROR_DETAILSOut as (
    select
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ID as ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.PROCESS_DATE as PROCESS_DATE,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_BATCH_ID as REFERENCE_BATCH_ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_PROCESS as REFERENCE_PROCESS,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_SUB_PROCESS as REFERENCE_SUB_PROCESS,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_TARGET_NAME as REFERENCE_TARGET_NAME,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_RECORD_ID as REFERENCE_RECORD_ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_BUSINESS_KEY as REFERENCE_BUSINESS_KEY,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_ATTRIBUTE_NAME as REFERENCE_ATTRIBUTE_NAME,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_ATTRIBUTE_VALUE as REFERENCE_ATTRIBUTE_VALUE,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ERROR_CODE as ERROR_CODE,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ERROR_SEVERITY as ERROR_SEVERITY,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.ADDITIONAL_DETAILS as ADDITIONAL_DETAILS,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.REFERENCE_BATCH_DETAILS_ID as REFERENCE_BATCH_DETAILS_ID,
        SC_mplt_DV_PROCESS_ERROR_DETAILSOut.CREATED_TS as CREATED_TS
    from SC_mplt_DV_PROCESS_ERROR_DETAILSOut as SC_mplt_DV_PROCESS_ERROR_DETAILSOut
)

select
    ID as ID,
    PROCESS_DATE as PROCESS_DATE,
    REFERENCE_BATCH_ID as REFERENCE_BATCH_ID,
    REFERENCE_PROCESS as REFERENCE_PROCESS,
    REFERENCE_SUB_PROCESS as REFERENCE_SUB_PROCESS,
    REFERENCE_TARGET_NAME as REFERENCE_TARGET_NAME,
    REFERENCE_RECORD_ID as REFERENCE_RECORD_ID,
    REFERENCE_BUSINESS_KEY as REFERENCE_BUSINESS_KEY,
    REFERENCE_ATTRIBUTE_NAME as REFERENCE_ATTRIBUTE_NAME,
    REFERENCE_ATTRIBUTE_VALUE as REFERENCE_ATTRIBUTE_VALUE,
    ERROR_CODE as ERROR_CODE,
    ERROR_SEVERITY as ERROR_SEVERITY,
    ADDITIONAL_DETAILS as ADDITIONAL_DETAILS,
    REFERENCE_BATCH_DETAILS_ID as REFERENCE_BATCH_DETAILS_ID,
    CREATED_TS as CREATED_TS
from DSR_SC_PROCESS_ERROR_DETAILSOut as DSR_SC_PROCESS_ERROR_DETAILSOut