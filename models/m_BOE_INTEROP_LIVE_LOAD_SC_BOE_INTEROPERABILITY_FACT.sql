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

with SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACTOut as (
    /* SubQuery from Source ==>SQ_SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACT */
    select
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
    from (
        select distinct
            date_trunc('DAY', a.BUSINESS_DATE) as BUSINESS_DATE,
            d.STRING_VALUE3 as EXCHANGE,
            d.STRING_VALUE5 as INTEROP_ID,
            d.STRING_VALUE4 as INTEROP_DESC,
            a.EXPOSURE_AMT as EXPOSURE_AMT,
            c.CURRENCY_MNEMONIC as CURRENCY_MNEMONIC,
            null as Coll_Type,
            null as FinInstrmId,
            null as issr,
            null as Trpty_MktVal_AMT,
            null as Trpty_MktVal_CCY,
            null as MktVal_AMT,
            null as PstHrcutVal_AMT,
            null as PstHrcutVal_CCY,
            'MGIN' as CollRqrmnt
        from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }} a
        inner join (
            select
                INSTRUMENT_GROUP,
                ISIN,
                SECURITY_VALUE,
                EXPOSURE_REF
            from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            where BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
              and COLLATERAL_GIVER_SHORT_CODE is null
        ) b
            on a.EXPOSURE_REF = b.EXPOSURE_REF
        inner join dw_mart.currency_dim c
            on c.ID = a.EXPOSURE_CURR_ID
        inner join application_variables d
            on a.COLLATERAL_REC_SHORT_CODE = d.STRING_VALUE2
           and d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
           and 'Y' != (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
           )
        where a.BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
          and a.COLLATERAL_GIVER_SHORT_CODE = 'LCHGIVER'

        union all

        select distinct
            date_trunc('DAY', a.BUSINESS_DATE) as BUSINESS_DATE,
            d.STRING_VALUE3 as EXCHANGE,
            d.STRING_VALUE5 as INTEROP_ID,
            d.STRING_VALUE4 as INTEROP_DESC,
            null as EXPOSURE_AMT,
            null as CURRENCY_MNEMONIC,
            b.INSTRUMENT_GROUP as Coll_Type,
            to_varchar(b.ISIN) as FinInstrmId,
            '00000000000000000000' as issr,
            max(a.EXPOSURE_AMT) as Trpty_MktVal_AMT,
            c.CURRENCY_MNEMONIC as Trpty_MktVal_CCY,
            sum((1 + b.MARGIN_PERCENT) * b.SECURITY_VALUE) as MktVal_AMT,
            sum(b.SECURITY_VALUE) as PstHrcutVal_AMT,
            c.CURRENCY_MNEMONIC as PstHrcutVal_CCY,
            'MGIN' as CollRqrmnt
        from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }} a
        inner join (
            select
                INSTRUMENT_GROUP,
                ISIN,
                SECURITY_VALUE,
                EXPOSURE_REF,
                MARGIN_PERCENT
            from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            where BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
              and COLLATERAL_GIVER_SHORT_CODE is null
        ) b
            on a.EXPOSURE_REF = b.EXPOSURE_REF
        inner join dw_mart.currency_dim c
            on c.ID = a.EXPOSURE_CURR_ID
        inner join application_variables d
            on a.COLLATERAL_REC_SHORT_CODE = d.STRING_VALUE2
           and d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
           and 'Y' != (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
           )
        where a.BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
          and a.COLLATERAL_GIVER_SHORT_CODE = 'LCHGIVER'
        group by
            date_trunc('DAY', a.BUSINESS_DATE),
            d.STRING_VALUE3,
            d.STRING_VALUE5,
            d.STRING_VALUE4,
            b.INSTRUMENT_GROUP,
            to_varchar(b.ISIN),
            c.CURRENCY_MNEMONIC

        union all

        select distinct
            date_trunc('DAY', a.BUSINESS_DATE) as BUSINESS_DATE,
            d.STRING_VALUE3 as EXCHANGE,
            d.STRING_VALUE5 as INTEROP_ID,
            d.STRING_VALUE4 as INTEROP_DESC,
            a.TRANSACTION_AMT as EXPOSURE_AMT,
            a.TRANSACTION_CCY as CURRENCY_MNEMONIC,
            null as Coll_Type,
            null as FinInstrmId,
            null as issr,
            null as Trpty_MktVal_AMT,
            null as Trpty_MktVal_CCY,
            null as MktVal_AMT,
            null as PstHrcutVal_AMT,
            null as PstHrcutVal_CCY,
            'MGIN' as CollRqrmnt
        from {{ source('reg_rep', 'RR_BNK_TRIPTYALC_INVST_FACT') }} a
        inner join application_variables d
            on a.MNEMONIC = d.STRING_VALUE
           and d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
        where d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          and a.BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
          and 'Y' = (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
           )

        union all

        select distinct
            date_trunc('DAY', a.BUSINESS_DATE) as BUSINESS_DATE,
            d.STRING_VALUE3 as EXCHANGE,
            d.STRING_VALUE5 as INTEROP_ID,
            d.STRING_VALUE4 as INTEROP_DESC,
            null as EXPOSURE_AMT,
            null as CURRENCY_MNEMONIC,
            a.COLLATERAL_GROUP as Coll_Type,
            to_varchar(a.ISIN) as FinInstrmId,
            '00000000000000000000' as issr,
            a.TRANSACTION_AMT as Trpty_MktVal_AMT,
            a.TRANSACTION_CCY as Trpty_MktVal_CCY,
            a.PRE_HC_MV_TX_CCY_AMT as MktVal_AMT,
            a.POST_HC_MV_TX_CCY_AMT as PstHrcutVal_AMT,
            a.TRANSACTION_CCY as PstHrcutVal_CCY,
            'MGIN' as CollRqrmnt
        from {{ source('reg_rep', 'RR_BNK_TRIPTYALC_INVST_FACT') }} a
        inner join application_variables d
            on a.MNEMONIC = d.STRING_VALUE
           and d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
        where a.BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
          and 'Y' = (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
           )
    )
),

SQ_SC_RR_POSITIONS_FACTOut as (
    /* SubQuery from Source ==>SQ_SQ_SC_RR_POSITIONS_FACT */
    select
        NOTIONAL_AMT,
        INTEROP_ID,
        INTEROP_DESC,
        CURRENCY_MNEMONIC
    from (
        select
            sum(
                decode(
                    nvl(INSTRUMENTS_DIM.SECURITY_TYPE,'NONTERM'),
                    'TERM',
                    abs(MTM_NOTIONAL),
                    0
                )
            ) as NOTIONAL_AMT,
            APPLICATION_VARIABLES.STRING_VALUE5 as INTEROP_ID,
            APPLICATION_VARIABLES.STRING_VALUE4 as INTEROP_DESC,
            CURRENCY_DIM.CURRENCY_MNEMONIC
        from {{ source('reg_rep', 'RR_POSITIONS_FACT') }} RR_POSITIONS_FACT
        inner join INSTRUMENTS_DIM
            on RR_POSITIONS_FACT.INSTRUMENT_ID = INSTRUMENTS_DIM.ID
        inner join CURRENCY_DIM
            on RR_POSITIONS_FACT.MARGIN_CURRENCY_ID = CURRENCY_DIM.ID
        inner join APPLICATION_VARIABLES
            on COUNTERPARTIES_DIM.CLEARING_MEMBER_MNEMONIC = APPLICATION_VARIABLES.STRING_VALUE5
        inner join COUNTERPARTIES_DIM
            on RR_POSITIONS_FACT.MEMBER_COUNTERPARTY_ID = COUNTERPARTIES_DIM.ID
        where RR_POSITIONS_FACT.BUSINESS_DATE_ID = (
                select distinct ID
                from {{ source('dw_mart', 'CALENDAR_DIM') }}
                where CAL_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
                  and TYPE = 'LTD_STD'
            )
          and RR_POSITIONS_FACT.EXCHANGE_ID in (
                select id
                from {{ source('dw_mart', 'exchange_dim') }}
                where EXCHANGE_MNEMONIC = 'ECL'
            )
        group by
            APPLICATION_VARIABLES.STRING_VALUE5,
            APPLICATION_VARIABLES.STRING_VALUE4,
            CURRENCY_DIM.CURRENCY_MNEMONIC
    )
),

SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut as (
    /* SubQuery from Source ==>SQ_SQ_SC_RR_TRADE_AND_TRANSACTION_FACT */
    select
        BUSINESS_DATE_TIME,
        TrdsClrd,
        INTEROP_ID,
        INTEROP_DESC
    from (
        select
            a.BUSINESS_DATE_TIME,
            round(count(a.ID)/2) as TrdsClrd,
            d.STRING_VALUE5 as INTEROP_ID,
            d.STRING_VALUE4 as INTEROP_DESC
        from {{ source('reg_rep', 'RR_TRADE_AND_TRANSACTION_FACT') }} a
        inner join COUNTERPARTIES_DIM b
            on a.MEMBER_COUNTERPARTY_ID = b.ID
        inner join application_variables d
            on b.CLEARING_MEMBER_MNEMONIC = d.STRING_VALUE5
        where d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          and a.EXCHANGE_ID in (
                select id
                from {{ source('dw_mart', 'exchange_dim') }}
                where EXCHANGE_MNEMONIC = 'ECL'
            )
          and a.TRADE_ACTION_TYPE = 'NEW'
          and a.BUSINESS_DATE_ID = (
                select distinct ID
                from {{ source('dw_mart', 'CALENDAR_DIM') }}
                where CAL_DATE = to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD')
                  and TYPE = 'LTD_STD'
            )
        group by
            a.BUSINESS_DATE_TIME,
            d.STRING_VALUE5,
            d.STRING_VALUE4
    )
),

SQ_SC_RR_POSITIONS_FACT_master as (
    select
        NOTIONAL_AMT,
        INTEROP_ID as INTEROP_ID1,
        INTEROP_DESC as INTEROP_DESC1,
        CURRENCY_MNEMONIC
    from SQ_SC_RR_POSITIONS_FACTOut
),

JNR_gross_CLRIDOut as (
    select
        t.BUSINESS_DATE_TIME,
        t.TrdsClrd,
        t.INTEROP_ID,
        t.INTEROP_DESC,
        p.NOTIONAL_AMT,
        p.INTEROP_ID1,
        p.INTEROP_DESC1,
        p.CURRENCY_MNEMONIC,
        row_number() over (order by 1) as jkey
    from SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut t
    inner join SQ_SC_RR_POSITIONS_FACT_master p
        on t.INTEROP_ID = p.INTEROP_ID1
),

exp_TTL_SECURITYOut as (
    select
        BUSINESS_DATE,
        null as Trdsclrd,
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
        null as NOTIONAL_AMT,
        PstHrcutVal_AMT,
        PstHrcutVal_CCY,
        CollRqrmnt,
        null as GROSS_NOTIONAL_CURRENCY
    from SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACTOut
),

DSR_exp_TTL_SECURITY1Out as (
    select
        j.BUSINESS_DATE_TIME as BUSINESS_DATE,
        j.TrdsClrd as Trdsclrd,
        j.INTEROP_ID as INTEROP_ID,
        j.INTEROP_DESC as INTEROP_DESC,
        j.NOTIONAL_AMT as NOTIONAL_AMT,
        j.CURRENCY_MNEMONIC as CURRENCY_MNEMONIC2
    from JNR_gross_CLRIDOut j
),

exp_TTL_SECURITY1Out as (
    select
        BUSINESS_DATE,
        Trdsclrd,
        'ECL' as EXCHANGE,
        INTEROP_ID,
        INTEROP_DESC,
        null as EXPOSURE_AMT,
        null as CURRENCY_MNEMONIC,
        null as Coll_Type,
        null as FinInstrmId,
        '00000000000000000000' as issr,
        null as Trpty_MktVal_AMT,
        null as Trpty_MktVal_CCY,
        null as MktVal_AMT,
        NOTIONAL_AMT,
        null as PstHrcutVal_AMT,
        CURRENCY_MNEMONIC2 as CURRENCY_MNEMONIC,
        null as CURRENCY_MNEMONIC1,
        null as PstHrcutVal_CCY,
        'Margin' as CollRqrmnt
    from DSR_exp_TTL_SECURITY1Out
),

Union_ALL_DATEOut as (
    select
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
    from exp_TTL_SECURITYOut

    union all

    select
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
    from exp_TTL_SECURITY1Out
),

SC_SEQ_INTEROP1Out as (
    select
        INTEROP_ID,
        CURRENCY_MNEMONIC,
        EXCHANGE,
        GROSS_NOTIONAL_CURRENCY,
        Trdsclrd,
        FinInstrmId,
        Trpty_MktVal_CCY,
        PstHrcutVal_AMT,
        MktVal_AMT,
        issr,
        Coll_Type,
        NOTIONAL_AMT,
        PstHrcutVal_CCY,
        Trpty_MktVal_AMT,
        EXPOSURE_AMT,
        INTEROP_DESC,
        CollRqrmnt
    from Union_ALL_DATEOut
),

Target_Table_Name_SK_Max_Value as (
    select max(GENERATED_SEQ) as max_generated_seq
    from {{ this }}
),

DSR_EXP_FINAL1Out as (
    select
        (select max_generated_seq from Target_Table_Name_SK_Max_Value) + row_number() over (order by (select null)) as GENERATED_SEQ,
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
    from SC_SEQ_INTEROP1Out
),

EXP_FINAL1Out as (
    select
        GENERATED_SEQ,
        lpad(GENERATED_SEQ, 9, '0') as v_SEQ_LPAD_TO_9,
        datediff(
            'SECOND',
            to_timestamp('01/01/1970 00:00:00','DD-MM-YYYY HH24:MI:SS'),
            current_timestamp()
        ) as v_EPOC_TIME,
        v_EPOC_TIME || v_SEQ_LPAD_TO_9 as v_CONCATE_ID,
        v_CONCATE_ID as ID,
        '{{ var("BATCH_ID") }}' as BATCH_ID,
        dsf_1.ID as BATCH_DETAILS_ID,
        date_trunc('DAY', to_date(ltrim(rtrim('{{ var("BATCH_BUSINESS_DATE") }}')),'YYYYMMDD')) as v_BATCH_BUSINESS_DATE,
        v_BATCH_BUSINESS_DATE as BATCH_BUSINESS_DATE,
        dsf_2.ID as BUSINESS_DATE_ID,
        count(*) over () + 1 as v_TOT_REC_COUNT,
        to_date('{{ var("BUSINESS_DATE") }}','YYYYMMDD') as BUSINESS_DATE,
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
        null as SUBMIT_FLAG,
        GROSS_NOTIONAL_CURRENCY,
        0 as ERROR_INDICATOR
    from DSR_EXP_FINAL1Out
    left join (
        select ID, BATCH_ID, TARGET_TABLE_NAME, FILE_NAME
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
                        BATCH_DETAILS.STATUS as STATUS,
                        BATCH_DETAILS.ACTIVE_FLAG as ACTIVE_FLAG,
                        BATCH_DETAILS.BATCH_DETAILS_START_TIME as BATCH_DETAILS_START_TIME,
                        rank() over (partition by BATCH_ID, TARGET_TABLE_NAME, FILE_NAME order by BATCH_DETAILS_START_TIME desc) as RANK
                    from BATCH_DETAILS
                    where RANK = 1
                      and STATUS = 'I'
                      and ACTIVE_FLAG = 1
                )
            )
        )
        where rnk_lst = 1
    ) dsf_1
        on dsf_1.BATCH_ID = '{{ var("BATCH_ID") }}'
       and dsf_1.TARGET_TABLE_NAME = '{{ var("TARGET_NAME") }}'
       and dsf_1.FILE_NAME = '{{ var("SOURCE_NAME") }}'
    left join (
        select ID, CAL_DATE, TYPE
        from (
            select
                ID,
                CAL_DATE,
                TYPE,
                row_number() over (partition by CAL_DATE, TYPE order by rnk_fst desc nulls last) as rnk_lst
            from (
                select
                    ID,
                    CAL_DATE,
                    TYPE,
                    row_number() over (partition by CAL_DATE, TYPE order by CAL_DATE, TYPE nulls last) as rnk_fst
                from CALENDAR_DIM
            )
        )
        where rnk_lst = 1
    ) dsf_2
        on dsf_2.TYPE = 'LTD_STD'
       and dsf_2.CAL_DATE = v_BATCH_BUSINESS_DATE
),

DSR_SC_BOE_INTEROPERABILITY_FACTOut as (
    select
        ID,
        BATCH_ID,
        BUSINESS_DATE,
        BUSINESS_DATE_ID,
        BATCH_DETAILS_ID,
        EXCHANGE,
        INTEROP_ID,
        INTEROP_DESC,
        EXPOSURE_AMT as TtlInitlMrgn_AMT,
        CURRENCY_MNEMONIC as TtlInitlMrgn_CCY,
        Trdsclrd as TrdsClrd,
        NOTIONAL_AMT as GrssNtnlAmt_AMT,
        GROSS_NOTIONAL_CURRENCY as GrssNtnlAmt_CCY,
        PstHrcutVal_AMT,
        PstHrcutVal_CCY,
        Trpty_MktVal_AMT,
        Trpty_MktVal_CCY,
        Coll_Type,
        FinInstrmId,
        issr,
        MktVal_AMT as MktVal_AMT,
        MktVal_AMT as MktVal_CCY,
        CollRqrmnt,
        SUBMIT_FLAG,
        ERROR_INDICATOR
    from EXP_FINAL1Out
)

select
    ID,
    BATCH_ID,
    BUSINESS_DATE,
    BUSINESS_DATE_ID,
    BATCH_DETAILS_ID,
    EXCHANGE,
    INTEROP_ID,
    INTEROP_DESC,
    TtlInitlMrgn_AMT,
    TtlInitlMrgn_CCY,
    TrdsClrd,
    GrssNtnlAmt_AMT,
    GrssNtnlAmt_CCY,
    PstHrcutVal_AMT,
    PstHrcutVal_CCY,
    Trpty_MktVal_AMT,
    Trpty_MktVal_CCY,
    Coll_Type,
    FinInstrmId,
    issr,
    MktVal_AMT,
    MktVal_CCY,
    CollRqrmnt,
    SUBMIT_FLAG,
    ERROR_INDICATOR
from DSR_SC_BOE_INTEROPERABILITY_FACTOut