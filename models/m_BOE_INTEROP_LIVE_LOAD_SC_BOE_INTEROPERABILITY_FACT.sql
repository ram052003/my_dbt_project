/*
Transformation Name ==> SC_BOE_INTEROPERABILITY_FACT
Transformation Type ==> Target
*/

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
    /* SubQuery from Source ==> SQ_SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACT */
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
        from (
            select
                BUSINESS_DATE,
                COLLATERAL_REC_SHORT_CODE,
                EXPOSURE_AMT,
                BASKET_NUMBER,
                EXPOSURE_REF,
                EXPOSURE_CURR_ID
            from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            where BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              and COLLATERAL_GIVER_SHORT_CODE = 'LCHGIVER'
        ) a
        inner join (
            select
                INSTRUMENT_GROUP,
                ISIN,
                SECURITY_VALUE,
                EXPOSURE_REF
            from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            where BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              and COLLATERAL_GIVER_SHORT_CODE is null
        ) b
            on a.EXPOSURE_REF = b.EXPOSURE_REF
        inner join dw_mart.currency_dim as c
            on c.ID = a.EXPOSURE_CURR_ID
        inner join application_variables as d
            on a.COLLATERAL_REC_SHORT_CODE = d.STRING_VALUE2
           and d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
           and 'Y' != (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
            )
        union
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
        from (
            select
                BUSINESS_DATE,
                COLLATERAL_REC_SHORT_CODE,
                EXPOSURE_AMT,
                BASKET_NUMBER,
                EXPOSURE_REF,
                EXPOSURE_CURR_ID
            from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            where BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              and COLLATERAL_GIVER_SHORT_CODE = 'LCHGIVER'
        ) a
        inner join (
            select
                INSTRUMENT_GROUP,
                ISIN,
                SECURITY_VALUE,
                EXPOSURE_REF,
                MARGIN_PERCENT
            from {{ source('reg_rep', 'RR_TRIPARTY_COLL_AND_EXP_FACT') }}
            where BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
              and COLLATERAL_GIVER_SHORT_CODE is null
        ) b
            on a.EXPOSURE_REF = b.EXPOSURE_REF
        inner join dw_mart.currency_dim as c
            on c.ID = a.EXPOSURE_CURR_ID
        inner join application_variables as d
            on a.COLLATERAL_REC_SHORT_CODE = d.STRING_VALUE2
           and d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
           and 'Y' != (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
            )
        group by
            date_trunc('DAY', a.BUSINESS_DATE),
            d.STRING_VALUE3,
            d.STRING_VALUE5,
            d.STRING_VALUE4,
            b.INSTRUMENT_GROUP,
            to_varchar(b.ISIN),
            c.CURRENCY_MNEMONIC
        union
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
        inner join application_variables as d
            on a.MNEMONIC = d.STRING_VALUE
        where d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          and a.BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
          and 'Y' = (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
            )
        union
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
        inner join application_variables as d
            on a.MNEMONIC = d.STRING_VALUE
        where d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          and a.BUSINESS_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
          and 'Y' = (
                select STRING_VALUE
                from APPLICATION_VARIABLES
                where VARIABLE_NAME = 'BOE_INTEROP_STRATEGIC_FLAG'
            )
    )
),

SQ_SC_RR_POSITIONS_FACTOut as (
    /* SubQuery from Source ==> SQ_SQ_SC_RR_POSITIONS_FACT */
    select
        NOTIONAL_AMT,
        INTEROP_ID,
        INTEROP_DESC,
        CURRENCY_MNEMONIC
    from (
        select
            sum(
                decode(
                    nvl(INSTRUMENTS_DIM.SECURITY_TYPE, 'NONTERM'),
                    'TERM',
                    abs(MTM_NOTIONAL),
                    0
                )
            ) as NOTIONAL_AMT,
            application_variables.STRING_VALUE5 as INTEROP_ID,
            application_variables.STRING_VALUE4 as INTEROP_DESC,
            CURRENCY_DIM.CURRENCY_MNEMONIC
        from {{ source('reg_rep', 'RR_POSITIONS_FACT') }}
        inner join INSTRUMENTS_DIM
            on RR_POSITIONS_FACT.INSTRUMENT_ID = INSTRUMENTS_DIM.ID
        inner join CURRENCY_DIM
            on RR_POSITIONS_FACT.MARGIN_CURRENCY_ID = CURRENCY_DIM.ID
        inner join application_variables
            on COUNTERPARTIES_DIM.CLEARING_MEMBER_MNEMONIC = APPLICATION_VARIABLES.STRING_VALUE5
        inner join COUNTERPARTIES_DIM
            on RR_POSITIONS_FACT.MEMBER_COUNTERPARTY_ID = COUNTERPARTIES_DIM.ID
        where BUSINESS_DATE_ID = (
                select distinct ID
                from {{ source('dw_mart', 'CALENDAR_DIM') }}
                where CAL_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
                  and TYPE = 'LTD_STD'
            )
          and exchange_id in (
                select id
                from {{ source('dw_mart', 'exchange_dim') }}
                where EXCHANGE_MNEMONIC = 'ECL'
            )
        group by
            application_variables.STRING_VALUE5,
            application_variables.STRING_VALUE4,
            CURRENCY_DIM.CURRENCY_MNEMONIC
    )
),

SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut as (
    /* SubQuery from Source ==> SQ_SQ_SC_RR_TRADE_AND_TRANSACTION_FACT */
    select
        BUSINESS_DATE_TIME,
        TrdsClrd,
        INTEROP_ID,
        INTEROP_DESC
    from (
        select
            a.BUSINESS_DATE_TIME,
            round(count(a.ID) / 2) as TrdsClrd,
            d.STRING_VALUE5 as INTEROP_ID,
            d.STRING_VALUE4 as INTEROP_DESC
        from {{ source('reg_rep', 'RR_TRADE_AND_TRANSACTION_FACT') }} a
        inner join COUNTERPARTIES_DIM b
            on a.MEMBER_COUNTERPARTY_ID = b.ID
        inner join application_variables d
            on b.CLEARING_MEMBER_MNEMONIC = d.STRING_VALUE5
        where d.VARIABLE_NAME = 'BOE_INTEROP_MNEMONIC'
          and a.exchange_id in (
                select id
                from {{ source('dw_mart', 'exchange_dim') }}
                where EXCHANGE_MNEMONIC = 'ECL'
            )
          and TRADE_ACTION_TYPE = 'NEW'
          and BUSINESS_DATE_ID = (
                select distinct ID
                from {{ source('dw_mart', 'CALENDAR_DIM') }}
                where CAL_DATE = to_date('{{ var("BUSINESS_DATE") }}', 'YYYYMMDD')
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
        SQ_SC_RR_POSITIONS_FACTOut.NOTIONAL_AMT as NOTIONAL_AMT,
        SQ_SC_RR_POSITIONS_FACTOut.INTEROP_ID as INTEROP_ID1,
        SQ_SC_RR_POSITIONS_FACTOut.INTEROP_DESC as INTEROP_DESC1,
        SQ_SC_RR_POSITIONS_FACTOut.CURRENCY_MNEMONIC as CURRENCY_MNEMONIC
    from SQ_SC_RR_POSITIONS_FACTOut
),

JNR_gross_CLRIDOut as (
    select
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.BUSINESS_DATE_TIME as BUSINESS_DATE_TIME,
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.TrdsClrd as TrdsClrd,
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.INTEROP_ID as INTEROP_ID,
        SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.INTEROP_DESC as INTEROP_DESC,
        SQ_SC_RR_POSITIONS_FACT_master.NOTIONAL_AMT as NOTIONAL_AMT,
        SQ_SC_RR_POSITIONS_FACT_master.INTEROP_ID1 as INTEROP_ID1,
        SQ_SC_RR_POSITIONS_FACT_master.INTEROP_DESC1 as INTEROP_DESC1,
        SQ_SC_RR_POSITIONS_FACT_master.CURRENCY_MNEMONIC as CURRENCY_MNEMONIC,
        row_number() over (order by 1) as jkey
    from SQ_SC_RR_POSITIONS_FACT_master
    inner join SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut
        on SQ_SC_RR_TRADE_AND_TRANSACTION_FACTOut.INTEROP_ID = SQ_SC_RR_POSITIONS_FACT_master.INTEROP_ID1
),

exp_TTL_SECURITYOut as (
    select
        BUSINESS_DATE as BUSINESS_DATE,
        null as Trdsclrd,
        EXCHANGE as EXCHANGE,
        INTEROP_ID as INTEROP_ID,
        INTEROP_DESC as INTEROP_DESC,
        EXPOSURE_AMT as EXPOSURE_AMT,
        CURRENCY_MNEMONIC as CURRENCY_MNEMONIC,
        Coll_Type as Coll_Type,
        FinInstrmId as FinInstrmId,
        issr as issr,
        Trpty_MktVal_AMT as Trpty_MktVal_AMT,
        Trpty_MktVal_CCY as Trpty_MktVal_CCY,
        MktVal_AMT as MktVal_AMT,
        null as NOTIONAL_AMT,
        PstHrcutVal_AMT as PstHrcutVal_AMT,
        PstHrcutVal_CCY as PstHrcutVal_CCY,
        CollRqrmnt as CollRqrmnt,
        null as GROSS_NOTIONAL_CURRENCY
    from SQ_SC_RR_TRIPARTY_COLL_AND_EXP_FACTOut
)

/* Additional transformations would follow here */