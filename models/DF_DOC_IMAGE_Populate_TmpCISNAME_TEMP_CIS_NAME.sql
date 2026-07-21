/* Transformation Name ==>TEMP_CIS_NAME ,Transformation Type ==>Target */

{{
    config(
        materialized='incremental',
        alias='TEMP_CIS_NAME',
        schema='',
        pre_hook="",
        post_hook="",
        incremental_strategy='append'
    )
}}

with SQLOut as (
    /* SubQuery from Source ==>SQL */
    /* transType: "Source" */
    select
        CUSTOMER_NUMBER,
        CUSTOMER_NAME
    from (
        select distinct
            CAR1.CUSTOMER_NUMBER,
            CUST.CUSTOMER_NAME
        from
            BRIOVIEW.CUSTOMER_ACCOUNT_RELATIONSHIP as CAR1
            join BRIOVIEW.CUSTOMER_ACCOUNT_RELATIONSHIP as CAR2
            join BRIOVIEW.CUSTOMER as CUST
            join BRIOVIEW.LOAN as LOAN
            join BRIOVIEW.DATE_LAST_BUS_DAY as LBD
        where
            CAR1.ACCOUNT_NUMBER = CAR2.ACCOUNT_NUMBER
            and CAR2.CUSTOMER_NUMBER = CUST.CUSTOMER_NUMBER
            and CAR1.SOURCE_SYSTEM_PROCESS_DATE = CAR2.SOURCE_SYSTEM_PROCESS_DATE
            and CAR1.ACCOUNT_NUMBER = LOAN.ACCOUNT_NUMBER
            and CAR1.SOURCE_SYSTEM_PROCESS_DATE = LOAN.SOURCE_SYSTEM_PROCESS_DATE
            and CAR1.SOURCE_SYSTEM = CAR2.SOURCE_SYSTEM
            and CAR1.SOURCE_SYSTEM = LOAN.SOURCE_SYSTEM
            and CAR1.PRODUCT_TYPE_CODE in ('AFSLN', 'LN', 'OLOAN')
            and CAR2.PRODUCT_TYPE_CODE in ('AFSLN', 'LN', 'OLOAN')
            and CAR1.RELATIONSHIP_TYPE_CODE in ('000', '022', '023', '059', '062', '064')
            and CAR2.RELATIONSHIP_TYPE_CODE in ('000', '022', '023', '059', '062', '064')
            and CAR1.SOURCE_SYSTEM_PROCESS_DATE = LBD.BUSINESS_DAY_DATE
            and CAR1.CUSTOMER_NUMBER <> CAR2.CUSTOMER_NUMBER
            and (CAR1.HISTORY_INDICATOR in (' ', 'B') or CAR1.HISTORY_INDICATOR is null)
            and (CAR2.HISTORY_INDICATOR in (' ', 'B') or CAR2.HISTORY_INDICATOR is null)

        union

        select distinct
            CAR3.CUSTOMER_NUMBER,
            CUST1.CUSTOMER_NAME
        from
            BRIOVIEW.CUSTOMER_ACCOUNT_RELATIONSHIP as CAR3
            join BRIOVIEW.CUSTOMER_ACCOUNT_RELATIONSHIP as CAR4
            join BRIOVIEW.CUSTOMER as CUST1
            join BRIOVIEW.COMMITMENT as CMMT
            join BRIOVIEW.DATE_LAST_BUS_DAY as LBD
        where
            CAR3.ACCOUNT_NUMBER = CAR4.ACCOUNT_NUMBER
            and CAR4.CUSTOMER_NUMBER = CUST1.CUSTOMER_NUMBER
            and CAR3.SOURCE_SYSTEM_PROCESS_DATE = CAR4.SOURCE_SYSTEM_PROCESS_DATE
            and CAR3.ACCOUNT_NUMBER = CMMT.ACCOUNT_NUMBER
            and CAR3.SOURCE_SYSTEM_PROCESS_DATE = CMMT.SOURCE_SYSTEM_PROCESS_DATE
            and CAR3.SOURCE_SYSTEM = CAR4.SOURCE_SYSTEM
            and CAR3.SOURCE_SYSTEM = CMMT.SOURCE_SYSTEM
            and CAR3.PRODUCT_TYPE_CODE in ('AFSLN', 'LN', 'OLOAN')
            and CAR4.PRODUCT_TYPE_CODE in ('AFSLN', 'LN', 'OLOAN')
            and CAR3.RELATIONSHIP_TYPE_CODE in ('000', '022', '023', '059', '062', '064')
            and CAR4.RELATIONSHIP_TYPE_CODE in ('000', '022', '023', '059', '062', '064')
            and CAR3.SOURCE_SYSTEM_PROCESS_DATE = LBD.BUSINESS_DAY_DATE
            and CAR3.CUSTOMER_NUMBER <> CAR4.CUSTOMER_NUMBER
            and (CAR3.HISTORY_INDICATOR in (' ', 'B') or CAR3.HISTORY_INDICATOR is null)
            and (CAR4.HISTORY_INDICATOR in (' ', 'B') or CAR4.HISTORY_INDICATOR is null)
        order by
            CAR3.CUSTOMER_NUMBER,
            CUST1.CUSTOMER_NAME
    )
),

DSR_QueryOut as (
    /* transType: "Expression" */
    select
        SQLOut.CUSTOMER_NUMBER as CIS_NUMBER,
        SQLOut.CUSTOMER_NAME as CUSTOMER_NAME
    from SQLOut
),

QueryOut as (
    /* transType: "Expression" */
    select
        CUSTOMER_NUMBER as CIS_NUMBER,
        CUSTOMER_NAME as CUSTOMER_NAME
    from DSR_QueryOut
)

select
    CIS_NUMBER as CIS_NUMBER,
    CUSTOMER_NAME as CUSTOMER_NAME
from QueryOut