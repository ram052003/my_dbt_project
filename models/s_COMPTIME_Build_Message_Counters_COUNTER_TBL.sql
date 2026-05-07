/* Transformation Name ==>COUNTER_TBL ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='COUNTER_TBL',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append')
}}

With SQ_U0287D01Out as (
	Select SSN, 
		NAME, 
		CURRENT_ACCT, 
		CURRENT_ORG, 
		FLSA_STATUS, 
		COMP_TIME_CUR_BAL, 
		COMP_TIME_YEAR_EARNED, 
		PP_END_DATE, 
		DAILY_DATE_EARNED, 
		COMP_TIME_RATE, 
		COMP_TIME_HOURS, 
		COMP_TIME_UNDEF
 from 
{{ source ('FlatFile_catalog_1','U0287D01') }} 
 ),

PAY_PERIODOut as (
	Select EDW_CREATE_DTM, 
		EDW_CREATE_USER , 
		in_CURR_PP_FLAG, 
		PP_NUM, 
		PP_END_YEAR, 
		PP_START_DTE, 
		PP_END_DTE, 
		LV_NUM, 
		LV_YEAR, 
		PAY_DTE, 
		CURR_PP_FLAG
 from 
{{ source ('lkpSource_1','PAY_PERIOD') }}
 ),

exp_InitialOut as (
	
Select 
	SSN as SSN,
	 NAME as NAME,
	 DECODE ( TRUE,
	 TRY_TO_NUMBER ( SSN ) IS NOT NULL,
	 'D',
	 'NO' ) as o_RECORD_TYPE_FLAG
From SQ_U0287D01Out as SQ_U0287D01Out),

DSR_fil_DetailOut as (
	
Select 
	exp_InitialOut.SSN as SSN,
	 exp_InitialOut.o_RECORD_TYPE_FLAG as RECORD_TYPE_FLAG
From exp_InitialOut as exp_InitialOut),

fil_DetailOut as (
	Select 
 * 
From DSR_fil_DetailOut as DSR_fil_DetailOut Where RECORD_TYPE_FLAG = 'D'),

agg_ALL_RECORDS_Out as (
	 
 select 
	COUNT ( SSN ) as o_DETAIL_RECORD_COUNT
 from fil_DetailOut
),
agg_ALL_RECORDS_NonGroupBy as (
 select 
  	SSN,
	 RECORD_TYPE_FLAG,
	 RECORD_TYPE
 from fil_DetailOut
 qualify row_number() over (order by SSN desc) = 1
),
agg_ALL_RECORDSOut as (
 select 
	dsf_1.o_DETAIL_RECORD_COUNT,
	 dsf_2.SSN,
	 dsf_2.RECORD_TYPE_FLAG,
	 dsf_2.RECORD_TYPE
 from agg_ALL_RECORDS_Out dsf_1
 cross join agg_ALL_RECORDS_NonGroupBy dsf_2
)
),

DSR_exp_Detail_CountOut as (
	
Select 
	agg_ALL_RECORDSOut.o_DETAIL_RECORD_COUNT as DETAIL_RECORD_COUNT
From agg_ALL_RECORDSOut as agg_ALL_RECORDSOut),

exp_Detail_CountOut as (
	
Select 
	DETAIL_RECORD_COUNT as DETAIL_RECORD_COUNT,
	 'Y' as o_CURR_PP_FLAG
From DSR_exp_Detail_CountOut as DSR_exp_Detail_CountOut),

lkp_PAY_PERIODOut as (
	Select 
	PP_NUM as PP_NUM,
	 PP_END_YEAR as PP_END_YEAR,
	 exp_Detail_Count.o_CURR_PP_FLAG AS in_CURR_PP_FLAG 
FROM
	exp_Detail_CountOut as exp_Detail_CountOut 
 LEFT JOIN PAY_PERIODOut
ON
	CURR_PP_FLAG = in_CURR_PP_FLAG
 
),

lkp_PAY_PERIOD_details as (
  Select
  	lkp_PAY_PERIODOut.PP_NUM as lkp_PP_NUM,
	 lkp_PAY_PERIODOut.PP_END_YEAR as lkp_PP_END_YEAR
  From lkp_PAY_PERIODOut as lkp_PAY_PERIODOut
),
dsJoiner_exp_Counters0Out as (
Select 
	exp_Detail_CountOut.DETAIL_RECORD_COUNT as DETAIL_RECORD_COUNT,
	 lkp_PAY_PERIOD_details.lkp_PP_NUM as lkp_PP_NUM,
	 lkp_PAY_PERIOD_details.lkp_PP_END_YEAR as lkp_PP_END_YEAR,
	 ROW_NUMBER ( ) OVER ( ORDER BY 1 ) as jkey 
From
exp_Detail_CountOut as exp_Detail_CountOut inner JOIN lkp_PAY_PERIOD_details as lkp_PAY_PERIOD_details 
ON
lkp_PAY_PERIOD_details.jkey = exp_Detail_CountOut.jkey 
),

exp_CountersOut as (
	
Select 
	'Number of detail records from the COMP TIME file.' as o_COUNTER_DESCRIPTION_1,
	 DETAIL_RECORD_COUNT as DETAIL_RECORD_COUNT,
	 lkp_PP_NUM as lkp_PP_NUM,
	 lkp_PP_END_YEAR as lkp_PP_END_YEAR
From dsJoiner_exp_Counters0Out as dsJoiner_exp_Counters0Out),

DSR_exp_FinalOut as (
	
Select 
	exp_CountersOut.o_COUNTER_DESCRIPTION_1 as COUNTER_DESCRIPTION,
	 exp_CountersOut.DETAIL_RECORD_COUNT as COUNTER_VALUE
From exp_CountersOut as exp_CountersOut),

exp_FinalOut as (
	
Select 
	CURRENT_TIMESTAMP ( ) as o_RUN_DATE,
	 '{{ var("PMMappingName") }}' as o_PROCESS_NAME,
	 COUNTER_DESCRIPTION as COUNTER_DESCRIPTION,
	 COUNTER_VALUE as COUNTER_VALUE
From DSR_exp_FinalOut as DSR_exp_FinalOut),

DSR_COUNTER_TBLOut as (
	
Select 
	exp_FinalOut.o_RUN_DATE as RUN_DATE,
	 exp_FinalOut.o_PROCESS_NAME as PROCESS_NAME,
	 exp_FinalOut.COUNTER_DESCRIPTION as COUNTER_DESCRIPTION,
	 exp_FinalOut.COUNTER_VALUE as COUNTER_VALUE
From exp_FinalOut as exp_FinalOut)


Select 
	RUN_DATE as RUN_DATE,
	PROCESS_NAME as PROCESS_NAME,
	COUNTER_DESCRIPTION as COUNTER_DESCRIPTION,
	COUNTER_VALUE as COUNTER_VALUE 
From DSR_COUNTER_TBLOut as DSR_COUNTER_TBLOut