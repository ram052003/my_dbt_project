/* Transformation Name ==>COMPTIME_MESSAGE_FILE ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='comptime_message_file',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='overwrite')
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

DSR_exp_Build_MessageOut as (
	
Select 
	exp_CountersOut.o_COUNTER_DESCRIPTION_1 as COUNTER_DESCRIPTION_1,
	 exp_CountersOut.DETAIL_RECORD_COUNT as COUNTER_1,
	 exp_CountersOut.lkp_PP_NUM as PP_NUM,
	 exp_CountersOut.lkp_PP_END_YEAR as PP_END_YEAR
From exp_CountersOut as exp_CountersOut),

exp_Build_MessageOut as (
	
Select 
	( CASE WHEN PP_NUM < 10 THEN LPAD ( TO_VARCHAR ( PP_NUM ),
	 2,
	 '0' ) ELSE TO_VARCHAR ( PP_NUM ) END ) as v_PP_NUM,
	 DECODE ( SUBSTRING ( '{{ var("PMRepositoryServiceName") }}',
	 1,
	 4 ),
	 'Dev_',
	 'Dev:',
	 'Test',
	 'Test:',
	 'Prod',
	 'Prod:' ) as v_ENVIRONMENT,
	 v_ENVIRONMENT || 'Comp Time File loaded successfully for Pay Period:' || TO_VARCHAR ( PP_END_YEAR ) || '-' || v_PP_NUM as v_SUBJECT,
	 '{{ var("MAP_SUBJECT") }}' as o_SUBJECT,
	 'Number of Detail Records from Comp Time file = ' || TO_VARCHAR ( COUNTER_1 ) as v_MESSAGE,
	 '{{ var("MAP_MESSAGE") }}' as o_MESSAGE
From DSR_exp_Build_MessageOut as DSR_exp_Build_MessageOut),

DSR_exp_Final_MessageOut as (
	
Select 
	exp_Build_MessageOut.o_SUBJECT as SUBJECT,
	 exp_Build_MessageOut.o_MESSAGE as MESSAGE
From exp_Build_MessageOut as exp_Build_MessageOut),

exp_Final_MessageOut as (
	
Select 
	SUBJECT as SUBJECT,
	 MESSAGE as MESSAGE
From DSR_exp_Final_MessageOut as DSR_exp_Final_MessageOut)


Select 
	SUBJECT as SUBJECT,
	MESSAGE as MESSAGE 
From exp_Final_MessageOut as exp_Final_MessageOut