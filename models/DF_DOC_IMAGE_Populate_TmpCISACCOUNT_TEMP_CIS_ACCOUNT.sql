/* Transformation Name ==>TEMP_CIS_ACCOUNT ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='TEMP_CIS_ACCOUNT',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append')
}}

With TREASURY_CREDITOut as (
	/* transType: "Source" */
Select ACCOUNT_NUMBER, 
		NOTE_NUMBER, 
		COMMITMENT_NUMBER, 
		CUSTOMER_NAME, 
		CIS_NUMBER, 
		TAXID_NUMBER, 
		RELATED_NAME, 
		RELATED_ACCOUNT, 
		STATUS, 
		EMPLOYEE_FLAG, 
		FT_BARCODE, 
		ACCOUNT_BRANCH_CBC_NUMBER
 from 
{{ source ('DS_ACCT_CLIENT','TREASURY_CREDIT') }}
 ),

QueryOut as (
	/* transType: "Expression" */
Select 
	CIS_NUMBER as CIS_NUMBER,
	 ACCOUNT_NUMBER as ACCOUNT_NUMBER
From TREASURY_CREDITOut as TREASURY_CREDITOut)

Select 
	CIS_NUMBER as CIS_NUMBER,
	ACCOUNT_NUMBER as ACCOUNT_NUMBER 
From QueryOut as QueryOut