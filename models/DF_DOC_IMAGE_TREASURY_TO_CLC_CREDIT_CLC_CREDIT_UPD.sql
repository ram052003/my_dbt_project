{{
  config(
    materialized='incremental',
    alias='CLC_CREDIT',
    schema='',
    pre_hook="",
    post_hook="",
    incremental_strategy='merge',
    unique_key=['CIS_NUMBER', 'ACCOUNT_BRANCH_CBC_NUMBER'],
    merge_update_columns=['CIS_NUMBER','ACCOUNT_BRANCH_CBC_NUMBER']
  )
}}

WITH TREASURY_CREDITOut AS (
    /* transType: "Source" */
    SELECT
        ACCOUNT_NUMBER,
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
    FROM {{ source('DS_ACCT_CLIENT', 'TREASURY_CREDIT') }}
),

DSR_QueryOut AS (
    /* transType: "Expression" */
    SELECT
        TREASURY_CREDITOut.RELATED_ACCOUNT          AS ACCOUNT_NUMBER,
        TREASURY_CREDITOut.CUSTOMER_NAME           AS CUSTOMER_NAME,
        TREASURY_CREDITOut.CIS_NUMBER              AS CIS_NUMBER,
        TREASURY_CREDITOut.TAXID_NUMBER            AS TAXID_NUMBER,
        TREASURY_CREDITOut.RELATED_NAME            AS RELATED_NAME,
        TREASURY_CREDITOut.EMPLOYEE_FLAG           AS EMPLOYEE_FLAG,
        TREASURY_CREDITOut.FT_BARCODE              AS FT_BARCODE,
        TREASURY_CREDITOut.ACCOUNT_BRANCH_CBC_NUMBER AS ACCOUNT_BRANCH_CBC_NUMBER
    FROM TREASURY_CREDITOut AS TREASURY_CREDITOut
),

QueryOut AS (
    /* transType: "Expression" */
    SELECT
        NVL(RELATED_ACCOUNT, ' ')                AS ACCOUNT_NUMBER,
        NVL(CUSTOMER_NAME, ' ')                  AS CUSTOMER_NAME,
        CIS_NUMBER,
        NVL(TAXID_NUMBER, 0)                     AS TAXID_NUMBER,
        NVL(RELATED_NAME, ' ')                   AS RELATED_NAME,
        RTRIM(EMPLOYEE_FLAG, ' ')                AS EMPLOYEE_FLAG,
        NVL(FT_BARCODE, ' ')                     AS FT_BARCODE,
        ACCOUNT_BRANCH_CBC_NUMBER
    FROM DSR_QueryOut AS DSR_QueryOut
),

Table_Comparison_LKPOut AS (
    /* transType: "Lookup Procedure" */
    SELECT
        QueryOut.ACCOUNT_NUMBER                     AS ACCOUNT_NUMBER_1,
        QueryOut.CUSTOMER_NAME                     AS CUSTOMER_NAME_1,
        QueryOut.CIS_NUMBER                        AS CIS_NUMBER_1,
        QueryOut.TAXID_NUMBER                      AS TAXID_NUMBER_1,
        QueryOut.RELATED_NAME                      AS RELATED_NAME_1,
        QueryOut.EMPLOYEE_FLAG                     AS EMPLOYEE_FLAG_1,
        QueryOut.FT_BARCODE                        AS FT_BARCODE_1,
        QueryOut.ACCOUNT_BRANCH_CBC_NUMBER          AS ACCOUNT_BRANCH_CBC_NUMBER_1,
        Query.ACCOUNT_NUMBER                        AS ACCOUNT_NUMBER_2,
        Query.CUSTOMER_NAME                        AS CUSTOMER_NAME_2,
        Query.CIS_NUMBER                           AS CIS_NUMBER_2,
        Query.TAXID_NUMBER                         AS TAXID_NUMBER_2,
        Query.RELATED_NAME                         AS RELATED_NAME_2,
        Query.EMPLOYEE_FLAG                        AS EMPLOYEE_FLAG_2,
        Query.FT_BARCODE                           AS FT_BARCODE_2,
        Query.ACCOUNT_BRANCH_CBC_NUMBER             AS ACCOUNT_BRANCH_CBC_NUMBER_2
    FROM QueryOut AS QueryOut
    LEFT JOIN DBO.CLC_CREDITOut
        ON QueryOut.CIS_NUMBER = DBO.CLC_CREDITOut.CIS_NUMBER
       AND QueryOut.ACCOUNT_BRANCH_CBC_NUMBER = DBO.CLC_CREDITOut.ACCOUNT_BRANCH_CBC_NUMBER
),

Table_Comparison_EXPOut AS (
    /* transType: "Expression" */
    SELECT
        ACCOUNT_NUMBER,
        CUSTOMER_NAME,
        CIS_NUMBER,
        TAXID_NUMBER,
        RELATED_NAME,
        EMPLOYEE_FLAG,
        FT_BARCODE,
        ACCOUNT_BRANCH_CBC_NUMBER,
        MD5(CONCAT(
            CIS_NUMBER,
            ACCOUNT_BRANCH_CBC_NUMBER,
            ACCOUNT_NUMBER,
            CUSTOMER_NAME,
            TAXID_NUMBER,
            RELATED_NAME,
            EMPLOYEE_FLAG
        )) AS src_hashcode,
        MD5(CONCAT(
            CIS_NUMBER,
            ACCOUNT_BRANCH_CBC_NUMBER,
            ACCOUNT_NUMBER,
            CUSTOMER_NAME,
            TAXID_NUMBER,
            RELATED_NAME,
            EMPLOYEE_FLAG
        )) AS tgt_hashcode,
        CASE
            WHEN CIS_NUMBER IS NULL AND ACCOUNT_BRANCH_CBC_NUMBER IS NULL THEN 1
            WHEN MD5(CONCAT(
                    CIS_NUMBER,
                    ACCOUNT_BRANCH_CBC_NUMBER,
                    ACCOUNT_NUMBER,
                    CUSTOMER_NAME,
                    TAXID_NUMBER,
                    RELATED_NAME,
                    EMPLOYEE_FLAG
                )) != MD5(CONCAT(
                    CIS_NUMBER,
                    ACCOUNT_BRANCH_CBC_NUMBER,
                    ACCOUNT_NUMBER,
                    CUSTOMER_NAME,
                    TAXID_NUMBER,
                    RELATED_NAME,
                    EMPLOYEE_FLAG
                )) THEN 3
            ELSE 0
        END AS change_code
    FROM Table_Comparison_LKPOut AS Table_Comparison_LKPOut
),

Map_OperationOut AS (
    /* transType: "Router" */
    SELECT
        Table_Comparison_EXPOut.ACCOUNT_NUMBER,
        Table_Comparison_EXPOut.CUSTOMER_NAME,
        Table_Comparison_EXPOut.CIS_NUMBER,
        Table_Comparison_EXPOut.TAXID_NUMBER,
        Table_Comparison_EXPOut.RELATED_NAME,
        Table_Comparison_EXPOut.EMPLOYEE_FLAG,
        Table_Comparison_EXPOut.FT_BARCODE,
        Table_Comparison_EXPOut.ACCOUNT_BRANCH_CBC_NUMBER,
        Table_Comparison_EXPOut.src_hashcode,
        Table_Comparison_EXPOut.tgt_hashcode,
        Table_Comparison_EXPOut.change_code
    FROM Table_Comparison_EXPOut AS Table_Comparison_EXPOut
    WHERE change_code = 3
)

SELECT
    ACCOUNT_NUMBER,
    CUSTOMER_NAME,
    CIS_NUMBER,
    TAXID_NUMBER,
    RELATED_NAME,
    EMPLOYEE_FLAG,
    FT_BARCODE,
    ACCOUNT_BRANCH_CBC_NUMBER
FROM Map_OperationOut AS Map_OperationOut