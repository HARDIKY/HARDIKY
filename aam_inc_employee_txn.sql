create or replace view IIFLW_INT_DB.BI_REPORTING.AAM_INC_EMPLOYEE_TXN(
    UNIQUE_TXN_NUMBER,
    CRN,
    SCRIP_NAME,
    ASSETCLASS,
    ASSETSUBCLASS,
    QUANTITY,
    GROSS_PRICE,
    ISIN,
    PAN_NO,
    TRANSACTIONDATE,
    TRANSACTIONTYPE,
    TRADE_FOR,
    NAME_OF_MINOR,
    GUARDIANRELATION,
    CREATEAT
) as

WITH MatchingPairs AS (
    SELECT
        T1.ID_CLIENT,
        T1.ID_ASSETCLASS,
        T1.transactiondate,
        T1.QUANTITY AS QuantityIn,
        T2.QUANTITY AS QuantityOut,
        T1.TXNID AS TXNID_IN,
        T2.TXNID AS TXNID_OUT
    FROM
        IIFLW_INT_DB.PUBLISH.FACT_TRANSACTION T1
        INNER JOIN
        IIFLW_INT_DB.PUBLISH.FACT_TRANSACTION T2
        ON
        T1.ID_CLIENT = T2.ID_CLIENT
        AND T1.ID_ASSETCLASS = T2.ID_ASSETCLASS
        AND T1.transactiondate = T2.transactiondate
        AND T1.transactiontype = 'Stock In'
        AND T2.transactiontype = 'Stock Out'
    GROUP BY
        T1.ID_CLIENT,
        T1.ID_ASSETCLASS,
        T1.transactiondate,
        T1.QUANTITY,
        T2.QUANTITY,
        T1.TXNID,
        T2.TXNID
),

TransactionsToExclude AS (
    SELECT
        TXNID_IN AS TXNID
    FROM
        MatchingPairs
    UNION
    SELECT
        TXNID_OUT AS TXNID
    FROM
        MatchingPairs
),

NetQuantity AS (
    SELECT
        ID_CLIENT,
        ID_ASSETCLASS,
        transactiondate,
        SUM(QUANTITY) AS NetQuantity
    FROM
        IIFLW_INT_DB.PUBLISH.FACT_TRANSACTION
    GROUP BY
        ID_CLIENT,
        ID_ASSETCLASS,
        transactiondate
),

ClientAndDependents AS (
    SELECT
        C.CRN,
        P.PAN_NO AS CLIENT_PAN,
        FIR.PAN_OF_RELATIVE AS DEPENDENT_PAN,
        FIR.type_of_relationship
    FROM
        IIFLW_INT_DB.MIRROR_MDM_SCHEMA.EBX_CLIENT C
        LEFT JOIN IIFLW_INT_DB.MIRROR_MDM_SCHEMA.EBX_CLIENT_PERSON_ROLE CPR ON C.MDM_CLIENT_ID = CPR.MDM_CLIENT_ID_
        LEFT JOIN IIFLW_INT_DB.MIRROR_MDM_SCHEMA.EBX_PERSON P ON P.MDM_PERSON_ID = CPR.MDM_PERSON_ID_
        LEFT JOIN IIFLW_INT_DB.MIRROR_MDM_SCHEMA.EBX_EMPLOYEE E ON P.PAN_NO = E.PAN_NO
        LEFT JOIN IIFLW_INT_DB.MIRROR_MDM_SCHEMA.EBX_FINANCIAL_AND_IMMEDIATE_RELATIVES FIR ON FIR.mdm_employee_id_ = E.MDM_EMP_ID
    WHERE
        CPR.PERSON_ROLE = 'PH'
        AND E.ACTUAL_RELIEVING_DATE IS NULL
        AND (UPPER(E.STATUS) = 'ACTIVE' OR UPPER(E.STATUS) = 'A')
        AND UPPER(FIR.RECORD_TYPE) = 'ADDITION'
        AND E.SUBTYPE = '360 ONE Alternate Asset Management Limited'
)

SELECT DISTINCT
    T.TXNID AS UNIQUE_TXN_NUMBER,
    C.SRC_CLIENT_CODE AS CRN,
    AC.ASSETNAME AS SCRIP_NAME,
    AC.ASSETCLASS AS ASSETCLASS,
    AC.ASSETSUBCLASS AS ASSETSUBCLASS,
    T.QUANTITY,
    T.BUYSELLPRICE AS GROSS_PRICE,
    AC.ISIN AS ISIN,
    P.PAN_NO AS PAN_NO,
    T.transactiondate,
    T.transactiontype AS TRANSACTIONTYPE,
    CASE
        WHEN P.IS_MINOR = 'Yes' THEN 'MINOR'
        WHEN P.IS_MINOR = 'No' THEN 'SELF'
    END::varchar(100) AS TRADE_FOR,
    CASE
        WHEN P.IS_MINOR = 'YES' THEN P.FIRST_NAME || ' ' || COALESCE(P.MIDDLE_NAME, '') || ' ' || P.LAST_NAME
        ELSE NULL
    END::varchar(100) AS NAME_OF_MINOR,
    CASE
        WHEN BC.ISMINOR = 'Y' THEN BC.GUARDIANRELATION
        ELSE ''
    END::varchar(100) GUARDIANRELATION,
    CT.CREATEAT
FROM
    IIFLW_INT_DB.PUBLISH.DIM_PERSON P
    LEFT JOIN IIFLW_INT_DB.PUBLISH.DIM_CLIENT C ON C.PRIMARY_ID_PERSON = P.ID AND C.ACTIVE_FLAG = 'Y'
    INNER JOIN IIFLW_INT_DB.PUBLISH.FACT_TRANSACTION T ON C.ID = T.ID_CLIENT
    LEFT JOIN IIFLW_INT_DB.MIRROR_CAL_SCHEMA.TXN CT ON T.TXNID = CT.ID
    INNER JOIN IIFLW_INT_DB.PUBLISH.DIM_ASSETCLASS AC ON AC.ID = T.ID_ASSETCLASS AND AC.ACTIVE_FLAG = 'Y' AND AC.ASSETCLASS <> 'Mutual Fund'
    LEFT JOIN IIFLW_INT_DB.MIRROR_CAL_SCHEMA.BHTB_CLIENT BC ON BC.CL_CODE = C.SRC_CLIENT_CODE AND BC.OP = 'I'
    LEFT JOIN TransactionsToExclude TE ON T.TXNID = TE.TXNID
    LEFT JOIN NetQuantity NQ ON T.ID_CLIENT = NQ.ID_CLIENT
        AND T.ID_ASSETCLASS = NQ.ID_ASSETCLASS AND T.transactiondate = NQ.transactiondate
    LEFT JOIN ClientAndDependents CAD ON P.PAN_NO = CAD.CLIENT_PAN OR P.PAN_NO = CAD.DEPENDENT_PAN
WHERE
    TE.TXNID IS NULL
    AND CAD.CRN IS NOT NULL
    AND NQ.NetQuantity <> 0
    AND T.transactiondate BETWEEN DATEADD(DAY, -10, CURRENT_DATE()) AND CURRENT_DATE()
    AND AC.ASSETCLASS <> 'Mutual Fund'
    AND AC.ASSETCLASS NOT IN (
        'Mutual Fund',
        'OPTIONS',
        'FUTURES',
        'Cash & Cash Equivalants',
        'Managed Account',
        'Commodity Options'
    )
    AND AC.assetsubclass NOT IN (
        'Equity Options',
        'Equity Futures',
        'Unlisted Equity',
        'Commodity Options',
        'Unlisted Equity - Real Estate',
        'Venture Capital Fund-Others'
    )
ORDER BY SCRIP_NAME, transactiondate;
