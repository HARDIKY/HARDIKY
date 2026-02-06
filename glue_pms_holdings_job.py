print("-"*20, "JOB STARTED", "-"*20)
print("importing libraries..")
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

import boto3
from datetime import datetime, timedelta
import dateutil.tz
import os
import json
print("libraries imported...!")

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bkt_name",
        "appflow_bckt",
        "folder_path",
        "mf_secret",
        "aif_appflow_bkt",
        "aif_bkt_name",
        "broker_mapping",
        "prd_cp_code_mapping",
        "pms_secret",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

mf_secret = args["mf_secret"]
pms_secret = args["pms_secret"]
bkt_name = args["bkt_name"]
appflow_bckt = args["appflow_bckt"]
folder_path = args["folder_path"]
aif_appflow_bkt = args["aif_appflow_bkt"]
aif_bkt_name = args["aif_bkt_name"]
broker_mapping = args["broker_mapping"]
prd_cp_code_mapping = args["prd_cp_code_mapping"]

IST = dateutil.tz.gettz("Asia/Kolkata")
currdt = (datetime.now(tz=IST) - timedelta(days=1)).strftime("%Y-%m-%d")
s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

print("Retrieving Connection Details...")

secret_response = secrets_client.get_secret_value(SecretId=mf_secret)
pg = json.loads(secret_response["SecretString"])
pg_url = pg["pg_url"]
pg_user = pg["pg_user"]
pg_password = pg["pg_password"]
print("Connection details from Secrets Manager retrieved for mf_secret...")

secret_response = secrets_client.get_secret_value(SecretId=pms_secret)
pms = json.loads(secret_response["SecretString"])
or_url = pms["or_url"]
or_user = pms["or_user"]
or_password = pms["or_password"]
or_url_pmsci = pms["or_url_pmsci"]
or_user_pmsci = pms["or_user_pmsci"]
or_password_pmsci = pms["or_password_pmsci"]
print("Connection details from Secrets Manager retrieved for pms_secret...")


def read_csv(path, bckt):
    s3_path = "s3://" + bckt + "/" + path
    df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("delimiter", "|")
        .csv(s3_path)
    )
    return df


def create_df(tablename, business):
    print("Connecting to ", business, " for table ", tablename)
    try:
        if business == "MF":
            df = (
                spark.read.format("jdbc")
                .option("url", pg_url)
                .option("user", pg_user)
                .option("password", pg_password)
                .option("dbtable", tablename)
                .option("driver", "org.postgresql.Driver")
                .load()
            )
            return df
        elif business == "AIF":
            df = (
                spark.read.format("jdbc")
                .option("url", pg_url)
                .option("user", pg_user)
                .option("password", pg_password)
                .option("dbtable", tablename)
                .option("driver", "org.postgresql.Driver")
                .load()
            )
            return df
        elif business == "PMS":
            if tablename.upper() == "WSAMCPRD.PORTFOLIO_PERF_DAILY":
                query = f"""(SELECT * FROM {tablename} WHERE TRUNC(FROMDATE) = (
                    SELECT MAX(TRUNC(FROMDATE)) FROM {tablename} WHERE TRUNC(FROMDATE) <= TRUNC(SYSDATE-1)
                )) filtered_table"""
                print("query: ", query)
                df = (
                    spark.read.format("jdbc")
                    .option("url", or_url)
                    .option("user", or_user)
                    .option("password", or_password)
                    .option("dbtable", query)
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .load()
                )
                print("WSAMCPRD.PORTFOLIO_PERF_DAILY created..!")
            else:
                df = (
                    spark.read.format("jdbc")
                    .option("url", or_url)
                    .option("user", or_user)
                    .option("password", or_password)
                    .option("dbtable", tablename)
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .load()
                )
            return df
        elif business == "PMSCI":
            if tablename.upper() == "WSMAAPRD.PORTFOLIO_PERF_DAILY":
                query = f"""(SELECT * FROM {tablename} WHERE TRUNC(FROMDATE) = (
                    SELECT MAX(TRUNC(FROMDATE)) FROM {tablename} WHERE TRUNC(FROMDATE) <= TRUNC(SYSDATE-1)
                )) filtered_table"""
                print("query: ", query)
                df = (
                    spark.read.format("jdbc")
                    .option("url", or_url)
                    .option("user", or_user)
                    .option("password", or_password)
                    .option("dbtable", query)
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .load()
                )
                print("WSMAAPRD.PORTFOLIO_PERF_DAILY created..!")
            else:
                df = (
                    spark.read.format("jdbc")
                    .option("url", or_url)
                    .option("user", or_user)
                    .option("password", or_password)
                    .option("dbtable", tablename)
                    .option("driver", "oracle.jdbc.driver.OracleDriver")
                    .load()
                )
            return df
        else:
            print("Not intended business line for processing.")
    except Exception as e:
        print("Error : JDBC Connection issue, unable to read table\n", e)
        return e


def write_csv(df, bckt_name, path):
    path = "s3://" + bckt_name + "/" + path
    (
        df.coalesce(1)
        .distinct()
        .write.mode("overwrite")
        .format("csv")
        .option("header", "True")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("delimiter", "|")
        .save(path)
    )
    print("File saved successfully at ", path)


def write_appflow_file(df, bckt_name, path):
    path = "s3://" + bckt_name + "/" + path
    (
        df.coalesce(1)
        .distinct()
        .write.mode("overwrite")
        .format("csv")
        .option("header", "True")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("delimiter", ",")
        .save(path)
    )
    print("File saved successfully at ", path)


def rename_s3_file(bckt_name, Prefix, new_s3_filename):
    response = s3.list_objects_v2(Bucket=bckt_name, Prefix=Prefix)
    s3_filename = "None"
    for obj in response.get("Contents", []):
        objectkey = obj["Key"]
        filename = os.path.basename(objectkey)
        if "part" in filename:
            s3_filename = filename
            print("Old Filename accessed successfully")
            break

    old_file_path = f"{Prefix}{s3_filename}"
    new_file_path = f"{Prefix}{new_s3_filename}.csv"
    print("Old File Path : ", old_file_path)
    print("New File Path : ", new_file_path)
    s3.copy_object(
        Bucket=bckt_name,
        CopySource={"Bucket": bckt_name, "Key": old_file_path},
        Key=new_file_path,
    )
    s3.delete_object(Bucket=bckt_name, Key=old_file_path)
    print("file Renamed Successfully")
    return "file Renamed Successfully"


def get_delta(prevdf, currdf, pkey):
    prevdf = prevdf.na.fill("").withColumn(
        "hashvalue", sha2(concat_ws("||", *prevdf.columns), 256)
    )
    currdf = currdf.na.fill("").withColumn(
        "hashvalue", sha2(concat_ws("||", *currdf.columns), 256)
    )
    updatedDf = (
        currdf.alias("curr")
        .join(prevdf.alias("prev"), pkey, "inner")
        .filter(col("prev.hashvalue") != col("curr.hashvalue"))
        .withColumn("op", lit("U"))
        .select("op", "curr.*")
    )
    insertDf = (
        currdf.alias("curr")
        .join(prevdf.alias("prev"), pkey, "left_anti")
        .withColumn("op", lit("I"))
        .select("op", "curr.*")
    )
    deleteDf = (
        prevdf.alias("prev")
        .join(currdf.alias("curr"), pkey, "left_anti")
        .withColumn("op", lit("D"))
        .select("op", "prev.*")
    )

    deleteDf = deleteDf.withColumn("CURRENT_MARKET_VALUE", lit("0"))

    updatedDf = updatedDf.drop("hashvalue")
    insertDf = insertDf.drop("hashvalue")
    deleteDf = deleteDf.drop("hashvalue")
    currdf = currdf.drop("hashvalue")

    print("going for union of all the dataframe..!")
    final_df = (
        updatedDf.unionByName(insertDf, allowMissingColumns=True)
        .unionByName(deleteDf, allowMissingColumns=True)
        .select("op", *currdf.columns)
    )
    print("column list of final_df: ", final_df.columns)
    final_df.printSchema()
    return final_df


def getPrevFile(entity_name, bckt):
    dates = []
    fullload_path = folder_path + "/fl/" + entity_name + "/"
    date_contents = boto3.client("s3").list_objects_v2(
        Bucket=bckt, Prefix=fullload_path
    )
    if "Contents" in date_contents:
        for value in date_contents["Contents"]:
            if ".csv" in value["Key"]:
                a = value["Key"].split("/")[-2]
                dates.append(a)
    else:
        print("Folder for the entity {} doesnot exist.".format(entity_name))
        return 0
    dates.sort(reverse=True)
    if len(dates) > 0 and dates[0] != "rundate=" + currdt:
        prev_path = folder_path + "/fl/" + entity_name + "/" + dates[0] + "/"
        print("Previous file path ", prev_path)
        return prev_path
    elif len(dates) > 1 and dates[0] == "rundate=" + currdt:
        prev_path = folder_path + "/fl/" + entity_name + "/" + dates[1] + "/"
        print("Previous file path ", prev_path)
        return prev_path
    else:
        return 0


def compute_incrementals(entity_name, pkeys, prev_filepath, curr_filepath, bckt, app_bkt):
    print("Into computing Incre")
    print("prev filepath: ", prev_filepath)
    print("current file :", curr_filepath)
    curr_df = read_csv(curr_filepath, bckt)
    prev_df = curr_df.limit(0)
    if prev_filepath != 0:
        prev_df = read_csv(prev_filepath, bckt)
    print("Computing delta...")
    inc_df = get_delta(prev_df, curr_df, pkeys)
    filepath = folder_path + "/inc/" + entity_name + "/rundate=" + currdt + "/"
    write_csv(inc_df, bckt, filepath)
    print("writing into appflow bucket..")
    write_appflow_file(inc_df, app_bkt, folder_path + "/" + entity_name + "/")
    if inc_df.count() > 0:
        print("Incrementals written")
    else:
        print("No incrementals to write")


# ---------------------ARTIFACTS TABLES FROM MAPPING FILES SHARED BY SALEFORCE -------------------------
BROKER_MAPPING = (
    spark.read.option("header", "true")
    .option("multiline", "true")
    .option("escape", '"')
    .option("quote", '"')
    .option("delimeter", ",")
    .csv(broker_mapping)
)
BROKER_MAPPING.createOrReplaceTempView("BROKER_MAPPING_V")

prd_cp_code_mapping_df = (
    spark.read.option("header", "true")
    .option("multiline", "true")
    .option("escape", '"')
    .option("quote", '"')
    .option("delimeter", ",")
    .csv(prd_cp_code_mapping)
)
prd_cp_code_mapping_df.createOrReplaceTempView("PRD_CP_CODE_MAPPING_V")

# ------------------------------------------------------ CAMS MF table    -----------------------------------------------------------------
mf_nav_df = create_df('"STIIFL"."NAV"', "MF")
mf_nav_df.createOrReplaceTempView("MF_NAV")

mf_sm_df = create_df('"STIIFL"."SCHEME_SETUP"', "MF")
mf_sm_df.createOrReplaceTempView("MF_SCHEME_SETUP")

mf_cs_df = create_df('"STIIFL"."CUSTOMER_SCHEMES"', "MF")
mf_cs_df.createOrReplaceTempView("MF_CUSTOMER_SCHEMES")

print("MF STIIFL Views created.!")
# ------------------------------------------------------ CAMS AIF table    -----------------------------------------------------------------

aif_nav_df = create_df('"STIFA"."NAV"', "AIF")
aif_nav_df.createOrReplaceTempView("AIF_NAV")

aif_sm_df = create_df('"STIFA"."SCHEME_SETUP"', "AIF")
aif_sm_df.createOrReplaceTempView("AIF_SCHEME_SETUP")

aif_cs_df = create_df('"STIFA"."CUSTOMER_SCHEMES"', "AIF")
aif_cs_df.createOrReplaceTempView("AIF_CUSTOMER_SCHEMES")

print("AIF STIFA Views created.!")

# ---------------------WSP WSAMCPRD PMS  TABLES-------------------------

pms_portfolio_perf_daily_df = create_df("WSAMCPRD.PORTFOLIO_PERF_DAILY", "PMS")
pms_portfolio_perf_daily_df.createOrReplaceTempView("PMS_PORTFOLIO_PERF_DAILY")

pms_client_m_df = create_df("WSAMCPRD.CLIENT_M", "PMS")
pms_client_m_df.createOrReplaceTempView("PMS_CLIENT_M")

pms_scheme_m_df = create_df("WSAMCPRD.scheme_m", "PMS")
pms_scheme_m_df.createOrReplaceTempView("PMS_SCHEME_M")

pms_intermediary_df = create_df("WSAMCPRD.intermediary_m", "PMS")
pms_intermediary_df.createOrReplaceTempView("PMS_INTERMEDIARY_M")

print("WSP WSAMCPRD PMS Views created.!")

# ---------------------WSP WSMAAPRD PMS-Co-inv  TABLES-------------------------
# WSMAAPRD.PORTFOLIO_PERF_DAILY
# WSMAAPRD.CLIENT_M
# WSMAAPRD.SCHEME_M
# WSMAAPRD.INTERMEDIARY_M

pms_co_portfolio_perf_daily_df = create_df("WSMAAPRD.PORTFOLIO_PERF_DAILY", "PMSCI")
pms_co_portfolio_perf_daily_df.createOrReplaceTempView("PMS_CO_PORTFOLIO_PERF_DAILY")

pms_co_client_m_df = create_df("WSMAAPRD.CLIENT_M", "PMSCI")
pms_co_client_m_df.createOrReplaceTempView("PMS_CO_CLIENT_M")

pms_co_scheme_m_df = create_df("WSMAAPRD.SCHEME_M", "PMSCI")
pms_co_scheme_m_df.createOrReplaceTempView("PMS_CO_SCHEME_M")

pms_co_intermediary_df = create_df("WSMAAPRD.INTERMEDIARY_M", "PMSCI")
pms_co_intermediary_df.createOrReplaceTempView("PMS_CO_INTERMEDIARY_M")

print("WSP WSMAAPRD PMS-Co-inv Views created.!")


# ------------------------MF---STIIFL---QUERY-----------------------------------------------

amc_mf_nav_df = spark.sql(
    """
SELECT * FROM (
SELECT SCH_CODE, NAV, NAV_DATE,NAV_SERIAL,
    ROW_NUMBER() OVER (PARTITION BY SCH_CODE ORDER BY NAV_DATE DESC) AS Rank
FROM MF_NAV N WHERE NAV_SERIAL = 1
) MF_NAV_LATEST WHERE Rank = 1 ORDER BY NAV_DATE DESC
"""
)

# ------------------------MF---STIIFL---QUERY-----------------------------------------------
amc_aif_nav_df = spark.sql(
    """
SELECT * FROM (
SELECT
    SCH_CODE, NAV, NAV_DATE,NAV_SERIAL,
    ROW_NUMBER() OVER (PARTITION BY SCH_CODE ORDER BY NAV_DATE DESC) AS Rank
FROM AIF_NAV  WHERE NAV_SERIAL = 1
) AIF_NAV_LATEST WHERE Rank = 1 ORDER BY NAV_DATE DESC
"""
)


# --------------------------------AMC HOLDING MF -- STIIFL---------------------

amc_mf_holding_df = spark.sql(
    """
with balqtybase as
(
select FOLIO_NO, CONCAT(FOLIO_NO,'-MF') AS FOLIO_ACCOUNT,
        CASE WHEN upper(trim(cs.BROKER_CODE)) = upper(trim(brm.BROKER_CODE))
            THEN COALESCE(REPLACE(trim(brm.AMFI_REGN_NO), ' ', ''), '')
            ELSE COALESCE(REPLACE(trim(cs.BROKER_CODE), ' ', ''), '')
        END AS CHANNEL_PARTNER_PARENT,
        SCH_CODE
        ,SUM(COALESCE(TOTAL_UNITS,0)) as BAL_QTY
from MF_CUSTOMER_SCHEMES cs
left join BROKER_MAPPING_V brm on upper(trim(cs.BROKER_CODE)) = upper(trim(brm.BROKER_CODE))
GROUP BY 1,2,3,4),

navbase as (
    select SCH_CODE,NAV_DATE,NAV_SERIAL,NAV
    from (
        select n.*
        ,row_number () over (partition by SCH_CODE order by SCH_CODE,NAV_DATE desc) as rn
        from MF_NAV n WHERE NAV_SERIAL = 1
        ) latest_nav where rn = 1 order by NAV_DATE desc
        )
select CONCAT(balq.FOLIO_NO,'-',balq.CHANNEL_PARTNER_PARENT,'-',balq.SCH_CODE,'-MF') AS SOURCE_SYSTEM_ID, balq.*,
CONCAT(balq.CHANNEL_PARTNER_PARENT,'-MF') AS CHANNEL_PARTNER,
SCH.SCHEME_TYPE, SPLIT_PART(SUBSTR(SCH.SCHNAME,1,79), ' (Formerly', 1) AS FOLIO_HOLDING_NAME, nav.NAV, nav.NAV_SERIAL, nav.NAV_DATE, round(balq.BAL_QTY*nav.NAV,3) as CURRENT_MARKET_VALUE
from balqtybase balq
left join navbase nav on balq.SCH_CODE = nav.SCH_CODE
left join MF_SCHEME_SETUP as SCH ON balq.SCH_CODE = SCH.SCHCODE
WHERE balq.CHANNEL_PARTNER_PARENT like 'ARN-%' OR balq.CHANNEL_PARTNER_PARENT like 'INA%' OR balq.CHANNEL_PARTNER_PARENT like 'INP%' OR balq.CHANNEL_PARTNER_PARENT like 'INZ%' OR balq.CHANNEL_PARTNER_PARENT like 'INB%' OR balq.CHANNEL_PARTNER_PARENT like 'CAT-1-EOP%' OR balq.CHANNEL_PARTNER_PARENT like 'DIRECT%'
"""
)


# --------------------------------AMC HOLDING AIF -- STIFA---------------------

amc_aif_holding_df = spark.sql(
    """
with balqtybase as
(
select  FOLIO_NO, CONCAT(FOLIO_NO,'-AIF') AS FOLIO_ACCOUNT,
        CASE WHEN upper(trim(cs.BROKER_CODE)) = upper(trim(brm.BROKER_CODE))
            THEN COALESCE(REPLACE(trim(brm.AMFI_REGN_NO), ' ', ''), '')
            ELSE COALESCE(REPLACE(trim(cs.BROKER_CODE), ' ', ''), '')
        END AS CHANNEL_PARTNER_PARENT,
        SCH_CODE
        ,SUM(COALESCE(TOTAL_UNITS,0)) as BAL_QTY
from AIF_CUSTOMER_SCHEMES cs left join BROKER_MAPPING_V brm on upper(trim(cs.BROKER_CODE)) = upper(trim(brm.BROKER_CODE))
GROUP BY 1,2,3,4) ,

navbase as (
    select SCH_CODE,NAV_DATE,NAV_SERIAL,NAV
    from (
        select n.*
        ,row_number () over (partition by SCH_CODE order by SCH_CODE,NAV_DATE desc) as rn
        from AIF_NAV n WHERE NAV_SERIAL = 1
        ) latest_nav where rn = 1 order by NAV_DATE desc
        )
select CONCAT(balq.FOLIO_NO,'-',balq.CHANNEL_PARTNER_PARENT,'-',balq.SCH_CODE,'-AIF') AS SOURCE_SYSTEM_ID, balq.*,
CASE WHEN upper(trim(balq.SCH_CODE)) = upper(trim(prd_cp_code.PRODUCT_CODE))
            THEN CONCAT(balq.CHANNEL_PARTNER_PARENT ,'-', prd_cp_code.PRODUCT_TYPE)
            ELSE ''
            END AS CHANNEL_PARTNER,
SCH.SCHEME_TYPE, SUBSTR(SCH.SCHNAME,1,79) AS FOLIO_HOLDING_NAME, nav.NAV, nav.NAV_SERIAL, nav.NAV_DATE, round(balq.BAL_QTY*nav.NAV,3) as CURRENT_MARKET_VALUE
from balqtybase balq
left join navbase nav on balq.SCH_CODE = nav.SCH_CODE
left join AIF_SCHEME_SETUP as SCH ON balq.SCH_CODE = SCH.SCHCODE
left join PRD_CP_CODE_MAPPING_V as prd_cp_code on balq.SCH_CODE = prd_cp_code.PRODUCT_CODE
WHERE balq.CHANNEL_PARTNER_PARENT like 'ARN-%' OR balq.CHANNEL_PARTNER_PARENT like 'INA%' OR balq.CHANNEL_PARTNER_PARENT like 'INP%' OR balq.CHANNEL_PARTNER_PARENT like 'INZ%' OR balq.CHANNEL_PARTNER_PARENT like 'INB%' OR balq.CHANNEL_PARTNER_PARENT like 'CAT-1-EOP%' OR balq.CHANNEL_PARTNER_PARENT like 'DIRECT%'
"""
)


# ------------------------------AMC HOLDING PMS BASE-------------------------------------
pms_holding_base_df = spark.sql(
    """
SELECT
  cm.CLIENTID,
  sm.SCHEMEID,
  sm.SCHEMENAME,
  ppd.FROMDATE AS AS_ON_DATE,
  SUM(ppd.ENDMKTVAL) AS CURRENT_MARKET_VALUE,
  im.AMFICODE,
  im.REFCODE4,
  prd_cp_code.PRODUCT_TYPE
FROM PMS_PORTFOLIO_PERF_DAILY ppd
INNER JOIN PMS_CLIENT_M cm ON cm.CLIENTID  = ppd.CLIENTID and cm.CLIENTFLAG in ('I','N','D','A')
INNER JOIN PMS_SCHEME_M sm  ON sm.SCHEMEID  = cm.SCHEMEID
INNER JOIN PMS_INTERMEDIARY_M im  ON im.INTERID  = cm.INTERID
LEFT JOIN PRD_CP_CODE_MAPPING_V as prd_cp_code on upper(trim(sm.SCHEMEID)) = upper(trim(prd_cp_code.PRODUCT_CODE))
GROUP BY cm.CLIENTID, sm.SCHEMEID, sm.SCHEMENAME, ppd.FROMDATE,
         im.AMFICODE, im.REFCODE4, prd_cp_code.PRODUCT_TYPE
"""
)
pms_holding_base_df.createOrReplaceTempView("PMS_HOLDING_BASE")

# ------------------------------AMC HOLDING PMS (NON-APRN)--------------------------------
amc_pms_holding_non_aprn_df = spark.sql(
    """
WITH keyed AS (
    SELECT
        *,
        REPLACE(UPPER(AMFICODE),' ','') AS AMFI_NORM,
        CASE
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'IN%' THEN AMFICODE
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'ARN%' AND REFCODE4 IS NULL THEN AMFICODE
            WHEN REFCODE4 IS NOT NULL AND REPLACE(UPPER(REFCODE4),' ','') LIKE 'APRN%' THEN REFCODE4
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'APRN%' THEN AMFICODE
            ELSE AMFICODE
        END AS KEY_VALUE,
        CASE
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'IN%' THEN 'AMFI_IN'
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'ARN%' AND REFCODE4 IS NULL THEN 'AMFI_ARN'
            WHEN REFCODE4 IS NOT NULL AND REPLACE(UPPER(REFCODE4),' ','') LIKE 'APRN%' THEN 'APRN'
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'APRN%' THEN 'APRN'
            ELSE 'AMFI_OTHER'
        END AS KEY_SOURCE
    FROM PMS_HOLDING_BASE
)
SELECT
 CONCAT(CLIENTID,'-', UPPER(REPLACE(IFNULL(KEY_VALUE,''), ' ', '')),'-',SCHEMEID,'-PMS') AS SOURCE_SYSTEM_ID,
 UPPER(REPLACE(IFNULL(KEY_VALUE,''), ' ', '')) as CHANNEL_PARTNER_PARENT,
 CASE WHEN PRODUCT_TYPE IS NOT NULL
      THEN IFNULL(CONCAT(upper(REPLACE(IFNULL(KEY_VALUE,''),' ','')) ,'-', PRODUCT_TYPE),'')
      ELSE ''
 END AS CHANNEL_PARTNER,
 CONCAT(CLIENTID,'-PMS') AS FOLIO_ACCOUNT,
 SUBSTR(SCHEMENAME,1,79) AS PRODUCT_NAME,
 SCHEMEID AS PRODUCT_CODE,
 AS_ON_DATE,
 round(CURRENT_MARKET_VALUE,3) AS CURRENT_MARKET_VALUE
FROM keyed
WHERE KEY_SOURCE <> 'APRN'
  AND (AMFI_NORM LIKE 'IN%' OR AMFI_NORM LIKE 'ARN%')
"""
)

print("amc_pms_holding_non_aprn_df: ")
amc_pms_holding_non_aprn_df.show(5)

# ------------------------------AMC HOLDING PMS (APRN ONLY)-------------------------------
amc_pms_holding_aprn_df = spark.sql(
    """
WITH keyed AS (
    SELECT
        *,
        REPLACE(UPPER(AMFICODE),' ','') AS AMFI_NORM,
        CASE
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'IN%' THEN AMFICODE
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'ARN%' AND REFCODE4 IS NULL THEN AMFICODE
            WHEN REFCODE4 IS NOT NULL AND REPLACE(UPPER(REFCODE4),' ','') LIKE 'APRN%' THEN REFCODE4
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'APRN%' THEN AMFICODE
            ELSE AMFICODE
        END AS KEY_VALUE,
        CASE
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'IN%' THEN 'AMFI_IN'
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'ARN%' AND REFCODE4 IS NULL THEN 'AMFI_ARN'
            WHEN REFCODE4 IS NOT NULL AND REPLACE(UPPER(REFCODE4),' ','') LIKE 'APRN%' THEN 'APRN'
            WHEN AMFICODE IS NOT NULL AND REPLACE(UPPER(AMFICODE),' ','') LIKE 'APRN%' THEN 'APRN'
            ELSE 'AMFI_OTHER'
        END AS KEY_SOURCE
    FROM PMS_HOLDING_BASE
)
SELECT
 CONCAT(CLIENTID,'-', UPPER(REPLACE(IFNULL(KEY_VALUE,''), ' ', '')),'-',SCHEMEID,'-PMS') AS SOURCE_SYSTEM_ID,
 UPPER(REPLACE(IFNULL(KEY_VALUE,''), ' ', '')) as CHANNEL_PARTNER_PARENT,
 CASE WHEN PRODUCT_TYPE IS NOT NULL
      THEN IFNULL(CONCAT(upper(REPLACE(IFNULL(KEY_VALUE,''),' ','')) ,'-', PRODUCT_TYPE),'')
      ELSE ''
 END AS CHANNEL_PARTNER,
 CONCAT(CLIENTID,'-PMS') AS FOLIO_ACCOUNT,
 SUBSTR(SCHEMENAME,1,79) AS PRODUCT_NAME,
 SCHEMEID AS PRODUCT_CODE,
 AS_ON_DATE,
 round(CURRENT_MARKET_VALUE,3) AS CURRENT_MARKET_VALUE
FROM keyed
WHERE KEY_SOURCE = 'APRN'
"""
)

print("amc_pms_holding_aprn_df: ")
amc_pms_holding_aprn_df.show(5)


# -------------------------------AMC HOLDING PMS-Co-inv------------------------------------

amc_pms_co_holding_df = spark.sql(
    """
SELECT * FROM (
SELECT
 CONCAT(cm.CLIENTID,'-', UPPER(REPLACE(IFNULL(im.AMFICODE,''), ' ', '')),'-',sm.SCHEMEID,'-PMS_Co_Inv') AS SOURCE_SYSTEM_ID,
 UPPER(REPLACE(IFNULL(im.AMFICODE,''), ' ', '')) as CHANNEL_PARTNER_PARENT,
  CASE WHEN upper(trim(sm.SCHEMEID)) = upper(trim(prd_cp_code.PRODUCT_CODE))
            THEN IFNULL(CONCAT(upper(REPLACE(im.AMFICODE,' ','')) ,'-', prd_cp_code.PRODUCT_TYPE),'')
            ELSE ''
            END AS CHANNEL_PARTNER,
 CONCAT(cm.CLIENTID,'-PMS_Co_Inv') AS FOLIO_ACCOUNT,
 SUBSTR(sm.SCHEMENAME,1,79) AS PRODUCT_NAME,
 sm.SCHEMEID AS PRODUCT_CODE,
 ppd.FROMDATE AS AS_ON_DATE,
 round(sum(ppd.ENDMKTVAL),3) AS CURRENT_MARKET_VALUE
FROM PMS_CO_PORTFOLIO_PERF_DAILY ppd
INNER JOIN PMS_CO_CLIENT_M cm ON cm.CLIENTID  = ppd.CLIENTID and cm.CLIENTFLAG in ('I','N','D','A','C')
INNER JOIN PMS_CO_SCHEME_M sm  ON sm.SCHEMEID  = cm.SCHEMEID
INNER JOIN PMS_CO_INTERMEDIARY_M im  ON im.INTERID  = cm.INTERID
LEFT JOIN PRD_CP_CODE_MAPPING_V as prd_cp_code on upper(trim(sm.SCHEMEID)) = upper(trim(prd_cp_code.PRODUCT_CODE))
GROUP BY CONCAT(cm.CLIENTID,'-', UPPER(REPLACE(IFNULL(im.AMFICODE,''), ' ', '')),'-',sm.SCHEMEID,'-PMS_Co_Inv'), UPPER(REPLACE(IFNULL(im.AMFICODE,''), ' ', '')), CHANNEL_PARTNER ,CONCAT(cm.CLIENTID,'-PMS_Co_Inv'), sm.SCHEMENAME,sm.SCHEMEID, ppd.FROMDATE
) amc_pms_co_holding
"""
)

print("amc_pms_co_holding_df: ")
amc_pms_co_holding_df.show(5)

# -------------------------------------------------------------------
# entity = {"SF_MF_NAV":[amc_mf_nav_df,bkt_name,appflow_bckt],"SF_AIF_NAV":[amc_aif_nav_df,aif_bkt_name,aif_appflow_bkt],"SF_MF_HOLDING":[amc_mf_holding_df,bkt_name,appflow_bckt],"SF_AIF_HOLDING":[amc_aif_holding_df,aif_bkt_name,aif_appflow_bkt],"SF_PMS_HOLDING":[amc_pms_holding_non_aprn_df,["SOURCE_SYSTEM_ID"],bkt_name,appflow_bckt],"SF_PMS_HOLDING_APRN":[amc_pms_holding_aprn_df,["SOURCE_SYSTEM_ID"],bkt_name,appflow_bckt],"SF_PMS_CO_HOLDING":[amc_pms_co_holding_df,["SOURCE_SYSTEM_ID"],bkt_name,appflow_bckt]}

entity = {
    "SF_PMS_HOLDING": [
        amc_pms_holding_non_aprn_df,
        ["SOURCE_SYSTEM_ID"],
        bkt_name,
        appflow_bckt,
    ],
    "SF_PMS_HOLDING_APRN": [
        amc_pms_holding_aprn_df,
        ["SOURCE_SYSTEM_ID"],
        bkt_name,
        appflow_bckt,
    ],
    "SF_PMS_CO_HOLDING": [
        amc_pms_co_holding_df,
        ["SOURCE_SYSTEM_ID"],
        bkt_name,
        appflow_bckt,
    ],
}


for entity_name in entity.keys():
    if (
        entity_name == "SF_MF_NAV"
        or entity_name == "SF_AIF_NAV"
        or entity_name == "SF_MF_HOLDING"
        or entity_name == "SF_AIF_HOLDING"
    ):
        print("-" * 40)
        print("JOB STARTING FOR :", entity_name)
        curr_df = entity.get(entity_name)[0]
        bkt_name = entity.get(entity_name)[1]
        appflow_bckt = entity.get(entity_name)[2]

        curr_filepath = folder_path + "/inc/" + entity_name + "/rundate=" + currdt + "/"
        appflow_filepath = folder_path + "/" + entity_name + "/"
        print("curr_filepath: ", curr_filepath)
        print("appflow_filepath: ", appflow_filepath)

        write_csv(curr_df, bkt_name, curr_filepath)
        write_appflow_file(curr_df, appflow_bckt, appflow_filepath)

        print("Renaming inbound bucket NAV files")
        rename_s3_file(bkt_name, curr_filepath, entity_name + "_" + currdt)
        print("Renaming Appflow bucket NAV files")
        rename_s3_file(appflow_bckt, appflow_filepath, entity_name + "_" + currdt)
        print(
            "Current date {0} file saved and renamed in S3 {1}, and in appflow bckt {2}".format(
                currdt, bkt_name + "/" + curr_filepath, appflow_bckt + "/" + appflow_filepath
            )
        )
        print("-" * 40)

    elif (
        entity_name == "SF_PMS_HOLDING"
        or entity_name == "SF_PMS_CO_HOLDING"
        or entity_name == "SF_PMS_HOLDING_APRN"
    ):
        print("JOB STARTING FOR :", entity_name)
        curr_df = entity.get(entity_name)[0]
        pk = entity.get(entity_name)[1]
        bkt_name = entity.get(entity_name)[2]
        app_bkt = entity.get(entity_name)[3]
        print("pk : ", pk, " bkt_name : ", bkt_name, " app_bkt : ", app_bkt)
        print("Searching for previous file for ", entity_name)
        prev_filepath = getPrevFile(entity_name, bkt_name)
        if prev_filepath == 0:
            print("No previous full load file for ", entity_name)
        curr_filepath = folder_path + "/fl/" + entity_name + "/rundate=" + currdt + "/"
        print("curr_filepath: ", curr_filepath)
        write_csv(curr_df, bkt_name, curr_filepath)
        print("Current date {} file saved in S3".format(currdt))
        if prev_filepath != 0:
            print("Previous Full load file from: {}".format(prev_filepath))
        print("Calculating Incrementals")
        compute_incrementals(entity_name, pk, prev_filepath, curr_filepath, bkt_name, app_bkt)
        print("Renameing the Full-load  and Incrementals files..")
        rename_s3_file(bkt_name, curr_filepath, entity_name + "_FULL_" + currdt)
        inc_curr_filepath = folder_path + "/inc/" + entity_name + "/rundate=" + currdt + "/"
        print("Incrementals path and file name ", inc_curr_filepath + entity_name + "_" + currdt)
        rename_s3_file(bkt_name, inc_curr_filepath, entity_name + "_INC_" + currdt)
        print("Renameing appflow incremental file ..")
        app_curr_filepath = folder_path + "/" + entity_name + "/"
        print("appflow incremental file path  ", app_curr_filepath)
        rename_s3_file(app_bkt, app_curr_filepath, entity_name + "_INC_" + currdt)
    else:
        print("No Business to run..")

print("-" * 20, "JOB COMPLETED.!", "-" * 20)
job.commit()
