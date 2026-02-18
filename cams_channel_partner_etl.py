import sys
import json
from datetime import datetime, timedelta

import boto3
import dateutil.tz
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *


def get_optional_arg(arg_name, default_value):
    if f"--{arg_name}" in sys.argv:
        return getResolvedOptions(sys.argv, [arg_name])[arg_name]
    return default_value


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bkt_name",
        "appflow_bckt",
        "folder_path",
        "mf_secret",
        "sif_secret",
        "state_ref_folder_path",
        "country_ref_folder_path",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Required args
mf_secret = args["mf_secret"]
sif_secret = args["sif_secret"]
bkt_name = args["bkt_name"]
appflow_bckt = args["appflow_bckt"]
folder_path = args["folder_path"]
state_ref_folder_path = args["state_ref_folder_path"]
country_ref_folder_path = args["country_ref_folder_path"]

# Optional args for MF and SIF specific behavior.
source_schema_mf = get_optional_arg("source_schema_mf", "STIIFL")
source_schema_sif = get_optional_arg("source_schema_sif", "SIFIFSP")

mf_folder_path = get_optional_arg("mf_folder_path", folder_path)
sif_folder_path = get_optional_arg("sif_folder_path", folder_path)

mf_appflow_bckt = get_optional_arg("mf_appflow_bckt", appflow_bckt)
sif_appflow_bckt = get_optional_arg("sif_appflow_bckt", appflow_bckt)
mf_appflow_folder_path = get_optional_arg("mf_appflow_folder_path", "SF")
sif_appflow_folder_path = get_optional_arg("sif_appflow_folder_path", "SF")

mf_entity_name = get_optional_arg("mf_entity_name", "SF_CP_CHILD")
sif_entity_name = get_optional_arg("sif_entity_name", "SF_SIF_CP_CHILD")

if mf_entity_name == sif_entity_name:
    raise ValueError("mf_entity_name and sif_entity_name must be different.")

ist = dateutil.tz.gettz("Asia/Kolkata")
currdt = (datetime.now(tz=ist) - timedelta(days=1)).strftime("%Y-%m-%d")
s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


def get_connection_details(secret_id, label):
    print(f"Retrieving connection details for {label}...")
    secret_response = secrets_client.get_secret_value(SecretId=secret_id)
    secret_payload = json.loads(secret_response["SecretString"])
    jdbc_url = (
        secret_payload.get("pg_url")
        or secret_payload.get("jdbc_url")
        or secret_payload.get("url")
    )
    username = (
        secret_payload.get("pg_user")
        or secret_payload.get("username")
        or secret_payload.get("user")
    )
    password = secret_payload.get("pg_password") or secret_payload.get("password")

    if not jdbc_url or not username or not password:
        raise ValueError(
            f"{label} secret is missing DB fields. "
            "Expected keys like pg_url/pg_user/pg_password or "
            "jdbc_url/username/password."
        )

    print(f"Connection details retrieved for {label}.")
    return {
        "url": jdbc_url,
        "user": username,
        "password": password,
    }


mf_conn = get_connection_details(mf_secret, "MF")
sif_conn = get_connection_details(sif_secret, "SIF")

print(
    "Runtime config -> "
    f"MF schema: {source_schema_mf}, SIF schema: {source_schema_sif}, "
    f"MF folder: {mf_folder_path}, SIF folder: {sif_folder_path}, "
    f"MF AppFlow path: s3://{mf_appflow_bckt}/{mf_appflow_folder_path}/{mf_entity_name}/, "
    f"SIF AppFlow path: s3://{sif_appflow_bckt}/{sif_appflow_folder_path}/{sif_entity_name}/"
)


def read_csv(path, bucket):
    s3_path = f"s3://{bucket}/{path}"
    return (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("delimiter", "|")
        .csv(s3_path)
    )


def create_df(table_name, business_name, connection_details):
    print(f"Connecting to {business_name} for table {table_name}")
    try:
        return (
            spark.read.format("jdbc")
            .option("url", connection_details["url"])
            .option("user", connection_details["user"])
            .option("password", connection_details["password"])
            .option("dbtable", table_name)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    except Exception as e:
        print(
            f"Error: JDBC connection issue for {business_name} "
            f"table {table_name}: {e}"
        )
        raise


def write_csv(df, bucket_name, path):
    formatted_df = df
    for col_name in formatted_df.columns:
        formatted_df = formatted_df.withColumn(
            col_name, regexp_replace(col(col_name), "[\\r\\n]+", " ")
        )
        formatted_df = formatted_df.withColumn(
            col_name, regexp_replace(col(col_name), "\\|", "")
        )

    s3_path = f"s3://{bucket_name}/{path}"
    (
        formatted_df.coalesce(1)
        .distinct()
        .write.mode("overwrite")
        .format("csv")
        .option("header", "True")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("quote", '"')
        .option("delimiter", "|")
        .save(s3_path)
    )
    print(f"File saved successfully at {s3_path}")


def write_appflow_file(df, bucket_name, path):
    s3_path = f"s3://{bucket_name}/{path}"
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
        .save(s3_path)
    )
    print(f"AppFlow file saved successfully at {s3_path}")


def get_delta(prev_df, curr_df, pkey):
    prev_hashed = prev_df.na.fill("").withColumn(
        "hashvalue", sha2(concat_ws("||", *prev_df.columns), 256)
    )
    curr_hashed = curr_df.na.fill("").withColumn(
        "hashvalue", sha2(concat_ws("||", *curr_df.columns), 256)
    )

    updated_df = (
        curr_hashed.alias("curr")
        .join(prev_hashed.alias("prev"), pkey, "inner")
        .filter(col("prev.hashvalue") != col("curr.hashvalue"))
        .withColumn("OP", lit("U"))
        .select("OP", "curr.*")
    )
    insert_df = (
        curr_hashed.alias("curr")
        .join(prev_hashed.alias("prev"), pkey, "left_anti")
        .withColumn("OP", lit("I"))
        .select("OP", "curr.*")
    )

    return updated_df.unionByName(insert_df, allowMissingColumns=True).drop("hashvalue")


def get_prev_file(entity_name, bucket, base_folder):
    dates = set()
    fullload_path = f"{base_folder}/fl/{entity_name}/"
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=fullload_path):
        for value in page.get("Contents", []):
            key = value["Key"]
            if key.endswith(".csv"):
                run_date_folder = key.split("/")[-2]
                dates.add(run_date_folder)

    dates = sorted(list(dates), reverse=True)
    if len(dates) > 0 and dates[0] != f"rundate={currdt}":
        prev_path = f"{base_folder}/fl/{entity_name}/{dates[0]}/"
        print(f"Previous file path: {prev_path}")
        return prev_path
    if len(dates) > 1 and dates[0] == f"rundate={currdt}":
        prev_path = f"{base_folder}/fl/{entity_name}/{dates[1]}/"
        print(f"Previous file path: {prev_path}")
        return prev_path
    print(f"No previous file found for {entity_name}")
    return 0


def compute_incrementals(
    entity_name,
    pkeys,
    prev_filepath,
    curr_filepath,
    data_bucket,
    base_folder,
    appflow_bucket,
    appflow_folder,
):
    print(f"Computing incrementals for {entity_name}")
    print(f"Previous filepath: {prev_filepath}")
    print(f"Current filepath: {curr_filepath}")

    curr_df = read_csv(curr_filepath, data_bucket)
    prev_df = curr_df.limit(0)
    if prev_filepath != 0:
        prev_df = read_csv(prev_filepath, data_bucket)

    inc_df = get_delta(prev_df, curr_df, pkeys)
    inc_path = f"{base_folder}/inc/{entity_name}/rundate={currdt}/"
    write_csv(inc_df, data_bucket, inc_path)

    appflow_path = f"{appflow_folder}/{entity_name}/"
    write_appflow_file(inc_df, appflow_bucket, appflow_path)

    inc_count = inc_df.count()
    if inc_count > 0:
        print(f"Incrementals written for {entity_name}: {inc_count}")
    else:
        print(f"No incrementals to write for {entity_name}")


def register_business_tables(prefix, source_schema, connection_details, business_name):
    table_names = [
        "BROKER_MASTER",
        "BROKER_BANKS",
        "BROKER_GST_MASTER",
        "PROCESSED_TRXNS",
    ]
    for table_name in table_names:
        full_table_name = f'"{source_schema}"."{table_name}"'
        table_df = create_df(full_table_name, business_name, connection_details)
        table_df.createOrReplaceTempView(f"{prefix}_{table_name}")
    print(f"Views created for {business_name}.")


def build_cp_child_df(prefix, product_type, source_suffix, sif_only):
    sif_filter_clause = ""
    if sif_only:
        sif_filter_clause = "AND COALESCE(TRIM(bm.sif_enabled_flag), 'N') = 'Y'"

    broker_master_view = f"{prefix}_BROKER_MASTER"
    broker_banks_view = f"{prefix}_BROKER_BANKS"
    broker_gst_view = f"{prefix}_BROKER_GST_MASTER"
    processed_trxns_view = f"{prefix}_PROCESSED_TRXNS"

    query = f"""
    SELECT CONCAT(ARN_Code__c, '-{source_suffix}') AS Source_System_Id__c, *
    FROM (
        SELECT
            bm.broker_name AS Account_Name__c,
            '' AS CP_Category__c,
            '' AS CP_Sub_Category__c,
            CASE
                WHEN bm.pan_no LIKE '___A%' THEN 'AOP'
                WHEN bm.pan_no LIKE '___B%' THEN 'BOI'
                WHEN bm.pan_no LIKE '___C%' THEN 'Company'
                WHEN bm.pan_no LIKE '___E%' THEN 'LLP'
                WHEN bm.pan_no LIKE '___F%' THEN 'Firm'
                WHEN bm.pan_no LIKE '___H%' THEN 'HUF'
                WHEN bm.pan_no LIKE '___P%' THEN 'Individual'
                WHEN bm.pan_no LIKE '___T%' THEN 'Trust'
                ELSE ''
            END AS CP_Entity_Status__c,
            bm.address1 AS Address_Line1__c,
            bm.address2 AS Address_Line2__c,
            bm.address3 AS Address_Line3__c,
            bm.city AS Address_Line_City__c,
            CASE
                WHEN bm.state_code = s.DB_STATE THEN UPPER(s.SF_STATE)
                ELSE ''
            END AS Address_Line_State__c,
            CASE
                WHEN bm.state_code = s.DB_STATE THEN UPPER(s.SF_COUNTRY)
                WHEN bm.country = c.DB_COUNTRY THEN UPPER(c.SF_COUNTRY)
                ELSE ''
            END AS Address_Line_Country__c,
            bm.pincode AS Pin_Code__c,
            bm.broker_code AS ARN_Code__c,
            '' AS Alternate_Online_Code__c,
            bm.pan_no AS PAN,
            DATE_FORMAT(bm.arn_exp, 'yyyy/MM/dd') AS ARN_Expiry_Date__c,
            '' AS OwnerId,
            '{product_type}' AS Product_Type__c,
            CASE
                WHEN bm.broker_code LIKE 'ARN-%' THEN 'Distributor'
                WHEN bm.broker_code LIKE 'INZ%' THEN 'RIA'
                WHEN bm.broker_code LIKE 'INA%' THEN 'RIA'
                WHEN bm.broker_code LIKE 'INP%' THEN 'RIA'
                WHEN bm.broker_code LIKE 'INB%' THEN 'RIA'
                WHEN bm.broker_code LIKE 'EOP%' THEN 'RIA'
                ELSE ''
            END AS CP_Type__c,
            '' AS CP_Presence__c,
            '' AS DOB_Date_of_Inc__c,
            '' AS CP_Zone__c,
            '' AS Location__c,
            'Monthly' AS Invoice_Frequency__c,
            '' AS Relationship_Tag__c,
            bb.bankname AS Bank_Name__c,
            bb.bank_city AS Bank_City__c,
            bb.bankacno AS Bank_Account_Number__c,
            bb.bankacno AS Confirm_Bank_Account_Number__c,
            'INR' AS Invoice_Currency__c,
            COALESCE(bgm.GSTIN, '') AS GST_Number__c,
            bb.bankbranch AS GST_Branch__c,
            bb.bankbranch AS Bank_Branch__c,
            bb.ifcsc AS Bank_IFSC_Code__c,
            CASE
                WHEN bb.bank_actype IN ('0') THEN ''
                WHEN bb.bank_actype IN ('SB', 'SA') THEN 'Savings'
                WHEN bb.bank_actype IN ('CA') THEN 'Current'
                ELSE bb.bank_actype
            END AS Bank_Account_Type__c,
            CONCAT_WS(' ', bm.NOMINEE_FIRST_NAME, bm.NOMINEE_MIDDLE_NAME, bm.NOMINEE_LAST_NAME)
                AS Name_of_Nominee__c,
            '' AS Relationship_with_Nominee__c,
            'Not Processed' AS Status,
            bm.EMAIL AS Contact_Person_Email__c,
            bm.MOBILE_NO AS Contact_Person_Mobile__c,
            bm.BROKER_NAME AS Contact_Person_Name__c,
            CASE
                WHEN bm.emplstatus IN (0, 3) THEN 'Not Empanelled'
                WHEN bm.emplstatus = 1 THEN 'Empanelled'
                WHEN bm.emplstatus = 2 THEN 'De-empanelled'
                ELSE 'Not Empanelled'
            END AS Empanelment_Status__c,
            DATE_FORMAT(pt.last_trxn_date, 'yyyy-MM-dd') AS Transaction_Date__c
        FROM {broker_master_view} bm
        LEFT JOIN {broker_banks_view} bb
            ON bm.Broker_code = bb.Brokcode
        LEFT JOIN (
            SELECT
                ARN,
                GSTIN
            FROM (
                SELECT
                    ARN,
                    GSTIN,
                    ROW_NUMBER() OVER (
                        PARTITION BY ARN
                        ORDER BY TIME_STAMP DESC, COMMIT_SCN DESC
                    ) AS rn
                FROM {broker_gst_view}
            ) gst_latest
            WHERE rn = 1
        ) bgm
            ON UPPER(TRIM(bm.Broker_code)) = UPPER(TRIM(bgm.ARN))
        LEFT JOIN state_df s
            ON UPPER(TRIM(s.DB_STATE)) = UPPER(TRIM(bm.state_code))
        LEFT JOIN cntry_df c
            ON UPPER(TRIM(c.DB_COUNTRY)) = UPPER(TRIM(bm.country))
        LEFT JOIN (
            SELECT
                Broker_code,
                MAX(trxn_date) AS last_trxn_date
            FROM {processed_trxns_view}
            GROUP BY Broker_code
            HAVING MAX(trxn_date) IS NOT NULL
        ) pt
            ON pt.Broker_code = bm.Broker_code
        WHERE
            (
                bm.Broker_code LIKE 'ARN-%'
                OR bm.Broker_code LIKE 'INA%'
                OR bm.Broker_code LIKE 'INP%'
                OR bm.Broker_code LIKE 'INZ%'
                OR bm.Broker_code LIKE 'INB%'
                OR bm.Broker_code LIKE 'EOP%'
                OR bm.Broker_code LIKE 'CAT-1-EOP%'
            )
            AND bm.pan_no IS NOT NULL
            AND (
                bm.emplstatus IN (1, 3)
                OR (bm.emplstatus NOT IN (1, 3) AND pt.last_trxn_date IS NOT NULL)
            )
            {sif_filter_clause}
    ) cp
    """
    return spark.sql(query)


print("CAMS channel partner incremental job started for MF + SIF")

cntry_df = (
    spark.read.option("header", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("quote", '"')
    .option("delimiter", ",")
    .csv(country_ref_folder_path)
)
state_df = (
    spark.read.option("header", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("quote", '"')
    .option("delimiter", ",")
    .csv(state_ref_folder_path)
)
cntry_df.createOrReplaceTempView("cntry_df")
state_df.createOrReplaceTempView("state_df")
print("Reference views created.")

register_business_tables(
    prefix="MF",
    source_schema=source_schema_mf,
    connection_details=mf_conn,
    business_name="MF",
)
register_business_tables(
    prefix="SIF",
    source_schema=source_schema_sif,
    connection_details=sif_conn,
    business_name="SIF",
)

cp_child_mf_df = build_cp_child_df(
    prefix="MF", product_type="MF", source_suffix="MF", sif_only=False
)
cp_child_sif_df = build_cp_child_df(
    prefix="SIF", product_type="SIF", source_suffix="SIF", sif_only=True
)

entity_map = {
    mf_entity_name: {
        "df": cp_child_mf_df,
        "pkeys": ["Source_System_Id__c"],
        "bucket": bkt_name,
        "base_folder": mf_folder_path,
        "appflow_bucket": mf_appflow_bckt,
        "appflow_folder": mf_appflow_folder_path,
    },
    sif_entity_name: {
        "df": cp_child_sif_df,
        "pkeys": ["Source_System_Id__c"],
        "bucket": bkt_name,
        "base_folder": sif_folder_path,
        "appflow_bucket": sif_appflow_bckt,
        "appflow_folder": sif_appflow_folder_path,
    },
}

for entity_name, config in entity_map.items():
    data_bucket = config["bucket"]
    print(f"Searching previous file for {entity_name}")
    prev_filepath = get_prev_file(entity_name, data_bucket, config["base_folder"])
    curr_filepath = f"{config['base_folder']}/fl/{entity_name}/rundate={currdt}/"
    print(f"Current filepath: {curr_filepath}")

    curr_df = config["df"]
    write_csv(curr_df, data_bucket, curr_filepath)
    print(f"Current date {currdt} file saved in S3 for {entity_name}")

    if prev_filepath != 0:
        print(f"Previous full load file path: {prev_filepath}")

    compute_incrementals(
        entity_name=entity_name,
        pkeys=config["pkeys"],
        prev_filepath=prev_filepath,
        curr_filepath=curr_filepath,
        data_bucket=data_bucket,
        base_folder=config["base_folder"],
        appflow_bucket=config["appflow_bucket"],
        appflow_folder=config["appflow_folder"],
    )

job.commit()
