####################################################################################
# Description : Script to run athena query from API and store the results to Dataset..
# ParamStore  : SYSTEM.S3BUCKET.DLZ, SYSTEM.S3BUCKET.LZ, SYSTEM.S3BUCKET.API.URL,
#               SYSTEM.ENVIRONMENT,SYSTEM.AWSREGION, <YOUR-PAT-NAME>
# Dependencies : amorphicutils.zip (> v4.0), playground_query_executor.py
# GlueVersion : 5.0

# Job Arguments 
#   Mandatory Arguments.
#   --user_id : Amorphic UserId
#   --pat_param_name : Name of the parameter in Parameter Store with PAT details.
#                      Value stored should be in below format.
#                      {
#   	                    "<ENVIRONMENT>" : {
#   	                        "pat" : "<USER_PAT>",
#                             "role" : "<ROLE_ID>"
#   	                        }
#                      }
#   --output_dataset :  Output DatasetName. e.g. domain:dataset
#   --sql_dataset :  DatasetName where SQL is stored. e.g. sql_domain:sql_dataset
#   --read_sql_file_name : SQL file name to be read from S3. e.g. sample_data_query.txt
#
#   Below are Optional Arguments
#       --log_lvl : (Optional) Sets the logging verbosity for the job.
#             Accepted values are DEBUG, INFO, WARNING, ERROR, or CRITICAL. Default 'INFO
#       --work_group: primary or AmazonAthenaEngineV3
#       --query_target_location: 'redshift' or 'athena'. Default 'athena'
#       --assume_role: 'yes' uses User IAM role. Default 'no'
#       --query_status_retry: No of times to retry before checking query status again. Default 20
#       --query_status_retry_delay: Delay in seconds for each retry. Default 15
####################################################################################


import sys
import boto3
import json
import logging
import amorphicutils.awshelper as amorphic_helper
from awsglue.utils import getResolvedOptions
from amorphicutils.pyspark.infra.gluespark import GlueSpark
from amorphicutils.pyspark import write
from amorphicutils.api.apiwrapper import ApiWrapper
from playground_query_executor import PlaygroundQueryExecutor


# Initialize GlueContext and SparkContext
glue_spark = GlueSpark()
glue_context = glue_spark.get_glue_context()
spark = glue_spark.get_spark()

# Configure Logging
log_level_str = "INFO"
if "--log_lvl" in sys.argv:
    opt_args = getResolvedOptions(sys.argv, ["log_lvl"])
    log_level_str = opt_args.get("log_lvl", "INFO").upper()

log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(level=log_level_str)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(log_level)

athena_client = boto3.client("athena")
ssm_client = boto3.client("ssm")


LZ_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.LZ", WithDecryption=False
)["Parameter"]["Value"]
DLZ_BUCKET_NAME = ssm_client.get_parameter(
    Name="SYSTEM.S3BUCKET.DLZ", WithDecryption=False
)["Parameter"]["Value"]
AMORPHIC_API_URL = ssm_client.get_parameter(
    Name="SYSTEM.API.URL", WithDecryption=False
)["Parameter"]["Value"]
AMORPHIC_ENV = ssm_client.get_parameter(
    Name="SYSTEM.ENVIRONMENT", WithDecryption=False
)["Parameter"]["Value"]
AMORPHIC_REGION = ssm_client.get_parameter(
    Name="SYSTEM.AWSREGION", WithDecryption=False
)["Parameter"]["Value"]

print(f"Region : {AMORPHIC_REGION}, Env: {AMORPHIC_ENV}")

args = getResolvedOptions(sys.argv, ["user_id", "output_dataset", "pat_param_name"])
AMORPHIC_PAT_DETAILS = ssm_client.get_parameter(
    Name=args["pat_param_name"], WithDecryption=True
)["Parameter"]["Value"]

USERID = args["user_id"]
W_DOMAIN = args["output_dataset"].split(":")[0]
W_DATASET = args["output_dataset"].split(":")[1]

auth_key = json.loads(AMORPHIC_PAT_DETAILS)[AMORPHIC_ENV]["pat"]
role_id = json.loads(AMORPHIC_PAT_DETAILS)[AMORPHIC_ENV]["role"]
api = ApiWrapper(
    url=AMORPHIC_API_URL, env=AMORPHIC_ENV, auth_key=auth_key, role_id=role_id
)


def read_sql_file(bucket_name, prefix, sql_file_name, region_name=None):
    """
    Reads a .sql file from S3 and returns the query as a string.
    :param bucket_name: Name of the S3 bucket. Defaults to 'us-east-1'.
    :param region: region of AWS S3 bucket.
    :param prefix: path of the SQL file in the bucket.
    :param sql_file_name: Name of the SQL file in the bucket.
    :return: SQL query as a string
    """

    LOGGER.info(
        "In read_sql_file, Bucket: %s, Prefix: %s, Filename %s",
        bucket_name,
        prefix,
        sql_file_name,
    )

    response_contents = amorphic_helper.list_bucket_objects(
        bucket_name=bucket_name, prefix=prefix, region=region_name
    )

    # print(response_contents)
    for content in response_contents:
        key = content["Key"]
        # filename pattern : <USERID>_<DATASET_ID>_<UPLOAD_EPOCH>_<FILENAME>
        filename = "_".join(key.split("/")[-1].split("_")[3:])
        if filename == sql_file_name:
            LOGGER.info("Reading the SQL file : %s", filename)
            s3_data = amorphic_helper.get_object(
                bucket_name=bucket_name, object_name=key
            )
            query = s3_data.read().decode("utf-8")
            return query

    LOGGER.info("File : %s not found. Check the file name.", sql_file_name)
    return None


def write_data(df, domain, dataset, user, full_reload=False, **kwargs):
    """
    Writes data to the Amorphic S3 using AmorphicUtils

    :param df: spark dataframe
    :param domain: Output Domain name for the dataset
    :param dataset: Output Dataset name
    :param user: Amorphic User ID with write access to the dataset.
    :return: Response dictionary containing exit codes,data and message.
    """
    csv_writer = write.Write(LZ_BUCKET_NAME, glue_context)
    response = csv_writer.write_csv_data(
        df,
        domain_name=domain,
        dataset_name=dataset,
        user=user,
        full_reload=full_reload,
        **kwargs,
    )

    if response["exitcode"] == 0:
        LOGGER.info("Successfully written data to view dataset.")
    else:
        LOGGER.error(
            "Failed to write output to view dataset with error {error}".format(
                error=response["message"]
            )
        )
        raise Exception(response["message"])
    return response


def execute_db_query(
    athena_query,
    work_group="primary",
    query_target_location="athena",
    assume_role="no",
    retry=5,
    delay=60,
):
    """
    Execute Athena Query and return the result as Spark DataFrame
    :param athena_query: SQL Query to be executed in Athena
    :param work_group: primary or AmazonAthenaEngineV3
    :param query_target_location: 'redshift' or 'athena'
    :param assume_role: 'yes' uses User IAM role.
    :param retry: No of times to retry before checking query status again.
    :param delay: Delay in seconds for each retry.
    :return: Spark DataFrame containing the result of the query
    """
    LOGGER.info("In execute_db_query")
    executor = PlaygroundQueryExecutor(
        api=api,
        query=athena_query,
        work_group=work_group,
        query_target_location=query_target_location,
        assume_role=assume_role,
        retry=retry,
        delay=delay,
        logger=LOGGER,
    )
    pd_df = executor.fetch_query_results()
    athena_df = spark.createDataFrame(pd_df.astype(str))
    return athena_df


def main():
    """
    Main
    """
    LOGGER.info("In Main")

    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "sql_dataset", "read_sql_file_name"]
    )
    sql_dataset = args["sql_dataset"]
    read_sql_file_name = args["read_sql_file_name"]
    print(f"SQL Dataset: {sql_dataset}, SQL File Name: {read_sql_file_name}")

    # in_domain:dataset -> in_domain/dataset/
    sql_dataset_prefix = f"{sql_dataset.strip().replace(':','/')}/"
    print(f"SQL Dataset Prefix: {sql_dataset_prefix}")

    optional_args = {
        "work_group": "primary",
        "query_target_location": "athena",
        "assume_role": "no",
        "query_status_retry": 20,
        "query_status_retry_delay": 15,
    }  # Retry Mins = 20 X 15 = 300s = 5 mins
    for k, v in optional_args.items():
        args[k] = (
            getResolvedOptions(sys.argv, [k])[k]
            if (f"--{k}" in sys.argv or k in sys.argv)
            else v
        )

    query = read_sql_file(DLZ_BUCKET_NAME, sql_dataset_prefix, read_sql_file_name)
    if query is not None:
        athena_spark_df = execute_db_query(
            query,
            work_group=optional_args["work_group"],
            query_target_location=optional_args["query_target_location"],
            assume_role=optional_args["assume_role"],
            retry=int(args["query_status_retry"]),
            delay=int(args["query_status_retry_delay"]),
        )

        if not athena_spark_df.rdd.isEmpty():
            # Show the result
            athena_spark_df.show()

            # Write the result to the output dataset
            response = write_data(
                athena_spark_df,
                domain=W_DOMAIN,
                dataset=W_DATASET,
                user=USERID,
                full_reload=False,
            )
            print("Write Response: ", response)
        else:
            print("No data returned from Athena query.")
    else:
        print("No query to execute. Exiting the job.")
        sys.exit(1)


if __name__ == "__main__":
    main()
