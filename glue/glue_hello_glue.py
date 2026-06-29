import boto3
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from pyspark.context import SparkContext

def hello_glue():
    """
    Lists the job definitions in your AWS Glue account, using the AWS SDK for Python (Boto3).
    """
    try:
        # Create the Glue client
        glue = boto3.client("glue")

        # List the jobs, limiting the results to 10 per page
        paginator = glue.get_paginator("get_jobs")
        response_iterator = paginator.paginate(
            PaginationConfig={"MaxItems": 10, "PageSize": 10}
        )

        # Print the job names
        print("Here are the jobs in your account:")
        for page in response_iterator:
            for job in page["Jobs"]:
                print(f"\t{job['Name']}")

    except ClientError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    sc = SparkContext()
    glueContext = GlueContext(sc)

    logger = glueContext.get_logger()
    logger.info("info message")
    logger.warn("warn message")
    logger.error("error message")

    hello_glue()



