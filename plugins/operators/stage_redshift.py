from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Copying a data as it is from S3 to redshift.

    Code is basically taken from Lesson 3 - Exercise 1
    """

    ui_color = "#358140"
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        jsonpath="",
        delimiter=",",
        ignore_headers=1,
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.jsonpath = jsonpath
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.jsonpath != "":
            copy_sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                IGNOREHEADER {self.ignore_headers}
                JSON '{self.jsonpath}'
                COMPUPDATE off
                REGION 'us-west-2'
                EMPTYASNULL
                BLANKSASNULL
                TRUNCATECOLUMNS
            """
        else:
            copy_sql = f"""
                COPY {self.table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                -- IGNOREHEADER {self.ignore_headers}
                -- DELIMITER '{self.delimiter}'
                JSON 'auto'
                COMPUPDATE off
                REGION 'us-west-2'
                EMPTYASNULL
                BLANKSASNULL
                TRUNCATECOLUMNS
            """
        self.log.info(copy_sql)
        redshift.run(copy_sql)
        self.log.info("Done!")
