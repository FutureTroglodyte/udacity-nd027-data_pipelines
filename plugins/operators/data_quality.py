from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Applying some data quality checks to all tables in tables_list:
    - Checking table emptyness
    - Cheking for duplicate rows
    - Checking for missing values

    got the get_pandas_df() hint from here:
        https://stackoverflow.com/questions/61555430/how-to-do-store-sql-output-to-pandas-dataframe-using-airflow
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tables_list=[],
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Checking Data Quality")
        for table in self.tables_list:
            df = redshift.get_pandas_df(sql=f"select * from {table}")
            self.log.info(f"Checking {table}")
            # Emptyness
            self.log.info(f"Table {table} is empty: " + str(df.empty))
            self.log.info(f"Table {table}'s number of rows: " + str(df.shape[0]))
            # Duplicate entries
            self.log.info(
                f"Table {table} has duplicate rows: " + str(df.duplicated().any())
            )
            self.log.info(
                f"Table {table}'s number of duplicate rows: "
                + str(df.duplicated().sum())
            )
            # Missing Values
            self.log.info(
                f"Table {table} has missing values: " + str(df.isnull().sum().sum() > 0)
            )
            self.log.info(
                f"Table {table}'s number of missing values: "
                + str(df.isnull().sum().sum())
            )
        self.log.info(f"Checking Data Quality Done!")
