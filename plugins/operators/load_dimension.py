from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Inserts data into dim table, given the tables's name & a select statement.

    Does truncate the table at first.
    """

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", table="", select_statement="", *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_statement = select_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Loading dimension table")
        redshift.run(f"INSERT INTO {self.table} " + self.select_statement)
        self.log.info("Done!")
