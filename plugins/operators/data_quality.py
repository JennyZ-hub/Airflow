from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
             redshift_conn_id="",
             table="",
             column="",
             *args, **kwargs):  

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.column=column

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.run(f"SELECT COUNT(*) FROM {self.table}")
        column_null=redshift.run(f"SELECT COUNT(*) FROM {self.table} WHERE {self.column} IS NULL")
        self.log.info(f'{column_null}')
        if records ==0:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        if column_null!=None:
            raise ValueError(f"Data quality check failed. {self.column} contained NULL")
        self.log.info(f"Data quality on table {self.table} passed")