from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
             redshift_conn_id="",
             sql_query="", 
             flag="",
             table="",
             *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.flag = flag
        self.table = table

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.flag == "delete_load":
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE {}".format(self.table))
        load_dimsql = self.sql_query
        redshift.run(load_dimsql)