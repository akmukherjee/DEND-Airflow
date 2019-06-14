from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 
                 redshift_conn_id="",
                 sql_query="",
                 tableList="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.tableList = tableList

    def execute(self, context):
        self.log.info('DataQualityOperator implemented ')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        qualityFlag = True
        # For each table check if there are any with 0 records and if so Fail the Overall Data Quality Test
        for table in self.tableList:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                qualityFlag= False
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                qualityFlag= False
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
        #If ALL Tables are Non Empty, we pass the Data Quality
        if (qualityFlag == True):
            logging.info(f"Data Quality Test PASSED!")
        else:
             logging.info(f"Data Quality Test FAILED!")
        