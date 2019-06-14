from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields    = ("s3_key",)
    sql_csv = """ 
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    CSV COMPUPDATE OFF;
    """
    
    sql_json = """ 
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    json '{}' COMPUPDATE OFF;
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 format= "json",
                 jsonPath = "auto",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format = format
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.jsonPath = jsonPath

    def execute(self, context):
        self.log.info('StageToRedshiftOperator implemented ')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.format == 'json'):
            #Set the JSON SQL
            #if JSONPath is auto, set s3_jsonPath to auto else set the JSONPath (Assumed to be a prefix in the same bucket)
            s3_jsonpath = self.jsonPath
            if(self.jsonPath != "auto"):
                s3_jsonpath = "s3://{}/{}".format(self.s3_bucket, self.jsonPath)
            formatted_sql = StageToRedshiftOperator.sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                s3_jsonpath)
            
        else:
            #Set the CSV SQL
            formatted_sql = StageToRedshiftOperator.sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key)
       
        redshift.run(formatted_sql)




