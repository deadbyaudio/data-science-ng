from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        COMPUPDATE OFF
        region '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 aws_region='',
                 jsonpath='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_region = aws_region
        self.jsonpath = jsonpath

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from Redshift table {}'.format(self.table))
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info('Copying data for {} from S3 to Redshift'.format(self.table))
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        json_path = 'auto' if self.jsonpath == 'auto' \
            else "s3://{}/{}".format(self.s3_bucket, self.jsonpath)

        sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_path,
            self.aws_region
        )
        redshift.run(sql)
