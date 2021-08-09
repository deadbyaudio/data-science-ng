from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 append_data=False,
                 table='songplays',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading fact table')
        if not self.append_data:
            self.log.info('Deleting info from fact table')
            redshift.run('DELETE FROM {}'.format(self.table))

        sql_statement = 'INSERT INTO {} {}'.format(self.table, self.query)
        redshift.run(sql_statement)
