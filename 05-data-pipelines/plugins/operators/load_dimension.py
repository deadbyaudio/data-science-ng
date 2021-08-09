from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 append_data=False,
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Load dimension table {}'.format(self.table))
        if not self.append_data:
            self.log.info('Deleting info from table {}'.format(self.table))
            redshift.run('DELETE FROM {}'.format(self.table))

        sql_statement = 'INSERT INTO {} {}'.format(self.table, self.query)
        redshift.run(sql_statement)
