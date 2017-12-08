import pyodbc
import sqlalchemy
import pandas as pd


class DBInterface:
    def __init__(self):
        self.con = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')

        connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
        self.engine = sqlalchemy.create_engine(connection_string).execution_options(autocommit=True)

    def select_data(self, query):
        print('DBInterface select: ', query)
        return pd.read_sql(sql=query, con=self.con)

    def upload_dataframe(self, df, to_schema, to_table, if_exists):
        df.to_sql(schema=to_schema, name=to_table, con=self.engine, index=False, if_exists=if_exists)

    def run_query(self, query):
        pass

    def get_default_walls_to_scan(self):
        df = self.select_data(query='select source_id from dbo.vw_default_walls_to_scan')
        return list(df['source_id'])

    def posts_truncate_staging(self):
        self.engine.execute('truncate table staging.posts')

    def posts_add_to_staging(self, posts):
        self.upload_dataframe(self, df=posts, to_schema='staging', to_table='posts', if_exists='append')
        pass

    def posts_merge(self):
        pass
