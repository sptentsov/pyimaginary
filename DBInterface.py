import pyodbc
import sqlalchemy
import pandas as pd


class DBInterface:
    def __init__(self):
        self.con = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')

        connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'

        # нужен для выполнения запросов в базу, дабы они не откатывались
        self.engine_autocommit = sqlalchemy.create_engine(connection_string).execution_options(autocommit=True)

        # пандовый to_sql сам в конце коммитит, поэтому ему автокоммит не подходит
        self.engine = sqlalchemy.create_engine(connection_string)

    def select_data(self, query):
        print('DBInterface select: ', query)
        return pd.read_sql(sql=query, con=self.con)

    def upload_dataframe(self, df, to_schema, to_table, if_exists):
        df.to_sql(schema=to_schema, name=to_table, con=self.engine, index=False, if_exists=if_exists)

    def run_query(self, query):
        pass

    # ---------------------------------------- groups members ----------------------------------------
    def groups_members_truncate_staging(self):
        self.engine_autocommit.execute('truncate table staging.groups_members')

    def groups_members_add_to_staging(self, members):
        self.upload_dataframe(df=members, to_schema='staging', to_table='groups_members', if_exists='append')

    def groups_members_merge(self):
        self.engine_autocommit.execute('exec discovering.merge_groups_members')

    # ---------------------------------------- selects ----------------------------------------
    def get_default_groups_to_update_members(self):
        df = self.select_data(query='''
            select group_id
            from dict.groups_to_monitor
            where is_members_scan_enabled = 1
        ''')
        return list(df['group_id'])

    def get_default_walls_to_scan(self):
        df = self.select_data(query='select source_id from dbo.vw_default_walls_to_scan')
        return list(df['source_id'])

    # ---------------------------------------- posts ----------------------------------------
    def posts_truncate_staging(self):
        self.engine_autocommit.execute('truncate table staging.posts')

    def posts_add_to_staging(self, posts):
        self.upload_dataframe(df=posts, to_schema='staging', to_table='posts', if_exists='append')

    def posts_merge(self):
        self.engine_autocommit.execute('exec discovering.merge_posts')

    # ---------------------------------------- likes and reposts ----------------------------------------
    def likes_and_reposts_truncate_staging(self):
        self.engine_autocommit.execute('truncate table staging.likes_and_reposts')

    def likes_and_reposts_to_staging(self, data):
        self.upload_dataframe(df=data, to_schema='staging', to_table='likes_and_reposts', if_exists='append')

    def likes_and_reposts_merge(self):
        self.engine_autocommit.execute('exec discovering.merge_likes_and_reposts')