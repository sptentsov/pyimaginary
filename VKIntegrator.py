from VKParser import VKParser
import pyodbc
import pandas as pd
import sqlalchemy

class VKIntegrator:
    def __init__(self):
        self.vkp = VKParser()

    def update_group_members(self):
        # load list of groups to check users
        conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
        groups = pd.read_sql('select group_id '
                             'from dict.groups_to_monitor '
                             'where is_scan_enabled = 1'
                             , conn
                             )

        # download members of each group using VK API
        members = pd.DataFrame()
        for g in groups['group_id']:
            members = members.append(pd.DataFrame({'group_id': g, 'user_id': self.vkp.get_group_members(g)}))

        # upload users to staging
        print('uploading groups\' members to sql')
        connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
        engine = sqlalchemy.create_engine(connection_string)
        members.to_sql(schema='staging', name='groups_members', con=engine, index=False, if_exists='replace')

        # write only enters/exits to dbo using SQL sproc
        print('mergin\' state diff inside SQL')
        engine.execution_options(autocommit=True).execute('exec VK.discovering.merge_groups')

        print('group members updated')

