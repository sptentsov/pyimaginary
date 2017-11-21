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
        engine.execution_options(autocommit=True).execute('exec VK.discovering.merge_groups_members')

        print('group members updated')

    def get_interesting_users(self):
        conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
        users = pd.read_sql('select user_id '
                            'from dbo.vw_interesing_users  '
                            , conn
                            )
        return list(users['user_id'])

    def update_users_groups(self):
        upload_to_sql_block_size = 1000  # каждую 1000 обработанных юзеров отправляем результат в SQL

        users = self.get_interesting_users()

        for b in range(0, len(users), upload_to_sql_block_size):
            print('Users groups block extraction launched. processed', b, 'users of', len(users))
            users_block = users[b:b + upload_to_sql_block_size]
            users_groups = self.vkp.get_users_groups(users_block)

            # upload users to staging
            print('uploading block with users_groups to sql')
            connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
            engine = sqlalchemy.create_engine(connection_string)
            users_groups.to_sql(schema='staging', name='users_groups', con=engine, index=False, if_exists='replace')

            # write only enters/exits to dbo using SQL sproc
            print('mergin\' state diff inside SQL')
            engine.execution_options(autocommit=True).execute('exec VK.discovering.merge_users_groups')

            print('block of users_groups uploaded to SQL')

        print('update_users_groups finished')

    def update_groups(self):
        conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
        groups = pd.read_sql('select group_id '
                             'from dbo.vw_groups  '
                             'order by users_with_grp desc'
                             , conn
                            )
        groups_list = list(groups['group_id'])

        upload_to_sql_block_size = 2000
        for b in range(0, len(groups_list), upload_to_sql_block_size):
            print('Group info block extraction launched. processed', b, 'groups of', len(groups_list))
            block = groups_list[b:b + upload_to_sql_block_size]

            groups_info = self.vkp.get_groups_info(block)

            # upload users to staging
            print('uploading block with users_groups to sql')
            connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
            engine = sqlalchemy.create_engine(connection_string)
            groups_info.to_sql(schema='staging', name='groups', con=engine, index=False, if_exists='replace')

            # write only enters/exits to dbo using SQL sproc
            print('mergin\' state diff inside SQL')
            engine.execution_options(autocommit=True).execute('exec VK.discovering.merge_groups')

            print('block of group info uploaded to SQL')
