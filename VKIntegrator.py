from VKParser import VKParser
import pyodbc
import pandas as pd
import sqlalchemy
import time
from datetime import datetime, timedelta



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

    def post_photos(self, from_group, to_group, since_dt):
        times_to_post = [
            [6, 44]
            , [7, 44]
            , [11, 44]
            , [12, 44]
            , [17, 44]
            , [19, 44]
            , [20, 44]
            , [21, 44]
            , [22, 44]
            , [23, 44]
        ]

        photos = self.vkp.get_photos(from_group)
        exclude = ['-157268412_456239191', '-157268412_456239190', '-157268412_456239189', '-157268412_456239188',
         '-157268412_456239187', '-157268412_456239186', '-157268412_456239185', '-157268412_456239184',
         '-157268412_456239183', '-157268412_456239182', '-157268412_456239181', '-157268412_456239180',
         '-157268412_456239179', '-157268412_456239178', '-157268412_456239177', '-157268412_456239176',
         '-157268412_456239175', '-157268412_456239174', '-157268412_456239173', '-157268412_456239172',
         '-157268412_456239171', '-157268412_456239170', '-157268412_456239169', '-157268412_456239168',
         '-157268412_456239167', '-157268412_456239166', '-157268412_456239165', '-157268412_456239164',
         '-157268412_456239163', '-157268412_456239162', '-157268412_456239161', '-157268412_456239160',
         '-157268412_456239159', '-157268412_456239158', '-157268412_456239157', '-157268412_456239156',
         '-157268412_456239155', '-157268412_456239154', '-157268412_456239153', '-157268412_456239152',
         '-157268412_456239151', '-157268412_456239150', '-157268412_456239149', '-157268412_456239148',
         '-157268412_456239147', '-157268412_456239146', '-157268412_456239145', '-157268412_456239144',
         '-157268412_456239143', '-157268412_456239142', '-157268412_456239141', '-157268412_456239140',
         '-157268412_456239139', '-157268412_456239138', '-157268412_456239137', '-157268412_456239136',
         '-157268412_456239135', '-157268412_456239134', '-157268412_456239133', '-157268412_456239132']
        exclude += ['-157268412_456239131', '-157268412_456239130', '-157268412_456239129', '-157268412_456239128',
         '-157268412_456239127', '-157268412_456239126', '-157268412_456239125', '-157268412_456239124',
         '-157268412_456239123', '-157268412_456239122', '-157268412_456239121', '-157268412_456239120',
         '-157268412_456239119', '-157268412_456239118', '-157268412_456239117', '-157268412_456239116',
         '-157268412_456239115', '-157268412_456239114', '-157268412_456239113', '-157268412_456239112',
         '-157268412_456239111', '-157268412_456239110', '-157268412_456239109', '-157268412_456239108',
         '-157268412_456239107', '-157268412_456239106', '-157268412_456239105', '-157268412_456239104',
         '-157268412_456239103', '-157268412_456239102', '-157268412_456239101', '-157268412_456239100',
         '-157268412_456239099', '-157268412_456239098', '-157268412_456239097', '-157268412_456239096',
         '-157268412_456239095', '-157268412_456239094', '-157268412_456239093', '-157268412_456239092',
         '-157268412_456239091', '-157268412_456239090', '-157268412_456239089', '-157268412_456239088',
         '-157268412_456239087', '-157268412_456239086', '-157268412_456239085', '-157268412_456239084',
         '-157268412_456239083', '-157268412_456239082', '-157268412_456239081', '-157268412_456239080',
         '-157268412_456239079', '-157268412_456239078', '-157268412_456239077', '-157268412_456239076',
         '-157268412_456239075', '-157268412_456239074', '-157268412_456239073', '-157268412_456239072']
        exclude += ['-157268412_456239071', '-157268412_456239070', '-157268412_456239069', '-157268412_456239068',
         '-157268412_456239067', '-157268412_456239066', '-157268412_456239065', '-157268412_456239064',
         '-157268412_456239063', '-157268412_456239062', '-157268412_456239061', '-157268412_456239060',
         '-157268412_456239059', '-157268412_456239058', '-157268412_456239057', '-157268412_456239056',
         '-157268412_456239055', '-157268412_456239054', '-157268412_456239053', '-157268412_456239052']
        print('WARNING! excluded', len(exclude), 'photos')
        photos = [x for x in photos if x not in exclude]
        print('uploading', len(photos), 'photos')


        first_dt = datetime.strptime(since_dt, '%Y-%m-%d')
        days_counter = 0
        for day in range(0, len(photos), len(times_to_post)):
            post_date = first_dt + timedelta(days=days_counter)
            days_counter += 1

            daily_photos = photos[day:day + len(times_to_post)]

            for photo in range(0, len(daily_photos)):
                post_time = post_date.replace(
                    hour=times_to_post[photo][0]
                    , minute=times_to_post[photo][1]
                )
                print(post_time, daily_photos[photo])
                self.vkp.wall_post(
                    owner_id=-to_group
                    , from_group=1  # от имени группы
                    , attachments='photo'+daily_photos[photo]
                    , publish_date=post_time.timestamp()
                )
                time.sleep(0.3)  # to avoid ban
            if days_counter % 3 == 0:
                print('long 5 sec sleep to avoid captcha')
                time.sleep(5)
