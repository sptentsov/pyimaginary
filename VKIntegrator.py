from VKParser import VKParser
from DBInterface import DBInterface
import pyodbc
import pandas as pd
import sqlalchemy
import time
from datetime import datetime, timedelta


class VKIntegrator:
    def __init__(self):
        self.vkp = VKParser()
        self.dbi = DBInterface()

    def update_group_members(self, groups=[]):
        # load list of groups to check users, if groups are not passed
        if not groups:
            conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
            groups_df = pd.read_sql('select group_id '
                                 'from dict.groups_to_monitor '
                                 'where is_members_scan_enabled = 1'
                                 , conn
                                 )
            groups = list(groups_df['group_id'])

        # create sqlalchemy engine to upload data
        connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
        engine = sqlalchemy.create_engine(connection_string)
        # print truncate SQL staging
        engine.execution_options(autocommit=True).execute('truncate table VK.staging.groups_members')

        counter = 1
        for g in groups:
            print('loading group', counter, 'of', len(groups))
            counter += 1

            for retry in range(3):
                try:
                    members = pd.DataFrame({'group_id': g, 'user_id': self.vkp.get_group_members(g)})
                    break
                except ValueError as err:
                    print(err)
                    print('retry this group in 3 sec..')
                    time.sleep(3)

            # upload users to staging
            print('uploading group', g, 'members to sql staging')
            members.to_sql(schema='staging', name='groups_members', con=engine, index=False, if_exists='append')

        # write only enters/exits to dbo using SQL sproc
        print('mergin\' staging->dbo inside SQL')
        engine.execution_options(autocommit=True).execute('exec VK.discovering.merge_groups_members')

        print('group members updated')

    def e1_extend_groups(self):
        print('executing e1_extend_groups sproc')
        connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
        engine = sqlalchemy.create_engine(connection_string)
        engine.execution_options(autocommit=True).execute('exec VK.ext.e1_extend_groups @basic_group = 144657300')

    def e1_get_extended_groups(self):
        conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
        groups_df = pd.read_sql('select group_id, run_time '
                             'from ext.e1_vw_extended_groups '
                             , conn
                             )
        print('got', groups_df.shape[0], 'extended groups for extension with run time', groups_df['run_time'][0])
        return list(groups_df['group_id'])

    def get_interesting_users(self, source_vw='dbo.vw_interesting_users'):
        conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
        users = pd.read_sql('select user_id '
                            'from ' + source_vw
                            , conn
                            )
        return list(users['user_id'])

    def update_users_groups(self, users):
        upload_to_sql_block_size = 1000  # каждую 1000 обработанных юзеров отправляем результат в SQL

        connection_string = 'mssql+pyodbc://localhost\\SQLEXPRESS/VK?driver=SQL+Server'
        engine = sqlalchemy.create_engine(connection_string)

        for b in range(0, len(users), upload_to_sql_block_size):
            print('Users groups block extraction launched. processed', b, 'users of', len(users))
            users_block = users[b:b + upload_to_sql_block_size]
            users_groups = self.vkp.get_users_groups(users_block)

            # upload users to staging
            print('uploading block with users_groups to sql')
            users_groups.to_sql(schema='staging', name='users_groups', con=engine, index=False, if_exists='replace')

            # write only enters/exits to dbo using SQL sproc
            print('mergin\' state diff inside SQL')
            engine.execution_options(autocommit=True).execute('exec VK.discovering.merge_users_groups')

            print('block of users_groups uploaded to SQL')

        print('update_users_groups finished')

    def update_groups(self, max_groups_to_update=200000):
        conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
        groups = pd.read_sql('select top ' + str(max_groups_to_update) + ' group_id '
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

    def post_photos(self, source_id, source_album, to_group, since_dt):
        # source_id = user_id or minus group_id
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

        photos = self.vkp.get_photos(source_id, source_album)
        exclude = []
        print('WARNING! excluded', len(set(photos).intersection(exclude)), 'photos')
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
                print('long 25 sec sleep to avoid captcha, and meaningless photo api call')
                time.sleep(25)
                self.vkp.get_photos(source_id, source_album)

    def scan_walls(self, source_ids=[]):
        # source_ids - список источников. Либо -group_id, либо user_id

        max_post_age_days = 10  # смотрим на все посты за последние max_post_age_days. по более старым инфо не обновляем
        x_dates_ago = datetime.now().date() - timedelta(days=max_post_age_days)
        oldest_post_unix_time = time.mktime(x_dates_ago.timetuple())

        # если список источников не задан - то берем его из sql вьюхи
        if not source_ids:
            source_ids = self.dbi.get_default_walls_to_scan()

        # посты в стэйжинг добавляем по мере цикла, мерджим одним заходом. поэтому сначала надо зачистить стэйдж
        self.dbi.posts_truncate_staging()

        # идем по всем стенам
        for source_id in source_ids:
            print('scanning wall of source', source_id)
            recent_posts = self.vkp.get_recent_posts(source_id, oldest_post_unix_time)

            # идем по всем постам на стене, собираем лойсы-репосты
            posts_list = list(recent_posts['post_id'])
            likes_and_reposts = self.vkp.get_likes_and_reposts(source_id, posts_list)  # TODO

            # складываем посты в базу уже после того, как успешно выгребли все лайки по ним
            # чтобы в случае падения можно было смерджить стэйж на середине и не порушить данные
            self.dbi.posts_add_to_staging(recent_posts)

            self.dbi.likes_and_reposts_to_staging(likes_and_reposts)

        # все данные успешно залиты в sql staging. мерджим их в дбо
        self.dbi.posts_merge()
        self.dbi.likes_and_reposts_merge()
        pass
