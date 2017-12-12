import urllib.request as ur
import pandas as pd
import numpy as np
import time
import vk
import pwd as pwd
from datetime import datetime
from requests.exceptions import ReadTimeout


class VKParser:
    # ACCESS_TOKEN = pwd.ACCESS_TOKEN
    def __init__(self):
        session = vk.Session(access_token=pwd.USER_TOKEN)
        self.vk_api = vk.API(session, v='5.69')

    def get_recent_posts(self, source_id, oldest_post_unix_time):
        current_offset = 0
        offset_step = 100
        data_from_vk = []

        while True:
            api_call_result = self.vk_api.wall.get(
                owner_id=source_id
                , count=100
                , offset=current_offset
            )
            time.sleep(0.3)  # выжидаем, чтобы со следующего запроса не побаниться

            data_from_vk += api_call_result['items']

            if not data_from_vk:
                print('received an empty list, stop scanning')
                break

            if len(data_from_vk) == api_call_result['count']:
                print('reached the oldest entry in the group, stop scanning')
                break

            if data_from_vk[-1]['date'] < oldest_post_unix_time:
                print('reached post with date', data_from_vk[-1]['date'], 'its older than needed')
                break

            print('not all recent posts got, query again')
            current_offset = current_offset + offset_step

        cleaned_data = [
            {
                'post_id': post['id']
                , 'comments': post['comments']['count']
                , 'likes': post['likes']['count']
                , 'reposts': post['reposts']['count']
                , 'views': post.get('views', {}).get('count', -1)  # для старых записей просмотры не определены
                , 'repost_from_source_id_signed': post.get('copy_history', [{}])[0].get('owner_id', 0)
                , 'repost_from_post_id': post.get('copy_history', [{}])[0].get('id', 0)
                , 'posted_dt_precise': datetime.fromtimestamp(post['date'])
            }
            for post in data_from_vk
            if post['date'] > oldest_post_unix_time
        ]

        df = pd.DataFrame(
            cleaned_data
            , columns=[
                'post_id', 'comments', 'likes'
                , 'reposts', 'views'
                , 'repost_from_source_id_signed', 'repost_from_post_id', 'posted_dt_precise'
            ]
        )

        df.insert(loc=0, column='is_posted_by_user', value=1 if source_id > 0 else 0)
        df.insert(loc=0, column='source_id', value=abs(source_id))

        return df

    def get_likes_and_reposts(self, source_id, posts_list):

        # вспомогательная функция, которая генерит код для VK execute
        # posts_block[::-1] инвертируем лист, ибо потом юзаем pop с конца листа, а оффсет задан для перовго элемента
        # внутри ВК скрипта цикл по постам, в нем цикл по оффсетам количества лойсов/репостов
        def vk_execute_code(current_offset, posts_block, execute_block_size, _source_id, is_like):
            return '''
                    var offset = ''' + str(current_offset) + ''';
                    var api_call_counter = 0;
                    var posts = [''' + ','.join(str(u) for u in posts_block[::-1]) + '''];
                    var post = 0;
                    var result = [];
                    var resp = {};
                    var cnt_likes = 0;
                    var offset_step = 1000;
                    var is_first_post = true;

                    while((posts.length > 0) && (api_call_counter < ''' + str(execute_block_size) + '''))
                    {
                        post = posts.pop();
                        if (!is_first_post)
                            offset = 0;
                        else
                            is_first_post = false;
                        
                        do
                        {
                            resp = API.likes.getList({
                                "type": "post"
                                , "owner_id": ''' + str(_source_id) + '''
                                , "item_id": post
                                , "offset": offset
                                , "count": offset_step
                                , "filter": "''' + ('likes' if is_like else 'copies') + '''"});
                            api_call_counter = api_call_counter + 1;
                            
                            resp = resp + {"post_id": post, "offset": offset};
                            result.push(resp);
                            
                            cnt_likes = resp["count"];
                            offset = offset + offset_step;
                        }
                        while((offset < cnt_likes) && (api_call_counter < ''' + str(execute_block_size) + '''))
                    }
                    
                    if (offset < cnt_likes)
                        result.push({"non_finished_offset": offset, "non_finished_post": post});
                    else
                    {  
                        if (posts.length == 0)
                            result.push({"non_finished_offset": -1, "non_finished_post": -1});
                        else
                            result.push({"non_finished_offset": 0, "non_finished_post": posts.pop()});
                    }
                    return result;
                '''

        def wall_posts_loop(_source_id, _posts_list, is_like):
            execute_block_size = 25  # количество постов за один execute. Сейчас он позволяет 25 запросовя

            current_post_index = 0
            current_offset = 0
            result = pd.DataFrame()

            # генерим код на экзекьют 25 постов (больше все равно не влезет)
            # но может влезть меньше, если лайков много. поэтому пилим цикл в экзекьюте, возвращаем последнее опрошенное
            while current_post_index < len(_posts_list):
                print(
                    'loading', 'likes' if is_like else 'reposts', 'from VK API.'
                    , 'processed', current_post_index, 'posts of', len(_posts_list)
                )
                posts_block = _posts_list[current_post_index:current_post_index + execute_block_size]

                code = vk_execute_code(current_offset, posts_block, execute_block_size, _source_id, is_like)
                data_from_vk = self.vk_api.execute(code=code)
                time.sleep(0.3)

                for d in data_from_vk:
                    # data_from_vk - массив словарей. В них либо инфа по лайкам/репостам, либо инфа на чем остановились
                    if 'post_id' in d.keys():
                        # если тут инфа по лайкам/репостам, то дописываем её
                        post_data = {
                            'is_like': 1 if is_like else 0
                            , 'actor_id': [abs(i) for i in d['items']]
                            , 'is_actor_a_user': [1 if i > 0 else 0 for i in d['items']]
                            , 'target_id': abs(_source_id)
                            , 'target_post_id': d['post_id']
                            , 'is_target_a_user': 1 if _source_id > 0 else 0
                        }
                        result = result.append(
                            pd.DataFrame(
                                post_data
                                , columns=[
                                    'is_like', 'actor_id', 'is_actor_a_user'
                                    , 'target_id', 'target_post_id', 'is_target_a_user'
                                ]
                            )
                        )
                    else:
                        non_finished_post = d['non_finished_post']
                        non_finished_offset = d['non_finished_offset']

                result.drop_duplicates(inplace=True)
                print('  got', result.shape[0], 'likes' if is_like else 'reposts', 'in block')
                # TODO validate actual likes with count field from VK

                if non_finished_post == -1:
                    current_post_index = current_post_index + execute_block_size
                    current_offset = 0
                    # print('  block finished, go to next block')
                else:
                    current_post_index = _posts_list.index(non_finished_post)
                    current_offset = non_finished_offset
                    # print('  block is not finished, continue with post_index =', current_post_index, 'offset',
                    #       current_offset)

                if current_post_index >= len(_posts_list):
                    break

            return result

        # b = self.vk_api.likes.getList(type='post', owner_id=-144657300, item_id=4539, count=1000, filter='copies')

        likes_and_reposts = pd.DataFrame()

        # грузим репосты
        likes_and_reposts = likes_and_reposts.append(wall_posts_loop(source_id, posts_list, is_like=False))

        # грузим лайки
        likes_and_reposts = likes_and_reposts.append(wall_posts_loop(source_id, posts_list, is_like=True))

        return likes_and_reposts

    def get_photos(self, source_id, source_album):
        # returns array of photos in group: ['-157268412_456239021', '-157268412_456239020', '-157268412_456239019']
        current_offset = 0
        offset_step = 200
        data_from_vk = []

        while True:
            api_call_result = self.vk_api.photos.getAll(
                owner_id=source_id
                , count=offset_step
                , photo_sizes=0
                , offset=current_offset
            )

            data_from_vk += api_call_result['items']

            print(current_offset, offset_step, api_call_result['count'], len(api_call_result))
            if current_offset + offset_step > api_call_result['count']:
                # уже вычитали всё. Проверяем, что фоток действительно столько
                if len(data_from_vk) != api_call_result['count']:
                    raise ValueError(
                        'Actual count of photos', len(data_from_vk)
                        , 'doesnt match count from vk:', api_call_result['count']
                    )
                break
            else:
                current_offset = current_offset + offset_step
                time.sleep(0.3)

        photos = [
            str(photo['owner_id']) + '_' + str(photo['id'])
            for photo in data_from_vk
            if photo['album_id'] == source_album
        ]

        return photos

    def wall_post(self, owner_id, from_group, attachments, publish_date):
        self.vk_api.wall.post(
            owner_id=owner_id
            , from_group=from_group  # от имени группы
            , attachments=attachments
            , publish_date=publish_date
        )

    def get_group_members(self, group_id):
        current_offset = 0
        users_in_group = []
        debug = {'offsets': [], 'counts': [], 'amounts': []}

        while True:
            code = '''
                var flag = 0;
                var members = [];
                var offset = ''' + str(current_offset) + ''';
                var count = 0;
                while(flag == 0)
                {
                    var resp = API.groups.getMembers({"group_id": ''' + str(group_id) + ''', "offset": offset, "count": "1000"});
                    members = members + resp.items;
                    offset = offset + 1000;
                    if ((offset > resp.count) || (offset > ''' + str(current_offset) + ''' + 24000)) 
                    {
                        count = resp.count;
                        flag = 1;
                    }
                }
                return {"users": members, "count": count, "last_offset": offset-1000};
            '''

            for retry in range(3):
                try:
                    data_from_vk = self.vk_api.execute(code=code)
                    break
                except ReadTimeout:
                    print('Read timeout from VK, retry in 3 sec..')
                    time.sleep(3)

            print('scanning group id:', group_id
                  , 'total count told by vk:', data_from_vk['count']
                  , 'last offset:', data_from_vk['last_offset']
                  , 'vk returned', data_from_vk['users'].__len__(), 'users in this batch'
                  )

            users_in_group += data_from_vk['users']

            debug['counts'].append(data_from_vk['count'])
            debug['offsets'].append(current_offset)
            debug['amounts'].append(len(set(data_from_vk['users'])))

            if data_from_vk['last_offset'] + 1000 > data_from_vk['count']:
                # we got all users. they should be unique and the quantity must be right
                if len(set(users_in_group)) != data_from_vk['count']:
                    raise ValueError(
                        'VK says it should be'
                        , data_from_vk['count']
                        , 'users in group, but we got'
                        , len(set(users_in_group))
                        , 'unique users. Debug info:'
                        , str(debug)
                    )
                time.sleep(0.3)
                break
            else:
                # not all users loaded, try to load next set
                current_offset = data_from_vk['last_offset'] + 1000
                # wait to avoid ban
                time.sleep(0.3)

        return users_in_group

    def get_users_groups(self, users):
        # на вход принимет список юзеров в виде листа интов
        # на выходе дает dataframe со следующими колонками
        # 'user_id' - id юзера из списка на входе
        # 'is_group' - 1, если то, на что он подписан - группа, 0 если личная страница
        # 'subscribed_on' - id того, на что он подписан (группы или страницы)

        users_block_size = 25  # количество юзеров, по которым тащим данные за один execute. Сейчас он позволяет 25 запросов
        result = pd.DataFrame()

        # берем по пачке юзеров размером users_block_size, и натравливаем на пачку execute
        for b in range(0, len(users), users_block_size):
            print('loading users groups from VK API. processed', b, 'users of', len(users))
            users_block = users[b:b + users_block_size]

            # выгужаем из ВК данные для блока юзеров
            code = '''
                    var flag = 0;
                    var users = [''' + ' ,'.join(str(u) for u in users_block) + '''];
                    var user = 0;
                    var result = [];
                    
                    while(users.length)
                    {
                        user = users.pop();
                        var resp = API.users.getSubscriptions({"user_id": user});
                        resp = resp + {"user_id": user};
                        result.push(resp);
                    }
                    return result;
                '''
            data_from_vk = self.vk_api.execute(code=code)

            # пакуем данные для этого блока юзеров в датафрейм
            for user in data_from_vk:
                if not isinstance(user, dict):
                    continue
                if 'groups' in user:
                    result = result.append(pd.DataFrame(
                        {
                            'user_id': user['user_id']
                            , 'is_group': 1
                            , 'subscribed_on': user['groups']['items']}
                        , columns=['user_id', 'is_group', 'subscribed_on']
                    ))
                if 'users' in user:
                    result = result.append(pd.DataFrame(
                        {
                            'user_id': user['user_id']
                            , 'is_group': 0
                            , 'subscribed_on': user['users']['items']}
                        , columns=['user_id', 'is_group', 'subscribed_on']
                    ))

            # wait to avoid ban
            time.sleep(0.3)

        return result

    def get_groups_info(self, groups):
        groups_block_size = 500  # столько за один вызов метода можно вытащить групп

        result = pd.DataFrame()

        for b in range(0, len(groups), groups_block_size):
            print('loading groups info from VK API. processed', b, 'groups of', len(groups))
            groups_block = groups[b:b + groups_block_size]
            data_from_vk = self.vk_api.groups.getById(
                group_ids=', '.join(str(g) for g in groups_block)
                , fields='members_count'
            )

            for g in data_from_vk:
                result = result.append(pd.DataFrame(
                    [[
                        g['id'], g['name'], g['screen_name']
                        , g.get('members_count', 0)  # banned groups have no count
                    ]]
                    , columns=['group_id', 'name', 'screen_name', 'members_count']
                ))
        return result

    def make_request(self, method, params):
        data = ur.urlopen(
            'https://api.vk.com/method/' + method + '?'
            + '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
            + '&access_token=' + self.ACCESS_TOKEN
        )
        return data.read()