import urllib.request as ur
import pandas as pd
import time
import vk
import pwd as pwd
from requests.exceptions import ReadTimeout


class VKParser:
    # ACCESS_TOKEN = pwd.ACCESS_TOKEN
    def __init__(self):
        session = vk.Session(access_token=pwd.USER_TOKEN)
        self.vk_api = vk.API(session)

    def get_recent_posts(self, source_id):
        self.vk_api.wall.get(owner_id=source_id, count=100)

    def get_likes(self):
        w = self.vk_api.wall.get(type='post', owner_id=-144657300, count=100)
        a = self.vk_api.likes.getList(type='post', owner_id=-144657300, item_id=4539, count=1000, filter='likes')
        b = self.vk_api.likes.getList(type='post', owner_id=-144657300, item_id=4539, count=1000, filter='copies')
        print(a,b)

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

            data_from_vk += api_call_result[1:len(api_call_result)]  # первый элемент - количество фоток в источнике

            print(current_offset, offset_step, api_call_result[0], len(api_call_result))
            if current_offset + offset_step > api_call_result[0]:
                # уже вычитали всё. Проверяем, что фоток действительно столько
                if len(data_from_vk) != api_call_result[0]:
                    raise ValueError(
                        'Actual count of photos', len(data_from_vk)
                        , 'doesnt match count from vk:', api_call_result[0]
                    )
                break
            else:
                current_offset = current_offset + offset_step
                time.sleep(0.3)

        photos = [
            str(photo['owner_id']) + '_' + str(photo['pid'])
            for photo in data_from_vk
            if photo['aid'] == source_album
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
                    members = members + resp.users;
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
                        g['gid'], g['name'], g['screen_name']
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