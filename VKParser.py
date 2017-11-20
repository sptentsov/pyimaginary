import urllib.request as ur
import time
import vk
import pwd as pwd


class VKParser:
    # ACCESS_TOKEN = pwd.ACCESS_TOKEN
    USER_TOKEN = pwd.USER_TOKEN

    def get_group_members(self, group_id):
        session = vk.Session(access_token=self.USER_TOKEN)
        vk_api = vk.API(session)

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
            data_from_vk = vk_api.execute(code=code)

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
                break
            else:
                # not all users loaded, try to load next set
                current_offset = data_from_vk['last_offset'] + 1000
                # wait to avoid ban
                time.sleep(0.3)

        return users_in_group

    def make_request(self, method, params):
        data = ur.urlopen(
            'https://api.vk.com/method/' + method + '?'
            + '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
            + '&access_token=' + self.ACCESS_TOKEN
        )
        return data.read()