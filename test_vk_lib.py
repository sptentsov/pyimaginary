import requests, json
from VKParser import VKParser
from VKIntegrator import VKIntegrator
import json
import vk
import pwd as pwd

# session = vk.Session()#access_token=pwd.USER_TOKEN)  # )#
# vk_api = vk.API(session)
#
# z = vk_api.wall.getReposts(owner_id=-144657300, post_id=3886)# user_id=4128662)
# print(z)
#
# exit(0)

#
# v = VKParser()
# params = {
#         'group_id': 144657300
#         , 'token': pwd.USER_TOKEN
#     }
# data = v.make_request(method='groups.Members', params=params)
# data_str = data.decode('utf-8')
# print(data_str)
# exit(0)

i = VKIntegrator()
i.update_group_members()

exit(0)

conn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=VK;Trusted_Connection=yes;')
x = pd.read_sql('select group_id '
                'from dict.groups_to_monitor '
                'where is_scan_enabled = 1'
                , conn
                )
print(x)
exit(0)

v = VKParser()
x = v.get_group_members(group_id=10772150)
print(len(x))



session = vk.Session(access_token=pwd.USER_TOKEN)
vk_api = vk.API(session)

z = vk_api.users.get(user_id=1)
print(z)


