import requests, json
from VKParser import VKParser
from VKIntegrator import VKIntegrator
import json
import vk


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

# в браузере
# https://oauth.vk.com/authorize?client_id=app6218276&scope=wall,offline&redirect_uri=https://oauth.vk.com/blank.html&response_type=token
# и потом из адресной строки берем токен

session = vk.Session(access_token='ba21f72eb0ad1598668a7dc245112b6e241f7e2022f99d852717c0fbd75697fe23a3c83bdc5db177450c0')
vk_api = vk.API(session)

z = vk_api.users.get(user_id=1)
print(z)


