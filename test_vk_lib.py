from VKParser import VKParser
from VKIntegrator import VKIntegrator
import vk
import pwd as pwd


session = vk.Session(access_token=pwd.USER_TOKEN)
vk_api = vk.API(session)
z = vk_api.wall.post(owner_id=-157268412, from_group=1, message='uh')
print(z)
exit(0)

i = VKIntegrator()
i.update_groups()
exit(0)

i = VKIntegrator()
i.update_group_members()
exit(0)

i = VKIntegrator()
i.update_users_groups()
exit(0)

v = VKParser()
x = v.get_group_members(group_id=10772150)
print(len(x))



session = vk.Session()
vk_api = vk.API(session)
z = vk_api.users.get(user_id=1)
print(z)


