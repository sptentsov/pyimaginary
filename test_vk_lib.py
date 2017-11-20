from VKParser import VKParser
from VKIntegrator import VKIntegrator
import vk

users = [4128662, 15385758, 121442051, 148009753]


v = VKParser()
v.get_users_groups(users)

session = vk.Session()
vk_api = vk.API(session)
for u in users:
    z = vk_api.users.getSubscriptions(user_id=u)
    pass

exit(0)

i = VKIntegrator()
i.update_group_members()

exit(0)


v = VKParser()
x = v.get_group_members(group_id=10772150)
print(len(x))



session = vk.Session()
vk_api = vk.API(session)
z = vk_api.users.get(user_id=1)
print(z)


