from VKParser import VKParser
from VKIntegrator import VKIntegrator
import vk
import pwd as pwd


i = VKIntegrator()
# i.post_photos(from_group=157268412, to_group=144657300, since_dt='2017-11-29')
i.post_photos(from_group=157268412, to_group=144657300, since_dt='2017-12-05')
exit(0)

session = vk.Session(access_token=pwd.USER_TOKEN)
vk_api = vk.API(session)
# z = vk_api.wall.post(owner_id=-157268412, from_group=1, message='uh oh')
z = vk_api.photos.getAll(owner_id=-157268412, count=200, photo_sizes=0)#-157268412))
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


