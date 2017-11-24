from VKParser import VKParser
from VKIntegrator import VKIntegrator
import vk
import pwd as pwd
import time

# v = VKParser()
# # print(v.get_photos(-271083408)) # 141310358 photo4128662_456239179 157268412
# # print(v.wall_post(-157268412, 1, 'photo-157268412_456239193', round(time.time()) + 30))
# exit(0)
#
# i = VKIntegrator()
# i.update_group_members()
# exit(0)

i = VKIntegrator()
# its dangerous, so commented
# alt multiprof: 271083408
# dark side: -157268412 cached album 249129913
# i.post_photos(from_group=157268412, to_group=144657300, since_dt='2017-11-29')
# i.post_photos(from_group=157268412, to_group=144657300, since_dt='2017-12-05')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-11-24')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-11-27')
i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-12-03')
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
i.update_users_groups()
exit(0)

v = VKParser()
x = v.get_group_members(group_id=10772150)
print(len(x))



session = vk.Session()
vk_api = vk.API(session)
z = vk_api.users.get(user_id=1)
print(z)


