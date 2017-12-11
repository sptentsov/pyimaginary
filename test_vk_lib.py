from VKParser import VKParser
from VKIntegrator import VKIntegrator
import vk
import pwd as pwd
import pandas as pd


def prepare_e1_data():
    i = VKIntegrator()

    i.update_group_members(groups=[144657300])  # update imaginary members
    basic_users = i.get_interesting_users(source_vw='[ext].[e1_vw_basic_users]')  # get imaginary members
    i.update_users_groups(users=basic_users)  # get groups of imaginary members
    i.update_groups(max_groups_to_update=20000)  # get group info about received groups
    # users_groups and groups_info are updated: data for sql of ext 1 is ready
    i.e1_extend_groups()
    groups = i.e1_get_extended_groups()
    i.update_group_members(groups=groups)


pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# prepare_e1_data()
# exit(0)


# v = VKParser()
# x = v.get_likes()
# exit(0)


i = VKIntegrator()

# i.scan_walls()  # source_ids=[4128662])
# exit(0)

# i.update_users_groups()
# exit(0)

# i.update_groups(max_groups_to_update=20000)
# exit(0)

i.update_group_members()
exit(0)

# i = VKIntegrator()
# its dangerous, so commented
# alt multiprof: 271083408
# dark side: -157268412 cached album 249129913
# i.post_photos(from_group=157268412, to_group=144657300, since_dt='2017-11-29')
# i.post_photos(from_group=157268412, to_group=144657300, since_dt='2017-12-05')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-11-24')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-11-27')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-12-03')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-12-09')
# i.post_photos(source_id=-157268412, source_album=249129913, to_group=144657300, since_dt='2017-12-12')
exit(0)

session = vk.Session(access_token=pwd.USER_TOKEN)
vk_api = vk.API(session)
# z = vk_api.wall.post(owner_id=-157268412, from_group=1, message='uh oh')
z = vk_api.photos.getAll(owner_id=-157268412, count=200, photo_sizes=0)#-157268412))
print(z)
exit(0)


v = VKParser()
x = v.get_group_members(group_id=10772150)
print(len(x))



session = vk.Session()
vk_api = vk.API(session)
z = vk_api.users.get(user_id=1)
print(z)


