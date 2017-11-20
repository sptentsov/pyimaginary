from VKParser import VKParser
import json
import time

v = VKParser()

with open('20171013_LC_MP_no_I_200k.txt') as f:
    content = f.readlines()
user_list = [int(x.strip()) for x in content]

i = 1
for user_id in user_list:
    if i % 5 == 0:
        print (i)
    time.sleep(0.1)
    params = {
        'owner_id': user_id
        , 'count': 100
        , 'filter': 'owner'
        , 'extended': 1
    }
    data = v.make_request(method='wall.get', params=params)
    data_str = data.decode('utf-8')
    with open('20171013_walls' + str(int(i/1000)) + '.txt', 'a', encoding='utf-8') as f:
        f.write(data_str + '\n')
    i = i+1

# # data_str = json.loads(data.decode('utf-8'))
# w = data['response']['wall']
#
#
# print(w[1])
print('fin')
