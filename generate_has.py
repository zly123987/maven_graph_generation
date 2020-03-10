import csv
import json
import os
import re
import requests
from utils import get_mongo_db_table
import datetime
now = str(datetime.datetime.now())
maven = get_mongo_db_table()
has_list = [[':START_ID(Library)', ':END_ID(Version)', ':TYPE']]

a = {}
b={}
gav_output = {}
PORT = 8090
def _sort(versionlist):
    session = requests.Session()
    res = session.post(f'http://localhost:{PORT}/sort', data={'versionlist': versionlist})
    res.raise_for_status()
    session.close()
    return res.content.decode()

def get_latest_gav():
    files = os.listdir('.')
    gav_jsons = {}
    ga_jsons = {}
    for file in files:
        date = re.search('maven_ga_sv_(\d.*)\.\d+\.json', file)
        if date:
            gav_jsons[int(date.group(1).replace('-','').replace(':','').replace(' ',''))]=file
        else:
            date = re.search('maven_ga_(\d.*)\.\d+\.json', file)
            if date:
                ga_jsons[int(date.group(1).replace('-','').replace(':','').replace(' ',''))]=file
    return ga_jsons[sorted(ga_jsons.keys(), reverse=True)[0]], gav_jsons[sorted(gav_jsons.keys(), reverse=True)[0]]
def get_gav_json():
    count = 0
    for doc in maven.find():
        count+=1
        print(count)
        library = doc['artifact']
        vendor = doc['group']
        version = doc['version']
        ga = vendor +':'+ library
        ga_ = vendor +'!@#$%'+ library
        b[ga_]=''
        if ga not in a.keys():
            a[ga]=[version]
        else:
            a[ga].append(version)
    count = 0

    for key in a:
        count+=1
        print(count)
        gav_output[key] = _sort('|'.join(a[key])).replace('\"','').split('|')

    json.dump(gav_output, open('maven_ga_sv_'+now+'.json','w'))
    json.dump(b, open('maven_ga_'+now+'.json','w+'))

def generate_has():
    count = 0
    ga_file, gav_file = get_latest_gav()
    gav_dict = json.load(open(gav_file,'r'))
    for doc in gav_dict:
        ga = doc
        versionlist = gav_dict[ga]

        id = ga
        for ver in versionlist:
            verid = ga + ':' + ver
            has_list.append([id, verid, 'HAS'])
        print(count, ga, len(has_list))
        count += 1
    with open("has.csv", "w", newline = '') as f:
        writer = csv.writer(f)
        writer.writerows(has_list)
if __name__ == "__main__":
    get_gav_json()
    #generate_has()
