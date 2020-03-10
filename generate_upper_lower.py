import requests
import concurrent.futures
import csv
import json
from utils import get_mongo_db_table, sort_versions_asc
from generate_has import get_latest_gav
maven = get_mongo_db_table()
ga_file, gav_file = get_latest_gav()
gav_dict = json.load(open(gav_file,'r'))
count = 0
PORT = '8090'
def _sort(versionlist):
    session = requests.Session()
    res = session.post(f'http://localhost:{PORT}/sort', data={'versionlist': versionlist})
    res.raise_for_status()
    session.close()
    return res.content.decode()


def generate_upper_lower():    
    cache = ''
    count_in = 0
    upper_list = [[':START_ID(Version)',':END_ID(Version)',':TYPE']]
    lower_list = [[':START_ID(Version)',':END_ID(Version)',':TYPE']]
    for doc in gav_dict:
        count_in += 1
        # vendor = doc['group']
        # library = doc['artifact']
        ga = doc
        
        # if cache == ga:
        #     continue
        # cache = ga
        print(count_in, ga)
        versionlist = gav_dict[ga]
        sorted_version_list = versionlist
        
        if sorted_version_list:
            for i in range(len(sorted_version_list) - 1):
                upper_list.append([ga + ':' + sorted_version_list[i], ga + ':' + sorted_version_list[i + 1], 'UPPER'])
                lower_list.append([ga + ':' + sorted_version_list[i + 1], ga + ':' + sorted_version_list[i], 'LOWER'])

    with open("upper_2.csv", "w+", newline = '') as f:
        writer = csv.writer(f)
        writer.writerows(upper_list)
    with open("lower_2.csv", "w+", newline = '') as f:
        writer = csv.writer(f)
        writer.writerows(lower_list)
# with concurrent.futures.ProcessPoolExecutor() as executor:
    
#         executor.map(parse_doc()
if __name__ == "__main__":
    generate_upper_lower()

