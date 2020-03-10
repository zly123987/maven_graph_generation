import os
import csv
import concurrent.futures
import requests
import logging
import json
import datetime
import pandas as pd
from utils import (
    format_version_list,
    get_versions_in_range,
    sort_versions_asc,
    format_marker_list,
    format_extra_list,
    get_mongo_db_table
)
from generate_has import get_latest_gav
PORT = '8090'
c=0
ga_file, gav_file = get_latest_gav()
gav_dict = json.load(open(gav_file,'r'))
gas = gav_dict.keys()



def read_csv(dep_file):
    df = pd.read_csv(dep_file, dtype=str)

    # Remove duplicates
    df['dScope'] = df['dScope'].astype(str).str.lower()
    df_part = df.query('"test" not in dScope').copy(deep=True)
    df_part['order'] = df_part.reset_index().index+1
    # df = df[['group','artifact','version','dG','dA','dV', 'dScope', 'isoptional']]
    df_part.drop_duplicates(inplace=True)


    return df_part


def _sort(versionlist, port):
    session = requests.Session()
    res = session.post(f'http://localhost:{PORT}/sort', data={'versionlist': versionlist})
    res.raise_for_status()
    session.close()
    return res.content.decode()

def check_sort(gav, ga_d, versions_in_range):
    port = 8090
    for i in range(1):
        try:
            a = _sort('|'.join(versions_in_range), str(port+i)).replace('\"','').split('|')
            return a
        except:
            
            f = open('errors','a')
            f.write(f'{i},{gav},{ga_d}, {versions_in_range}')
            f.close()
    return False


def check(ga_d, version, vrange):
    port = 8090
    for i in range(1):
        try:
            a = check_in_range(version, vrange, str(port+i))
            return a
        except:
            
            f = open('errors','a')
            f.write(f'{i},{ga_d},{version},{vrange}')
            f.close()
    return False
def check_in_range(version, range, port):       
    session = requests.Session()
    res = session.get(f'http://localhost:{port}/checkinrange?version={version}&range={range}'\
        .replace(' ', '%20').replace('[', '%5B').replace(']', '%5D'))
    res.raise_for_status()
    session.close()
    if res.content.decode() == 'true':
        return True
    else:
        return False

def generate_csvs(csv_file):
    def parse_ver_range(version_d):
        # See if dep version is a range or a version
        if not ga_d in gas:
            return None
        versions_d_from_dict = gav_dict[ga_d]
        if ',' in version_d:
            versions_in_range = []
            for ver in versions_d_from_dict:
                if check(ga_d, ver, version_d):
                    versions_in_range.append(ver)
            versions_in_range = check_sort(gav, ga_d, versions_in_range)    
        else:
            versions_in_range = [version_d] if version_d in versions_d_from_dict else []

        if versions_in_range:
            return  versions_in_range 
        return None
    global c
    c+=1
    
    print(c, csv_file)
    input_ = read_csv(csv_file)
    depend_list = []
    default_list = []
    libdepends_list = []

    for i, line in input_.iterrows():
        vendor = line['group']
        library = line['artifact']
        version = line['version']
        vendor_d = line['dG']
        library_d = line['dA']
        version_d = line['dV']
        scope = line['dScope']
        is_optional = line['isoptional']
        d_type = line['dType']
        order = line['order']
        ga = f'{vendor}:{library}'
        gav = f'{vendor}:{library}:{version}'
        ga_d = f'{vendor_d}:{library_d}'
        # gav_d = f'{vendor_d}:{library_d}:{version_d}'
        if ga in gas:
            libid = ga_d
            verid = gav
            libmasterid = ga
            if [libmasterid, libid, 'LIBDEPENDS'] not in libdepends_list:
                libdepends_list.append([libmasterid, libid, 'LIBDEPENDS'])
            sorted_versions = parse_ver_range(version_d)

            if sorted_versions:
                depend_list.append([verid, libid, version_d, '|'.join(sorted_versions), scope, is_optional, d_type, order, 'DEPENDS'])
                versionid = f'{ga_d}:{sorted_versions[-1]}'
                default_list.append([verid, versionid, 'DEFAULT'])
                


    
    print(depend_list)
    with open('depends_debug.csv', "a", newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(depend_list)
    with open('default_debug.csv', "a", newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(default_list)
    with open("libdepends_debug.csv",  "a", newline='') as f:   
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        # libdepends_list = list(set(libdepends_list))
        writer.writerows(libdepends_list)

def generate_dependencies(): 
    with open('depends_debug.csv', "w", newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow([':START_ID(Version)', ':END_ID(Library)', 'version_range', 'versions', 'dScope', 'isoptional', 'dType', 'order', ':TYPE'])
    with open('default_debug.csv', "w", newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow([':START_ID(Version)', ':END_ID(Version)', ':TYPE'])
    with open("libdepends_debug.csv",  "w", newline='') as f:   
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow([':START_ID(Library)', ':END_ID(Library)', ':TYPE'])

    generate_csvs(file_list[0])

c=0

if __name__ == "__main__":
    dir1 = 'csv_folder/'
    file_list = ['com.amazonaws_aws-java-sdk_1.2.9.csv'] #'com.nimbusds_oauth2-oidc-sdk_2.0.csv']#os.listdir(dir1)
    prefix = lambda x:y+x

    y=dir1
    file_list = list(map(prefix, file_list))
    print(len(file_list))
    generate_dependencies()
