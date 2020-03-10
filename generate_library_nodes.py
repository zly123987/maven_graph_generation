import csv
import json
import requests
from utils import (
    format_version_list,
    get_versions_in_range,
    sort_versions_asc,
    format_marker_list,
    format_extra_list,
    get_ver_elements,
    get_mongo_db_table,
    get_ver_elements

)
from generate_has import get_latest_gav
PORT = '8090'
session = requests.Session()
def _sort(versionlist):
    res = session.post(f'http://localhost:{PORT}/sort', data={'versionlist': versionlist})
    res.raise_for_status()
    session.close()
    return res.content.decode()

def get_qualifier(version):
    res = session.get(f'http://localhost:{PORT}/get_qualifier?version='+str(version))
    res.raise_for_status()
    session.close()
    return res.content.decode()


def generate_nodes_versions():
    ga_file, gav_file = get_latest_gav()
    gav_dict = json.load(open(gav_file,'r'))
    ga_dict = json.load(open(ga_file,'r'))
    library_csv_doc = [['libraryId:ID(Library)', 'vendor', 'library', ':LABEL']]
    lib_ver_doc = [['version', 'vendor', 'library', 'versionId:ID(Version)', 'qualifier', ':LABEL'
    ]]
    c =0
    # print(len(maven))
    for doc in ga_dict:
        # vendor = doc['group']
        # library = doc['artifact']
        # ga = f'{vendor}:{library}'
        vendor,library = doc.split('!@#$%')
        ga = f'{vendor}:{library}'

        print(len(library_csv_doc), len(lib_ver_doc), ga)
        # vendor, library = ga.split(':')
        versionlist = gav_dict[ga]

        c+=1
        id = ga
        library_csv_doc.append([id, vendor, library, 'Library'])
        for ver in versionlist:
            version = ver
            
            versionid = f'{ga}:{version}'
            try:
                qualifier = get_qualifier(version)
            except:
                qualifier = 'UNKNOWN'
            lib_ver_doc.append([version, vendor, library, versionid, qualifier, 'Version'])
    with open("library_nodes.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(library_csv_doc)
    with open("library_versions.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(lib_ver_doc)
if __name__ == "__main__":
    generate_nodes_versions()