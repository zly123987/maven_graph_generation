
from pymongo import MongoClient
import csv
# from version_util.models import SemanticVersion

def format_version_list(versionlist):
    if len(versionlist) == 0:
        return ''
    ret = versionlist[0]
    if len(versionlist) > 1:
        for i in range(len(versionlist) - 1):
            ret += '|' + versionlist[i + 1]
    return ret

def get_versions_in_range(lib, vers, spec):
    ver_list = []
    if(spec):
        for ver in vers:
            try:
                if spec.contains(ver):
                    ver_list.append(ver)
            except Exception as e:
                print(" contains exc ", lib, " ", ver)
                with open("contains_exc.csv", "a", newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows([[str(e), str(spec), ver]])
        return ver_list
    else:
        return vers

def sort_versions_asc(versions):
    semvers = []
    for v in versions:
        try:
            sv = Version(v)
            semvers.append(sv)
        except:
            sv = LegacyVersion(v)
            semvers.append(sv)
    sorted_semvers = sorted(semvers)
    sorted_vers = []
    for semver in sorted_semvers:
        sorted_vers.append(str(semver))
    return sorted_vers

def format_marker_list(markers):
    if not markers :
        return ''
    else:
        return str(markers)

def format_extra_list(extras):
    if not extras:
        return ''
    else:
        extras = '|'.join(extras)
        return extras

def get_mongo_db_table():
    MONGO_ADDRESS = '192.168.0.145'
    MONGO_INITDB_ROOT_USERNAME = 'root'
    MONGO_INITDB_ROOT_PASSWORD = 'example'
    MONGO_SERVER_PORT = 27018
    MONGO_AUTH_SOURCE = 'admin'
    MONGO_DB_NAME = 'library-crawler'
    mongo = MongoClient(MONGO_ADDRESS, port=int(MONGO_SERVER_PORT))
    crawl = mongo[MONGO_DB_NAME]
    maven = crawl['maven']
    return maven

def get_ver_elements(ver_str):
    ver_elements = []
    try:
        ver = Version(ver_str)
    except:
        ver = LegacyVersion(ver_str)
    if ver:
        try:
            semver = SemanticVersion(ver.base_version)
            ver_elements.append(semver.major)
            ver_elements.append(semver.minor)
            ver_elements.append(semver.patch)
            ver_elements.append(ver.pre)
            ver_elements.append(ver.post)
            ver_elements.append(str(ver.epoch))
            return ver_elements
        except:
            return []

