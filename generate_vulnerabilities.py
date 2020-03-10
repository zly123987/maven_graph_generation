from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from utils import get_mongo_db_table
import csv
import json
from generate_has import get_latest_gav
DB_HOST = "cvetriage.cqy3hiulpjht.ap-southeast-1.rds.amazonaws.com"
DB_USER = 'cvetriage_ro'
DB_PWD = 'cvetriage_ro'
DB_NAME = 'cvetriage'
ga_file, gav_file = get_latest_gav()
gav_dict = json.load(open(gav_file,'r'))
def check_ver_in_mongo_db(vendor, lib, ver):
    try:
        vers = gav_dict[vendor+':'+lib]
        if vers:
            
            if ver in vers:
                return True
            else:
                return False
        else:
            return False
    except:
        return False
def get_vulnerability_db_info(platform):
    '''
    Get the list of vulnerabilities for every library version in scantist DB
    Tables used -
    scantist_library, scantist_library_version, scantist_libraryversionissue, scantist_securityissue
    '''
    Base = automap_base()
    # Create engine, session
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:5432/{DB_NAME}',
                          client_encoding='utf-8')
    session = Session(engine)
    # Reflect the tables
    Base.prepare(engine, reflect=True)
    # Mapped classes are now created with names by default
    # matching that of the table name.
    Library = Base.classes.scantist_library
    LibraryVersion = Base.classes.scantist_library_version
    LibraryVersionIssue = Base.classes.scantist_libraryversionissue
    SecurityIssue = Base.classes.scantist_securityissue
    # TODO: get only specific columns
    vul_query_result = (session
                              .query(LibraryVersion, Library, LibraryVersionIssue, SecurityIssue)
                              .join(Library)
                              .filter(Library.platform == platform,
                                      Library.is_valid == True,
                                      LibraryVersion.is_valid == True)
                              .outerjoin(LibraryVersionIssue).outerjoin(SecurityIssue)
                              .filter(LibraryVersionIssue.is_valid == True,
                                      SecurityIssue.is_valid == True)
                            #    .yield_per(1000)
                              .with_entities(Library.name, LibraryVersion.id,  LibraryVersion.version_number, SecurityIssue.public_id, Library.vendor))

    vulnerabilities = {}
    for row in vul_query_result:
       library_name = row[0]
       version_id = str(row[1])
       version_number = row[2]
       public_id = row[3]
       vendor = row[4]
       #get vulnerabilities associated with only those versions present in mongo db
       present = check_ver_in_mongo_db(vendor, library_name, version_number)
       if present:
           if library_name not in vulnerabilities:
               vulnerabilities[library_name] = {version_number : [public_id]}
           else:
               if version_number in vulnerabilities[library_name].keys():
                   public_ids = vulnerabilities[library_name][version_number]
                   if(public_id not in public_ids):
                       public_ids.append(public_id)
                       vulnerabilities[library_name][version_number] = public_ids
               else:
                   vulnerabilities[library_name][version_number] = [public_id]
       #else:
           #print(" not present ", vendor,  library_name, " -- ", version_number)
    return vulnerabilities

def get_vers_from_db(platform):
    '''
    Get the list of libraries in every package and their respective versions from scantist DB
    Tables used -
    scantist_library, scantist_library_version
    '''
    Base = automap_base()
    # Create engine, session
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:5432/{DB_NAME}',
                          client_encoding='utf-8')
    session = Session(engine)
    # Reflect the tables
    Base.prepare(engine, reflect=True)
    # Mapped classes are now created with names by default
    # matching that of the table name.
    Library = Base.classes.scantist_library
    LibraryVersion = Base.classes.scantist_library_version
    version_query_result = (session
                              .query(LibraryVersion, Library)
                              .join(Library)
                              .filter(Library.platform == platform,
                                      Library.is_valid == True,
                                      LibraryVersion.is_valid == True)
                            #    .yield_per(1000)
                               .with_entities(Library.name, LibraryVersion.id,  LibraryVersion.version_number, Library.vendor))
    versions  = {}
    for row in version_query_result:
        library_name = row[0]
        version_number = row[2]
        vendor = row[3]
        if not check_ver_in_mongo_db(vendor, library_name, version_number) or vendor == None:
            continue
        #If library id is already present in dictionary, append more versions to it
        if library_name not in versions:
            versions[library_name] = {version_number : vendor +':'+ library_name + ':' + version_number}
        else:
            versions[library_name][version_number] = vendor +':'+ library_name + ':' + version_number

        # if library_name == 'jackson-databind':
        #     print('jackson:', versions[library_name])
    print(" No. of versions ", version_query_result.count())
    return versions

def write_to_csv(header, rows, file_name):
   with open(file_name, 'a', newline = '') as writeFile:
       writer = csv.writer(writeFile)
       if(header):
            writer.writerow(header)
       if(rows):
            writer.writerows(rows)

def remove_duplicates(old, new):
    with open(old,'r') as in_file, open(new,'w') as out_file:
        seen = set()
        for line in in_file:
            if line in seen: continue
            seen.add(line)
            out_file.write(line)

def set_vulnerability_info(cves, version_id, vul_nodes, vul_edges):
    for cve in cves:
        if(cve is not None):
            #if(cve.startswith("CVE")):
            vul_nodes.append([cve, "Vulnerability"])
            vul_edges.append([cve, version_id, "AFFECTS"])

def get_version_id(ver_info, lib_name, version_number):
   if lib_name in ver_info.keys():
       ver_dict = ver_info[lib_name]
       if version_number in ver_dict.keys():
            return ver_dict[version_number]
   return None

def generate_vul_CSVs(ver_info, vul_info):
    vul_node_header = ["VulnerabilityId:ID(Vulnerability)", ":LABEL"]
    vul_edge_header = [":START_ID(Vulnerability)", ":END_ID(Version)", ":TYPE"]
    vul_nodes = []
    vul_edges = []
    for lib_name in vul_info:
       ver_number_dict = vul_info[lib_name]
       for version_number in ver_number_dict:
           cves = ver_number_dict[version_number]
           if(cves is not None):
               version_id = get_version_id(ver_info, lib_name, version_number)
               if version_id:
                    set_vulnerability_info(cves, version_id, vul_nodes, vul_edges)
               else:
                   print(" empty id ", version_id, " for ", lib_name, " ", version_number)

    write_to_csv(vul_node_header, vul_nodes,  "vul_nodes.csv")
    remove_duplicates("vul_nodes.csv", "vul_nodes_unique.csv")
    write_to_csv(vul_edge_header, vul_edges,  "vul_edges.csv")
    remove_duplicates("vul_edges.csv", "vul_edges_unique.csv")

def generate_vul():
    vul_info = get_vulnerability_db_info("Maven")
    ver_info = get_vers_from_db("Maven")
    generate_vul_CSVs(ver_info, vul_info)
if __name__ == "__main__":
    generate_vul()
