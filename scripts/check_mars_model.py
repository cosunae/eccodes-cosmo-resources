import argparse
import eccodes as ec
from typing import Dict, Any
import hashlib
import json

def dict_hash(dictionary: Dict[str, Any]) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()

parser = argparse.ArgumentParser(prog="check_mars_model",
                                 description="script to check that mars data is defined and unique for all records within a file")
parser.add_argument('-f','--filename')

args = parser.parse_args()

f = open(args.filename, 'rb')

keys = ('class','stream', 'levtype', 'paramId','level')
hash_keys = {}
cnt=0
index = {}
while 1:
    gid = ec.codes_grib_new_from_file(f)
    if gid is None:
        break
    
    vals = {}
    for key in keys:
        print("LL", ec.codes_get(gid,'levtype'))
        if key == 'level':
            val = ec.codes_get_double(gid,key)
        else:
            val = ec.codes_get(gid,key)
        if val == 'unknown':
            print("ec", ec.codes_get(gid, "typeOfLevel"), ec.codes_get_long(gid, 'typeOfFirstFixedSurface'), ec.codes_get_long(gid, 'typeOfSecondFixedSurface'), ec.codes_get(gid, 'shortName'))
            raise RuntimeError("unknown key:"+key)
        
        vals[key] = val
        print('key:', ec.codes_get_double(gid, key))
        
    hash = dict_hash(vals)
    if hash in hash_keys.keys():
        raise RuntimeError("Hash already found,", vals, " for record #:", cnt, ' It was already inserted with index: ', index[hash])
    index[hash] = cnt
    hash_keys[dict_hash(vals)] = vals
    
    cnt+=1
