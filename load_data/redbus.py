import django
import sys
import os
import json
from datetime import datetime

from pandas.io.json import json_normalize
proj_dir = os.environ["PROJDIR"]
sys.path.append(os.path.join(proj_dir, "src", "dj"))
os.environ["DJANGO_SETTINGS_MODULE"] = "proj.settings"
django.setup()
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def fetch_and_flatten():
    index_keys = set()
    data_file_name = 'redbus_dataset.json'
    index_name = 'redbus_index.json'
    data_source = 'redbus'
    result_file = open(f"{proj_dir}/{data_file_name}", "w")
    from ackore.models import Policy
    events = Policy.objects.all(plan_id='redbus')
    for obj in events:
        in_json = {
            "id": obj.data.get('id'),
            "user_id": obj.data.get('user_id'),
            "name" : obj.data.get('name'),
            "phone" : obj.data.get('phone'),
            "city" : obj.data.get('city'),
            "email" : obj.data.get('email'),
            "driver_id" : obj.data.get('driver_id'),
            "dl_number" : obj.data.get('dl_number'),
            "vehicle_number" : obj.data.get('vehicle_number'),
            "extra" : obj.data.get('extra'),
            "cancelled" : obj.data.get('cancelled'),
            "timestamp": str(obj.created_on)[:10]+'T'+str(obj.created_on)[11:19]+'.000Z',
        }
        flat_json = flatten_json(in_json)
        out_json = {}
        for key, value in flat_json.items():
            if value != "":
                value = f"{value}"
                flat_json[key] = value
                out_json[key] = value
                index_keys.add(key)
        out_json = json.dumps(out_json)
        result_file.write(f"{out_json}\n")
    keys = list(index_keys)
    create_index(keys, data_source, index_name, data_file_name)

def create_index(keys, data_source, index_name, data_file_name):
    index = {
        "type": "index",
        "spec": {
            "dataSchema": {
                "dataSource": data_source,
                "parser": {
                    "type": "string",
                    "parseSpec": {
                        "format": "json",
                        "dimensionsSpec": {
                            "dimensions": keys
                        },
                        "timestampSpec": {
                            "column": "timestamp",
                            "format": "iso"
                        }
                    }
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "day",
                    "queryGranularity": "none",
                    "intervals": ["2015-01-01/2020-12-30"],
                    "rollup": False
                }
            },
            "ioConfig": {
                "type": "index",
                "firehose": {
                    "type": "local",
                    "baseDir": "quickstart/",
                    "filter": data_file_name
                },
                "appendToExisting": False
            },
            "tuningConfig": {
                "type": "index",
                "targetPartitionSize": 5000000,
                "maxRowsInMemory": 25000,
                "forceExtendableShardSpecs": True
            }
        }
    }
    json.dump(index, open(index_name, 'w'), indent=4)


