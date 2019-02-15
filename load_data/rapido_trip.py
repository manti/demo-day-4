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
    data_file_name = 'rapido_trip_dataset.json'
    index_name = 'rapido_trip_index.json'
    data_source = 'rapido_trip'
    result_file = open(f"{proj_dir}/{data_file_name}", "w")
    from ackore.models import Policy
    events = Policy.objects.all(plan_id='rapido_trip')
    for obj in events:
        in_json = {
            "user_id": obj.data.get('user_id'),
            "name": obj.data.get('name'),
            "phone": obj.data.get('phone'),
            "ride_type": obj.data.get('ride_type'),
            "trip_id": obj.data.get('trip_id'),
            "booked_on": obj.data.get('booked_on'),
            "city": obj.data.get('city'),
            "expected_trip_start_time": obj.data.get('expected_trip_start_time'),
            "email": obj.data.get('email'),
            "passenger_id": obj.data.get('passenger_id'),
            "vehicle_type": obj.data.get('vehicle_type'),
            "extra": obj.data.get('extra'),
            "pickup": obj.data.get('pickup'),
            "drop": obj.data.get('drop'),
            "cancelled": obj.data.get('cancelled'),
            "endorsements": obj.data.get('endorsements'),
            "addons": obj.data.get('addons'),
            "premium": obj.data.get('premium'),
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
    keys = list(keys)
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


