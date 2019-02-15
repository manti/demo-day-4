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
    data_file_name = 'car_dataset.json'
    index_name = 'car_index.json'
    data_source = 'car'
    result_file = open(f"{proj_dir}/{data_file_name}", "w")
    from ackore.models import Policy
    events = Policy.objects.filter(plan_id__in=["car_tp", "car_bundled", "car_comprehensive"])
    for obj in events:
        in_json = {
            "plan_id" : obj.plan_id,
            "plan_type": obj.data.get('plan_type'),
            "name": obj.data.get('name'),
            "variant": obj.data.get('variant'),
            "registration_year": obj.data.get('registration_year'),
            "pincode": obj.data.get('pincode'),
            "previous_policy_expiry_range": obj.data.get('previous_policy_expiry_range'),
            "previous_policy_expiry": obj.data.get('previous_policy_expiry'),
            "policy_start_date": obj.data.get('policy_start_date'),
            "policy_expiry_date": obj.data.get('policy_expiry_date'),
            "tenure": obj.data.get('tenure'),
            "previous_policy_expired": obj.data.get('previous_policy_expired'),
            "selected_idv": obj.data.get('selected_idv'),
            "net_premium": obj.data.get('net_premium'),
            "gst_price": obj.data.get('gst_price'),
            "gst_percentage": obj.data.get('gst_percentage'),
            "payment_id": obj.data.get('payment_id'),
            "idit_id": obj.data.get('idit_id'),
            "car_name": obj.data.get('car_name'),
            "ncb_discount": obj.data.get('ncb_discount'),
            "covers": obj.data.get('covers'),
            "premium": obj.data.get('premium'),
            "discount": obj.data.get('discount'),
            "is_claim_previous_policy": obj.data.get('is_claim_previous_policy'),
            "previous_ncb": obj.data.get('previous_ncb'),
            "invoice_date": obj.data.get('invoice_date'),
            "communication_address": obj.data.get('communication_address'),
            "city": obj.data.get('city'),
            "is_vehicle_financed": obj.data.get('is_vehicle_financed'),
            "partner_id": obj.data.get('partner_id'),
            "is_cng_lpg_kit": obj.data.get('is_cng_lpg_kit'),
            "cng_lpg_kit_cost": obj.data.get('cng_lpg_kit_cost'),
            "nominee_name": obj.data.get('nominee_name'),
            "nominee_age": obj.data.get('nominee_age'),
            "nominee_relationship": obj.data.get('nominee_relationship'),
            "appointee_name": obj.data.get('appointee_name'),
            "appointee_relationship": obj.data.get('appointee_relationship'),
            "previous_policy_number": obj.data.get('previous_policy_number'),
            "previous_insurer": obj.data.get('previous_insurer'),
            "manufacturing_year": obj.data.get('manufacturing_year'),
            "previous_policy_type": obj.data.get('previous_policy_type'),
            "pan_number": obj.data.get('pan_number'),
            "corporate_gst_number": obj.data.get('corporate_gst_number'),
            "payment_option": obj.data.get('payment_option'),
            "use_apd": obj.data.get('use_apd'),
            "is_new": obj.data.get('is_new'),
            "seller_id": obj.data.get('seller_id'),
            "pa_cover_required": obj.data.get('pa_cover_required'),
            "is_policy_holder_individual": obj.data.get('is_policy_holder_individual'),
            "dob": obj.data.get('dob'),
            "pa_cover_tenure": obj.data.get('pa_cover_tenure'),
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


