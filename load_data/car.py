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
            "plan_type": obj.plan_type,
            "name": obj.name,
            "variant": obj.variant,
            "registration_year": obj.registration_year,
            "pincode": obj.pincode,
            "previous_policy_expiry_range": obj.previous_policy_expiry_range,
            "previous_policy_expiry": obj.previous_policy_expiry,
            "policy_start_date": obj.policy_start_date,
            "policy_expiry_date": obj.policy_expiry_date,
            "tenure": obj.tenure,
            "previous_policy_expired": obj.previous_policy_expired,
            "selected_idv": obj.selected_idv,
            "net_premium": obj.net_premium,
            "gst_price": obj.gst_price,
            "gst_percentage": obj.gst_percentage,
            "payment_id": obj.payment_id,
            "idit_id": obj.idit_id,
            "car_name": obj.car_name,
            "ncb_discount": obj.ncb_discount,
            "covers": obj.covers,
            "premium": obj.premium,
            "discount": obj.discount,
            "is_claim_previous_policy": obj.is_claim_previous_policy,
            "previous_ncb": obj.previous_ncb,
            "invoice_date": obj.invoice_date,
            "communication_address": obj.communication_address,
            "city": obj.city,
            "is_vehicle_financed": obj.is_vehicle_financed,
            "partner_id": obj.partner_id,
            "is_cng_lpg_kit": obj.is_cng_lpg_kit,
            "cng_lpg_kit_cost": obj.cng_lpg_kit_cost,
            "nominee_name": obj.nominee_name,
            "nominee_age": obj.nominee_age,
            "nominee_relationship": obj.nominee_relationship,
            "appointee_name": obj.appointee_name,
            "appointee_relationship": obj.appointee_relationship,
            "previous_policy_number": obj.previous_policy_number,
            "previous_insurer": obj.previous_insurer,
            "manufacturing_year": obj.manufacturing_year,
            "previous_policy_type": obj.previous_policy_type,
            "pan_number": obj.pan_number,
            "corporate_gst_number": obj.corporate_gst_number,
            "payment_option": obj.payment_option,
            "use_apd": obj.use_apd,
            "is_new": obj.is_new,
            "seller_id": obj.seller_id,
            "pa_cover_required": obj.pa_cover_required,
            "is_policy_holder_individual": obj.is_policy_holder_individual,
            "dob": obj.dob,
            "pa_cover_tenure": obj.pa_cover_tenure,
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


