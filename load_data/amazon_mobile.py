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
    from amazon_mobile.models import Policy, Plan, 
    from users.models import User

    events = Policy.objects.filter(creatred_on__gte='2018-09-01')
    for obj in events:
        in_json = {
            "customer_name": obj.customer_name,
            "customer_purchase_id": obj.customer_purchase_id,
            "address": obj.address,
            "city": obj.city,
            "state": obj.state,
            "sale_price": obj.sale_price,
            "status": obj.status,
            "email": obj.email,
            "purchased_on": obj.purchased_on,
            "dispatched_on": obj.dispatched_on,
            "invoiced_on": obj.invoiced_on,
            "payment_mode": obj.payment_mode,
            "imei": obj.imei,
            "delivered_on": obj.delivered_on,
            "customer_id": obj.customer_id,
            "created_on": obj.created_on,
            "updated_on": obj.updated_on,
            "cancelled_on": obj.cancelled_on,
            "item_name": obj.item_name,
            "phone_order_id": obj.phone_order_id,
            "plan_order_id": obj.plan_order_id,
            "subscription_end": obj.subscription_end,
            "subscription_start": obj.subscription_start,
            "is_policy_complete": obj.is_policy_complete,
            "document": obj.document,
            "policy_number": obj.policy_number,
            "sequence": obj.sequence,
            "plan_price": obj.plan_price,
            "insurance_cancellation_date": obj.insurance_cancellation_date,
            "insurance_delivery_date": obj.insurance_delivery_date,
            "mobile_cancellation_date": obj.mobile_cancellation_date,
            "mobile_delivery_date": obj.mobile_delivery_date,
            "is_activated_by_user": obj.is_activated_by_user,
            "is_replaceable": obj.is_replaceable,
            "is_standalone": obj.is_standalone,
            "is_updated_by_user": obj.is_updated_by_user,
            "user": obj.user.phone,
            "plan": obj.plan.plan_type,
            "pincode": obj.pincode.pincode,
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
