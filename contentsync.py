from datetime import timedelta, datetime, date
from requests.auth import HTTPBasicAuth
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
import csv, json

ES_HOSTS = ['esm1',
'esm2',
'esm',
'esd1',
'esd2',
'esd3',
'esd4',
'esd5'
]

ES_HTTP_AUTH = 'token'
ES_REQUEST_TIMEOUT = 120
ES_INDEX_TRACKS_PATTERN = "sync-logs-*"
ELASTIC_SEARCH = Elasticsearch(hosts=ES_HOSTS,
                               http_auth=ES_HTTP_AUTH,
                               timeout=ES_REQUEST_TIMEOUT)

jsonfile = open(f"./ibs.json",)
airline = json.load(jsonfile)
alldatas = []
dates = []
col_filter = ["@timestamp", "airline", "airplane", "content_name", "sync_status", "content_id", "channel_name", "content_type", "file_size", "download_percentage"]

def timeformatter(string):
    datedata = string
    datedata = datedata.strip("Z").split("T")
    historical = datedata[0].split("-")
    temporal = datedata[1].split(":")
    temporal.pop()
    date = datetime(int(historical[0]), int(historical[1]), int(historical[2]), int(temporal[0]), int(temporal[1]))
    return date

for aircraft in airline["ibs"]:
    query_body = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "minimum_should_match": 1,
                            "should": [
                            {
                                "match_phrase": {
                                "airplane.keyword": aircraft
                                }
                            }
                            ]
                        }
                    },
                    {
                    "range": {
                        "@timestamp": {
                        "gte": "now-24d",
                        "lt": "now",
                        }
                    }
                    }
                ]
            }
        },
        "aggs": {
            "airplane": {
                "terms": {
                    "field":"content_id.keyword",
                    "size": 5000
                },
                "aggs": {
                    "top_group_hits": {
                    "top_hits": {
                        "sort": [
                        {
                            "@timestamp": {
                            "order": "desc"
                            }
                        }
                        ],
                        "size": 1
                        }
                    }
                }
            }
        }
    }

    result = ELASTIC_SEARCH.search(index=ES_INDEX_TRACKS_PATTERN, body=query_body, size=10000)
    contentbuckets = result["aggregations"]["airplane"]["buckets"]
    for eachbucket in contentbuckets:
        hitscontent = eachbucket["top_group_hits"]["hits"]["hits"][0]["_source"]
        for field in col_filter:
            if(hitscontent.get(f'{field}') == None):
                hitscontent.update({f'{field}':'-'})
        alldatas.append(hitscontent)
    datecheck = None
    for tailid in alldatas:
        if (tailid["airplane"] == aircraft):
            formeddate = timeformatter(tailid["@timestamp"])
            if (datecheck == None):
                datecheck = {
                    "info":[
                        formeddate, tailid["airplane"]
                    ]
                }
            else:
                if (formeddate > datecheck['info'][0]):
                    datecheck = {
                        "info":[
                            formeddate, tailid["airplane"]
                        ]
                    }
    dates.append(datecheck)

filename = f'./results/ContentSync-{date.today().strftime("%d-%b-%Y")}.csv'
with open(filename, 'w') as f:
    wr = csv.DictWriter(f, fieldnames=col_filter)
    wr.writeheader()
    ac = 0
    for v in alldatas:
        formeddate2 = timeformatter(v["@timestamp"])
        if (dates[ac] == None):
            ac = ac + 1
            continue
        if (v["airplane"] != dates[ac]["info"][1]):
            ac = ac + 1
        elif (formeddate2 == dates[ac]["info"][0]):
            dict_you_want = { your_key: v[your_key] for your_key in col_filter }
            wr.writerow(dict_you_want)
