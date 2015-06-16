#!/usr/bin/env python
__author__ = 'Alex Moskalenko'

import json
import urllib2
import sys

es_url = "http://localhost:9200"
# https://www.elastic.co/guide/en/elasticsearch/guide/current/_cluster_health.html
cluster_path = "/_cluster/health?level=indices"
# https://www.elastic.co/guide/en/elasticsearch/guide/current/_monitoring_individual_nodes.html
node_path = "/_nodes/_local/stats"
discovery_prefix = "{#INDEX}"

try:
    query_type = sys.argv[1]
except IndexError:
    print "ZBX_NOTSUPPORTED"
    sys.exit(3)


try:
    query = sys.argv[2].split(".")
except IndexError:
    query = None


def get_es_json(url):
    try:
        return urllib2.urlopen(url, timeout=3).read()
    except:
        return None


def get_data_from_json(url, query_type):
    data = json.loads(get_es_json(url))
    if data is None:
        sys.exit(3)

    if query_type == "node" or query_type == "node_keys":
        data = data["nodes"].itervalues().next()

    return data


# Recursively going through output of JSON and printing result with paths
def get_all_keys(data, path="", beforepath=""):
    for key, item in data.iteritems():
        beforepath = path
        if not path == "":
            path = path + "." + key
        else:
            path = key

        if type(item) is dict:
            get_all_keys(item, path=path, beforepath=beforepath)
        elif type(item) is list and len(item) == 1:
            if type(item[0]) is dict:
                get_all_keys(item[0], path=path, beforepath=beforepath)
        elif type(item) is list:
            print path + ":" + ', '.join(str(p) for p in item)
        else:
            print path + ":" + str(item)

        path = beforepath

# Zabbix works best with decimal values so we need to convert API responses
def convert_value_to_decimal(value):
    return {
        "true": "0",
        "false": "1",
        "green": "0",
        "yellow": "1",
        "red": "2"
        }.get(str(value).lower(), value)


def get_es_value(data, query):
    position = 0
    tmp_data = data
    try:
        for item in query:
            if type(tmp_data) is dict:
                tmp_data = tmp_data[item]
            elif type(tmp_data) is list and len(tmp_data) == 1:
                tmp_data = tmp_data[0][item]
            else:
                # Without this check script can produce funny results
                if position < len(query):
                    return "ZBX_NOTSUPPORTED"
                break
            position += 1
    except TypeError:
        return "ZBX_NOTSUPPORTED"
    except KeyError:
        return "ZBX_NOTSUPPORTED"

    # Some keys have multiple values in result
    if type(tmp_data) is list:
        return ', '.join(str(p) for p in tmp_data)
    else:
        return convert_value_to_decimal(tmp_data)


def zbx_indices_discovery(data):
    indices = []
    for index in data["indices"]:
        indices.append({discovery_prefix: index})

    return json.dumps({"data": indices}, indent=4)


def es_ping(url):
    if get_es_json(url) is None:
        return "1"
    else:
        return "0"

if query_type == "node":
    print get_es_value(get_data_from_json(es_url + node_path, query_type), query)
    sys.exit(0)
if query_type == "cluster":
    print get_es_value(get_data_from_json(es_url + cluster_path, query_type), query)
    sys.exit(0)
if query_type == "node_keys":
    get_all_keys(get_data_from_json(es_url + node_path, query_type))
    sys.exit(0)
if query_type == "cluster_keys":
    get_all_keys(get_data_from_json(es_url + cluster_path, query_type))
    sys.exit(0)
if query_type == "discovery":
    print zbx_indices_discovery(get_data_from_json(es_url + cluster_path, query_type))
    sys.exit(0)
if query_type == "ping":
    print es_ping(es_url + node_path)
    sys.exit(0)

print "ZBX_NOTSUPPORTED"
