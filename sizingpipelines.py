pipeline = [{'$match': {
    'collections': {
        "$exists": 1
    }
    }}, {
    "$group": {
    "_id": '$ns',
    "DataSize_no_compression": {
        "$sum": {
        "$divide": [
            '$dataSize',
            1073741824
        ]
        }
    },
    "indexSize_GB": {
        "$sum": {
        "$divide": [
            '$indexSize',
            1073741824
        ]
        }
    },
    'StorageSize_compressed_GB': {
        '$sum': {
        '$divide': [
            '$storageSize',
            1073741824
        ]
        }
    },
    'objects': {
        '$sum': '$objects'
    },
    'objects_in_Millions': {
        "$sum": {
        "$divide": [
            '$objects',
            1000000
        ]
        }
    },
    'collections': {
        '$push': {
        'collections': '$collections',
        'numIndices': '$indexes',
        'database': '$db'
        }
    }
}}]

status_pipeline = [{
    "$project": {
    "network_Out_GB":{
        '$divide': [
            "$network.bytesOut",
            1073741824
        ]
    },
        "network_In_GB":{
        '$divide': [
            "$network.bytesIn",
            1073741824
        ]
    },
    "uptime_Days":{
        "$divide": [
        "$uptime", 86400
        ]
    },
        "uptime_Years":{
        "$divide": [
        "$uptime", 31536000
        ]
    },
    "query_stats_In_Millions": {
        "inserts" : {
        "$divide":["$opcounters.insert",1000000]
        },
        "query" : {
        "$divide":["$opcounters.query",1000000]
        },
        "getmore" : {
        "$divide":["$opcounters.getmore",1000000]
        },
        "command" : {
        "$divide":["$opcounters.command",1000000]
        },
        "delete" : {
        "$divide":["$opcounters.delete",1000000]
        }
    },
    "query_stats_raw": {
        "inserts" : "$opcounters.insert",
        "query" : "$opcounters.query",
        "getmore" : "$opcounters.getmore",
        "command" : "$opcounters.command",
        "delete" : "$opcounters.delete",
    },
    "replicationStats":"$replicationStats"
}}]