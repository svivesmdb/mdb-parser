import re
import json
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from bson import BSON

# To not bloat the code I've separated the pipelines somewhere else
import sizingpipelines


"""
Each file that will be processed is the output of only one server' instance.
There are two types of files that can be processed, serverstatus and db status.
"""

def readDiagnosticsData():
    import bson
    import zlib
    from bson.codec_options import CodecOptions
    mypath = "./diagnosticsdatasample/"
    onlyfiles = [f for f in listdir(mypath) if path.isfile(path.join(mypath, f))]

def stripUnwantedCharacters(block):
    """
    This function will remove unwanted characters such as tabs, dollar symbols, 
new-line carriage returns etc. so that it can be processed as a valid JSON.
    """
    block = block.replace('\r\n','')
    block = block.replace('\n','')
    block = block.replace('\t','')
    block = block.replace('/','')
    block = block.replace('$','')

    # This is to avoid duplicate inserts
    block = block.replace('_id','id')
    block = block.strip()

    return block


def replaceExtendedJsonTypestoBasicJson(block):
    """
    This function will reemplace extended JSON types such as NumberDecimal, NumberLong etc. 
    stripping them and converting them into plain JSON (no types, string types or integer).
    """

    # @TODO ADD MORE TYPES HERE
    keywords = ['BinData','NumberLong','ObjectId','Decimal128']

    for k in keywords:
        if block.find(k) > 0:
            block = re.sub(r'' + k + r'\(\"([0-9\.]+)+\"\)', r'\1', block)
            block = re.sub(r'' + k + r'\(([0-9.]+)+\)', r'\1', block)

    if block.find('ISODate') > 0:
        block = re.sub(r'ISODate\(\"([0-9TZ.:\-])+\"\)', r'"\1"', block)

    if block.find('Timestamp') > 0:
        block = re.sub(r'Timestamp\(([0-9])+, ([0-9])+\)', r'"\1"', block)   

    return block


def replaceDotsOnJsonKeys(block):
    """
    Convert keys that are using 'something.1' to 'something_1'.
    this will avoid problems on "normal" JSON
    """
    matches = re.findall(r"(\"[A-Za-z0-9\_\.]+\")+", block)
    
    for match in matches:
        if match.find('.') > -1:
            final_res = match.replace('.','_')
            block = block.replace(match, final_res)
    
    return block

def doWeNeedToIgnoreLine(line, openbrakets):
    ignore = False
    # Ignore random stuff such as empty lines, new lines and random numbers
    ignore = ignore or (line.strip() == '' or line == '\n' or line.strip().isdigit())
    # Ignore lines that we have on the script we send to the customer to retrieve the data we3 are parsing        
    ignore = ignore or (line.find('|- Collection in database') > -1 or line.find('-- Database: ') > -1)
    # Ignore Version and connections strings
    ignore = ignore or (line.find('MongoDB shell version') > -1 or line.find("connecting to: ") > -1 or line.find("MongoDB server version") > -1)
    ignore = ignore or (line.find('Implicit session') > -1)
    ignore = ignore or (openbrakets == 0 and line.find('[') > -1 and line.find(']') > -1)

    return ignore

####################################################################################
####################################################################################
####################################################################################
####################################################################################

def processServerStatusFile(f, infile, fname):

    fout = open("./output/" + fname + "__OUT.json", "w+")

    # Will track how many open brackets we have to parse each 
    # output as a separate JSON.
    openbrakets = 0
    block = ''
    out = []
    isindexdesc = False
    lastCollection = ''

    for line in f: 

        if line.find('{') > -1:
            openbrakets = openbrakets + 1
        elif line.find('}') > -1:
            #print("Found closing bracket on line " + line)
            openbrakets = openbrakets - 1

        if doWeNeedToIgnoreLine(line, openbrakets):
            line = ''

        if openbrakets >= 0 and line != '':
            # All good with this line, we can add it to the JSON block
            block += line

        if openbrakets == 0 and block != '':
            # Now, we finished processing the entire JSON block
            # since we found the final closing bracket (openbrackets = 0)
            # we can proceed strip out the characters we dont want and
            # convert the block to JSON
            block = stripUnwantedCharacters(block)
            block = replaceExtendedJsonTypestoBasicJson(block)
            block = replaceDotsOnJsonKeys(block)

            if block != '':
                # The final version of the block is all good,
                # we can convert this block into JSON 
                block_json = False

                try:
                    block_json = json.loads(block)
                except Exception as e:
                    print("! Error decoding json " + block)
                    print(e)

                if block_json:
                    out.append(block)
                    block = ''

    fout.write("\n".join(out))

def processStatsFile(f, infile, fname, isServerStatusFile):

    fout = open("./output/" + fname + "__OUT.json", "w+")

    # Will track how many open brackets we have to parse each 
    # output as a separate JSON.
    openbrakets = 0
    block = ''
    out = []
    isindexdesc = False
    lastCollection = ''
    lastDb = ''

    for line in f: 

        if line.find('{') > -1:
            openbrakets = openbrakets + 1

        if line.find('}') > -1:
            openbrakets = openbrakets - 1

        if doWeNeedToIgnoreLine(line, openbrakets):
            line = ''

        # We list the indices for a given collection right at the end 
        # of a collection itself, so add these indices as a new field on the document
        if not isServerStatusFile and openbrakets == 0 and line.find('[') > -1:
            openbrakets = 1
            isindexdesc = True
            line = '{ "calculatedListOfindexes": ['

        if not isServerStatusFile and line.find(']') > -1 and isindexdesc == True:
            line = '] }'
            openbrakets = 0
            isindexdesc = False

        if openbrakets >= 0:
            # All good with this line, we can add it to the JSON block
            block += line

        if openbrakets == 0 and block != '':
            # Now, we finished processing the entire JSON block
            # since we found the final closing bracket (openbrackets = 0)
            # we can proceed strip out the characters we dont want and
            # convert the block to JSON
            block = stripUnwantedCharacters(block)
            block = replaceExtendedJsonTypestoBasicJson(block)
            block = replaceDotsOnJsonKeys(block)

            if block != '':
                # The final version of the block is all good,
                # we can convert this block into JSON 
                block_json = False

                try:
                    block_json = json.loads(block)
                except Exception as e:
                    print("! Error decoding json " + block)
                    print(e)

                if block_json:
                    if not isServerStatusFile and block_json and block_json.get('calculatedListOfindexes') != None:
                        block_json['forCollection'] = lastCollection
                        block = json.dumps(block_json)
                    
                    if block_json.get('db') != None:
                        lastDb = block_json.get('db')
                    else:
                        block_json['belongsToDb'] = lastDb

                    if not isServerStatusFile and block_json and block_json.get('ns') != None:
                        lastCollection = block_json.get('ns')
                        
                    out.append( json.dumps(block_json) )
                    block = ''

    fout.write("\n".join(out))

    return out

def processFile(infile, fname, stats_collection, status_collection, restrictions):
    f = open(infile, "r")
    # Read all lines first to differentiate if it's a server status 
    # file or it's a sizing collections file.
    lines = f.read()
    f.flush()
    f.close()
    
    f=open(infile, "r")
    # Simple check, we are checking if this file is a status file
    # collection stats files do not have such keywords.
    isStatusFile = lines.find('uptimeEstimate') > -1 and lines.find('uptimeMillis') > -1
    process_output = processStatsFile(f, infile, fname, isStatusFile)

   # Trigger the correct pipeline and save it to the correct collection
   # there are two possible pipelines and target collections, the status 
   # one and the collection stats one.
    pipeline_to_execute = []
    if isStatusFile:
        target_collection = status_results_col
        source_collection = status_collection
        pipeline_to_execute = sizingpipelines.status_pipeline.copy()
    else:
        target_collection = results_col
        source_collection = stats_collection
        pipeline_to_execute = sizingpipelines.pipeline.copy()
    
    # Now, add the JSON documents parsed into the appropiate collection
    process_object_dict = {}
    for output in process_output:
        process_object_dict = json.loads(output)
        # Mark the origin of the calculations
        process_object_dict['sourcefile'] = fname
        if source_collection:
            source_collection.insert_one(process_object_dict)


    # If we need to restrict
    match_stage_params = {}
    if restrictions == None:
        # Add the matching needed to make sure we run the pipeline only for the current file.    
        match_stage_params = {"sourcefile":fname}

    else:
        match_stage_params = {"sourcefile":fname}
        
        # Only including the databases specified and none other
        if 'onlyDb' in restrictions:
            match_stage_params['db'] = restrictions['onlyDb'] 
            #match_stage_params['belongsToDb'] = restrictions['onlyDb']

        # Excluding specific collections
        if 'excludeCollection' in restrictions:
            match_stage_params['ns'] = {"$nin": restrictions['excludeCollection']} 
        
        # Excluding specific databases
        if 'excludeDb' in restrictions:
            match_stage_params['db'] = {"$nin": restrictions['excludeDb']}
            match_stage_params['belongsToDb'] = {"$nin": restrictions['excludeDb']}

    
    pipeline_to_execute.insert(0, {"$match":match_stage_params})

    # Include where the data comes from on the pipeline
    pipeline_to_execute.append({"$addFields":{"sourceFile":fname}})

    # Run the aggregation pipeline on the results
    if len(process_output) > 0:
        if source_collection:
            result = list(source_collection.aggregate(pipeline_to_execute))
            if len(result) > 0:
                pipeline_results = result[0]
                del pipeline_results['_id']
                target_collection.insert_one(pipeline_results)


if __name__== "__main__":
    import sys
    from os import listdir, path, mkdir
    import argparse

    parser = argparse.ArgumentParser()
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source', help='Source directory')
    group.add_argument('--file', help='Filename')
    
    #parser.add_argument('-e', help='Extension')
    parser.add_argument('--collection', help='Destination collection name', required=True)
    parser.add_argument('--db',  help='Destination database name', default="mdb_sa_sizings")
    parser.add_argument('--host', help='Host of the MongoDB server')
    parser.add_argument('--port', help='Port of the MongoDB server')
    parser.add_argument('--uri', help='Complete string for the destination MongoDB server')
    parser.add_argument('--extension', help='File extension',default="txt")
    
    parser.add_argument('--onlyDb',  help='Only calculate the stats for one single database')
    parser.add_argument('--excludeDb',  help='Ignore specific databases when calculating the stats', action='append')
    parser.add_argument('--excludeCollection',  help='Ignore scpecific collections when calculating the stats',action='append')

    params = parser.parse_args()
    output_database_name = params.db
    output_collection_name = params.collection

    # Connect to which MongoDB instance?
    connectionStr = ''
    if params.uri:
        connectionStr = params.uri # "mongodb://localhost:27017"
    elif params.host and params.port:
        connectionStr = "mongodb://%s:%s" % (params.host, params.port)
    
    if not path.exists('./output'):
        mkdir("./output")

    status_results_col = results_col = stats_collection = status_collection = None
    
    if connectionStr != '':
        client = MongoClient(connectionStr)
        
        db = client[output_database_name]

        stats_collection = db[output_collection_name + "-raw-statistics"]
        stats_collection.delete_many({})

        status_collection = db[output_collection_name + "-raw-status"]
        status_collection.delete_many({})

        results_col = db[output_collection_name + "-statistics-results"]
        results_col.delete_many({})

        status_results_col = db[output_collection_name + "-status-results"]
        status_results_col.delete_many({})

    # Check if we are excluding any databases or collections on the calculations
    restrictions = None
    if params.excludeDb or params.excludeCollection or params.onlyDb:
        restrictions = {}
        if params.excludeDb: restrictions['excludeDb'] = params.excludeDb
        if params.excludeCollection: restrictions['excludeCollection'] = params.excludeCollections
        if params.onlyDb: restrictions['onlyDb'] = params.onlyDb

    if params.source:
        mypath = params.source
        onlyfiles = [f for f in listdir(mypath) if path.isfile(path.join(mypath, f))]
        i = 0
                
        for f in onlyfiles:
            if f.endswith('.' + params.extension): 
                i+=1
                processFile(path.join(mypath, f), f, stats_collection, status_collection, restrictions)
        
        print("Done processing " + str(i) + " files")

    elif params.file:
        import os
        import ntpath
        fname = ntpath.basename(params.file)
        fname = os.path.splitext(fname)[0]
        processFile(params.file, fname, stats_collection, status_collection, restrictions)
        print("Done!")

    