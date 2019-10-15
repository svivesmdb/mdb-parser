mongo <host> --eval 'db.serverStatus();' > statusForServer.json

mongo <host> --eval 'db.getMongo().getDBNames().forEach(function (d) {print("-- Database: " + d);var curr_db = db.getSiblingDB(d);printjson(curr_db.stats());curr_db.getCollectionNames().forEach(function (coll, d) { print("|- Collection in database: " + coll);var c = curr_db.getCollection(coll); if (typeof c != "function") { printjson(c.stats()); printjson(c.getIndexes()); } });});' > statusForServer.json


mongo --eval 'db.getMongo().getDBNames().forEach(function (d) {print("-- Database: " + d);var curr_db = db.getSiblingDB(d);printjson(curr_db.stats());curr_db.getCollectionInfos().forEach(function (coll, d) {print("|- Collection in database: " + coll.name);var c = curr_db.getCollection(coll.name); if (typeof c != "function" && coll.type != "view"){ try { printjson(c.stats()); printjson(c.getIndexes());} catch (e) {} } });});' > statusForServer.txt


    
db.getMongo().getDBNames().forEach(function (d) {
    print("-- Database: " + d);
    var curr_db = db.getSiblingDB(d);
    printjson(curr_db.stats()); 
    curr_db.getCollectionNames().forEach(function (coll, d) { 
        print("|- Collection in database: " + coll);
        var c = curr_db.getCollection(coll); 
        if (typeof c != "function") { 
            printjson(c.stats()); printjson(c.getIndexes()); 
        } 
    });
});
