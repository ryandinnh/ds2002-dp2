#!/usr/bin/env python3
from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

#script functionality
def importJSONToMongoDB():\
    #variables to update count.txt
    totalImported = 0
    totalOrphaned = 0 #in count.txt its orphaned but how do I check that?
    totalCorrupted = 0

    #connecting to mongoDB atlas
    MONGOPASS = os.getenv('MONGOPASS')
    #uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
    client = MongoClient(MONGOPASS, connectTimeoutMS=200, retryWrites=True) #The original arguments from the instructions not needed because $MONGOPASS includes uri, user, and pass
    # specify a database
    db = client.rqd3qmk
    # specify a collection
    collection = db.records

    #listing directory contents
    path = "data"

    for (root, dirs, file) in os.walk(path):
        try:
            for f in file:
                #importing
                file_path = os.path.join(root, f)
                with open(file_path, 'r') as file:
                    file_data = json.load(file)
                # Inserting the loaded data in the collection
                # if JSON contains data more than one entry
                # insert_many is used else insert_one is used
                if isinstance(file_data, list):
                    collection.insert_many(file_data)
                    totalImported += len(file_data)
                else:
                    collection.insert_one(file_data)
                    totalImported += 1
        #error handling
        #corrupted documents exist in the fileset
        except json.JSONDecodeError:
            totalCorrupted += 1
        #complete documents could not be imported
        except Exception as e:
            print(f"Failed to import {filename}: {str(e)}")
            totalOrphaned += 1

    print(f"Total Imported: {totalImported}")
    print(f"Total Orphaned: {totalOrphaned}")
    print(f"Total Corrupted: {totalCorrupted}")


#script execution
if __name__ == '__main__':
    importJSONToMongoDB()
