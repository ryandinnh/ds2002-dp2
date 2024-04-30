#!/usr/bin/env python3
from pymongo import MongoClient, errors
import os
import json

def importJSONToMongoDB():
    totalImported = 0
    totalOrphaned = 0
    totalCorrupted = 0
    dupeKeyError = 0 #counter for dupe key exception

    MONGOPASS = os.getenv('MONGOPASS')
    client = MongoClient(MONGOPASS, connectTimeoutMS=200, retryWrites=True)
    db = client.rqd3qmk
    collection = db.records

    path = "data"

    for (root, dirs, files) in os.walk(path):
        for f in files:
            file_path = os.path.join(root, f)
            try:
                with open(file_path, 'r') as file:
                    file_data = json.load(file)
                for doc in file_data: #parse through the actual records in the .json file so I dont have to use insert_many. I switched to this because of the missing orphaned records but I was overthinking the solution.
                    try:
                        #manually check which files are getting processed to see where these missing 5 records are. (not neccessary anymore)
                        #print(f"Processing document with _id: {doc.get('_id', 'No ID found')} in file: {f}")
                        collection.insert_one(doc)
                        totalImported += 1
                    except errors.DuplicateKeyError as e:
                        #I was getting a duplicate key error when running so added exception. Should honestly be only getting these when not clearing the collection after?
                        print(f"dupe key error {f}: {str(e)}")
                        dupeKeyError += 1
            except json.JSONDecodeError as e:
                print(f"Corrupted record {f}: {str(e)}")
                totalCorrupted += 1
                #orphaned records are the remaining records in corrupted files
                if isinstance(file_data, list):
                    totalOrphaned += (len(file_data) - totalCorrupted)
            except Exception as e:
                print(f"Other error: Failed to import {f}: {str(e)}")
                #totalOrphaned += 1 #assuming these are the orphaned documents

    print(f"Total Imported: {totalImported}")
    print(f"Total Orphaned: {totalOrphaned}")
    print(f"Total Corrupted: {totalCorrupted}")
    print(f"Total dupeKeyErrors: {dupeKeyError}")

if __name__ == '__main__':
    importJSONToMongoDB()

#total is 295 records? so missing 5