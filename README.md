# Data Project 2: Import JSON to MongoDB

In this data project you will write a single file of code to import a bundle of fifty (50) separate JSON files into a new collection within a MongoDB database. Each file contains one or more record.

The script you write should import every individual record when executed once. You cannot run the script 50 separate times to import all files and records.

The data for import can be found in the `data/` directory of this repository.

Follow the steps below carefully and create a solution of your own.

## 1. Fork this Repository and open in Gitpod

Fork this repo so that you are working with your own copy of the code and can add, commit, and push freely. You will be submitting the URL to your fork for grading.

Open your repository in Gitpod by appending `https://gitpod.io/#` before the GitHub URL.

## 2. Choose Your Code

You are free to write this assignment in a `bash` script using a CLI tool called `mongoimport`, in Python using `pymongo`,
or both.

## 3. Connecting to MongoDB Atlas

See [this page](https://canvas.its.virginia.edu/courses/105117/pages/mongodb-credentials) in Canvas for how to set up connection credentials if you need them.

DO NOT commit the password to your GitHub repository!

For command-line access to MongoDB you have `MONGO-ATLAS` as an available command pre-built into your Gitpod configuration.

### Python in Gitpod

If using Python in Gitpod you already have the `MONGOPASS` environment variable and can then connect to a db/collection in Atlas using this code:

```
from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.<db-name>
# specify a collection
collection = db.<collection-name>
```

- [`pymongo` documentation](https://pymongo.readthedocs.io/en/stable/)


### `mongoimport` in Gitpod

If using `mongoimport` within a `bash` script in Gitpod, you should create a new Gitpod User Variable with your full URI (including username, password, host address, and DB name).

For instance, you could create a new variable named `MONGODB` with a value of `mongodb+srv://nmagee:xxxxxxxxxxx@cluster0.pnxzwgz.mongodb.net/<YOUR-DB>`

Note that you should replace the `xxxxxx` with the actual password, and replace `<YOUR-DB>` with the name of your MongoDB database. **Your URI must specify a database at the end.**

Then you can call this URI within a terminal command to connect automatically each time with each invocation like this:

```
mongoimport --uri $MONGODB --collection test  data.json --jsonArray
```

## 4. Listing Directory Contents

It is up to you to determine how you want to import the fifty files within `data/`. But here are some methods you might want to draw from:

### `bash`

In `bash` you can traverse a directory's contents, item by item:

```
for file in data/*
do
  echo "$file"
done
```

This means that each file name, in the course of each for-loop, becomes a variable you can use or pass to other code or commands.

For instance, this version of the above code would pass the file name of each file into a separate Python script:

```
for file in data/*
do
  /usr/bin/python3 my-import-script.py $file
done
```

### Python

In Python you can also traverse a directory's contents, item by item:

```
import os

directory = "data"

for filename in os.listdir(directory):
  with open(os.path.join(directory, filename)) as f:
    print(f)
    # do other things with f
```

## 5. Importing

### `mongoimport` with `bash`

To import a single JSON file into MongoDB using `mongoimport` use this syntax:

```
mongoimport --uri $MONGODB --collection test data.json --jsonArray
```

Note these details:

- The DB name should already be specified as part of the URI you created above.
- The collection should be specified in the command. You can create a new collection by simply naming it here. This example imports into a collection mamed "test".
- The data file itself is then passed.
- Finally, you must indicate that you are passing in a `--jsonAarray` for the command to succeed.

- [`mongoimport` documentation](https://www.mongodb.com/docs/database-tools/mongoimport/)

### `pymongo` in Python

To import a single JSON file into MongoDB using `pymongo` in a Python script use this syntax:

```
# assuming you have defined a connection to your db and collection already:

    # Loading or Opening the json file
    try:
      file_data = json.load(f)
    except Exception as e:
      print(e, "error when loading", f)
     
# Inserting the loaded data in the collection
# if JSON contains data more than one entry
# insert_many is used else insert_one is used
    if isinstance(file_data, list):
      try:
        collection.insert_many(file_data)
      except Exception as e:
        print(e, "when importing into Mongo")
    else:
      try:
        collection.insert_one(file_data)
      except Exception as e:
        print(e)
```

- [`pymongo` documentation](https://pymongo.readthedocs.io/en/stable/)


## 6. Error Handling

You may encounter an error when importing this stack of files. Your code should be able to handle this without breaking. You do not need to log your errors but your code should continue processing additional records as much as possible.

Remember to assess where in your flow the errors occur, and handle them accordingly at each/any/every point with separate handlers as needed.

If you cannot elegantly handle the errors you may want to change your approach, or even your programming language.

## 7. Testing / Clearing Your Collection

As you test your code, MongoDB will not allow you to re-import files that have already been imported. To drop the collection to test again, open `mongosh` by typing `MONGO-ATLAS` in the Gitpod terminal.

Then:
```
use nem2p               # specify your database name
db.COLLECTION.drop()    # where COLLECTION is the name of your collection
```

## 8. Import Count

After successfully importing all complete records contained in all the files, determine how many:

- complete documents have been imported into your collection
- complete documents could not be imported
- corrupted documents exist in the fileset

This file can be manually calculated and populated. Your code does need to output this file.

Note the number of records for each category in the file named `count.txt` and commit it to your repository.

## 9. Submit

Add, commit, and push your working script and the `count.txt` file to your fork of the repository.

Submit the GitHub URL of your fork for grading. This DP is worth 17 possible points.

### Grading Rubric

| Element  | Points |
|---|---|
| Setup  | 1  |
| Connection  | 2  |
| Looping through files | 3  |
| Inserting into DB  | 4  |
| Error Handling  | 4  |
| Count accuracy  | 2  |
