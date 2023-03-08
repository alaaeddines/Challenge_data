import json
from pymongo import MongoClient


def collection(uri):
    client = MongoClient(uri)
    database=client["rhobs"]
    collection=database["people"]
    return collection 




def load(uri="mongodb://localhost:27017/",datapath="data.json.codechallenge.janv22.RHOBS.json"):
    coll=collection(uri=uri)
    with open(datapath,"r") as fp:
        data=json.load(fp)

        for person in data:
            coll.insert_one(person)

#load the json file to MongoDB 
load()

print("done")



