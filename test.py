import pymongo 

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["test"]
collection = db["test"]

results = []
results.append({"1": {"attribute1": "value1", "attribute2": "value2"}})
results.append({"2": {"attribute1": "value3", "attribute2": "value4"}})

collection.insert_many(results)

client.close()