from pymongo import MongoClient

from datetime import datetime

def age_pyramid(job):
    current_year = datetime.now().year
    age_dict = {"0-9": 0, "10-19": 0, "20-29": 0, "30-39": 0, "40-49": 0, "50-59": 0, "60-69": 0, "70-79": 0, "80-89": 0, "90+": 0}
    
    # get the data about the job and do iteration 
    for doc in collection.find({"job": job}):
        birth_year = int(doc["birthdate"][:4])
        age_range = (current_year - birth_year) // 10
        
        # increment the age count for the specific age range
        if age_range < 0 or age_range >= 10:
            continue
        elif age_range == 9:
            age_dict["90+"] += 1
        else:
            age_dict[f"{age_range*10}-{age_range*10+9}"] += 1
    
    # return the age dictionary
    return age_dict



def companies_list(n):
   
    
    pipeline = [{"$group": {"_id": "$company", "count": {"$sum": 1}}}]

    result = collection.aggregate(pipeline)

    # get the companies that have more than n persons
    companies = [doc["_id"] for doc in result if doc["count"] > n]

    return companies

# connect to MongoDB in my local computer
client = MongoClient("mongodb://localhost:27017/")

# select the database and collection
db = client["rhobs"]
collection = db["people"]

#Count the number of men and women
num_men = collection.count_documents({"sex": "M"})
num_women = collection.count_documents({"sex": "F"})

# print the result
print("Nombre des hommes est:", num_men)
print("Nombre des femmes est:", num_women)

print("la liste des entreprises avec plus de 70 personnes sont: ", companies_list(70))

print("la pyramide des ages pour tonnelier est:",age_pyramid("tonnelier"))


