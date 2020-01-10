from pymongo import MongoClient
import time
import os
import csv
import config

start = time.time()
start_tuple=time.localtime()
start_time = time.strftime("%Y-%m-%d %H:%M:%S", start_tuple)

#1.step - load csv file
csv_dir=config.abuse_data
command="https_proxy="+config.proxy_settings+" wget -O "+csv_dir+"abuse.csv 'https://urlhaus.abuse.ch/downloads/csv/'"
os.system(command)
#2.step - read data from csv file
csv_files=[entry for entry in os.listdir(csv_dir) if os.path.isfile(os.path.join(csv_dir, entry))]
for csv_f in csv_files:
    if csv_f.endswith('csv'):
        with open(os.path.join(csv_dir, csv_f)) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')           
            val=[]
            for row in csv_reader:
                if row[0].startswith('#'):
                    pass
                else:
                    id = str(row[0])
                    dateadded = str(row[1])
                    protocol = str(row[2]).split('://')[0]
                    domain = str(row[2]).split('//')[-1].split('/')[0]
                    link = str(row[2]).split('//')[-1].split('/', 1)[-1]
                    url_status = str(row[3])
                    threat = str(row[4])
                    tags = str(row[5])
                    urlhaus_link = str(row[6])
                    reporter = str(row[7])
                    rline = { "id": id, "dateadded": dateadded, "protocol": protocol, "domain": domain, "link": link, "url_status": url_status, "threat": threat, "tags": tags, "urlhaus_link": urlhaus_link, "reporter": reporter}
                    val.append(rline) 
        #3.step delete csv file
        os.remove(csv_dir+csv_f)
def data_import_mongodb():
    #4. step - check if a specfic database exist
    myclient = MongoClient("mongodb://localhost:27017/")
    dblist=myclient.list_database_names()#Return a list of your system's databases
    if "abuse" in dblist:
        #5.step - check if collection exists
        mydb=myclient["abuse"]
        collist=mydb.list_collection_names()##Return a list of collections in database
        if "data" in collist:
            print("Found old dataset.")
        #6.step - if collection exists delete all collections
            mycol = mydb["data"]
            mycol.drop()#deletes the "data" collection
            print("Old dataset deleted.")
        data_import_mongodb()
    else:
        #7.step - create database. In MongoDB, a database is not created until it gets content!
        mydb=myclient["abuse"]
        print("Database created.")
        #8.step - create collection
        mycol=mydb["data"]
        print("Collection created.")
        #9.step - insert some data
        x = mycol.insert_many(val)
        print("Data from csv file inserted in collection.")
        x = mycol.find_one()#Find the first document in collection:
        print("Data sample: ", x) 
data_import_mongodb()

end = time.time()
end_tuple = time.localtime()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", end_tuple)
print("Script ended: "+end_time)
print("Script running time: "+time.strftime('%H:%M:%S', time.gmtime(end - start)))

