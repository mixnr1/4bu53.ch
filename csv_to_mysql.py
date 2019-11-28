import time
import os
import csv
import config
import mysql.connector
start = time.time()
start_tuple=time.localtime()
start_time = time.strftime("%Y-%m-%d %H:%M:%S", start_tuple)
mydb = mysql.connector.connect(
    host=config.mydb_host,
    user=config.mydb_user,
    passwd=config.mydb_passwd,
    database=config.mydb_database
    ) 
mycursor = mydb.cursor()
csv_dir=config.abuse_data
csv_files=[entry for entry in os.listdir(csv_dir) if os.path.isfile(os.path.join(csv_dir, entry))]
for csv_f in csv_files:
    if csv_f.endswith('csv'):
        with open(os.path.join(csv_dir, csv_f)) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')           
            for row in csv_reader:
                if row[0].startswith('#'):
                    pass
                else:
                    mycursor.execute('INSERT INTO abuse(id, dateadded, url, url_status, threat, tags, urlhaus_link, reporter)  VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")', row) 
            mydb.commit()
end = time.time()
end_tuple = time.localtime()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", end_tuple)
print("Script ended: "+end_time)
print("Script running time: "+time.strftime('%H:%M:%S', time.gmtime(end - start)))
