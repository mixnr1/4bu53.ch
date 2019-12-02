import time
import os
import csv
import config
import mysql.connector
start = time.time()
start_tuple=time.localtime()
start_time = time.strftime("%Y-%m-%d %H:%M:%S", start_tuple)
csv_dir=config.abuse_data
command="https_proxy="+config.proxy_settings+" wget -O "+csv_dir+"abuse.csv 'https://urlhaus.abuse.ch/downloads/csv/'"
os.system(command)
mydb = mysql.connector.connect(
    host=config.mydb_host,
    user=config.mydb_user,
    passwd=config.mydb_passwd,
    database=config.mydb_database
    ) 
mycursor = mydb.cursor()

mycursor.execute("DROP TABLE IF EXISTS abuse;")
mycursor.execute("CREATE TABLE abuse (id VARCHAR(255), dateadded VARCHAR(255), protocol LONGTEXT, domain LONGTEXT, link TEXT, url_status VARCHAR(255), threat VARCHAR(255), tags VARCHAR(255), urlhaus_link VARCHAR(255), reporter VARCHAR(255))")    


csv_dir=config.abuse_data
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
                    # print(id + " , " + dateadded + " , " + protocol + " , " + domain + " , " + link+ " , " + url_status+ " , " + threat+ " , " + tags+ " , " + urlhaus_link + " , " + reporter)
                    rline = (id, dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter)
                    val.append(rline)
                    
            sql="INSERT IGNORE INTO abuse(id, dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter)  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            mycursor.executemany(sql, val)
            mydb.commit()
        os.remove(csv_dir+csv_f)
end = time.time()
end_tuple = time.localtime()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", end_tuple)
print("Script ended: "+end_time)
print("Script running time: "+time.strftime('%H:%M:%S', time.gmtime(end - start)))
