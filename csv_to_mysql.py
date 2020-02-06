import time
import os
import csv
import config
import mysql.connector
start = time.time()
start_tuple=time.localtime()
start_time = time.strftime("%Y-%m-%d %H:%M:%S", start_tuple)

def create_tables():
    mydb = mysql.connector.connect(
        host=config.mydb_host,
        user=config.mydb_user,
        passwd=config.mydb_passwd,
        database=config.mydb_database
    ) 
    mycursor = mydb.cursor()
    # mycursor.execute("DROP TABLE IF EXISTS abuse;")
    # create table which has Primary Key and FULLTEXT index
    mycursor.execute(
        """
            CREATE TABLE abuse (
                abuseid int NOT NULL AUTO_INCREMENT, 
                dateadded VARCHAR(255), 
                protocol VARCHAR(255), 
                domain VARCHAR(255),
                link LONGTEXT, 
                url_status VARCHAR(255), 
                threat VARCHAR(255), 
                tags VARCHAR(255), 
                urlhaus_link VARCHAR(255), 
                reporter VARCHAR(255), 
                PRIMARY KEY (abuseid)
            );
        """
    )
    print("Created table: abuse.")

def check_if_tables_exsist():
    dbname=config.mydb_database
    mydb = mysql.connector.connect(
      host=config.mydb_host,
      user=config.mydb_user,
      passwd=config.mydb_passwd,
      database=config.mydb_database
    ) 
    mycursor = mydb.cursor()
    stmt = "show tables FROM browserdb WHERE Tables_in_browserdb LIKE 'abuse';"
    mycursor.execute(stmt)
    return mycursor.fetchall()

def list_in_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

#load csv file
csv_dir=config.abuse_data
command="https_proxy="+config.proxy_settings+" wget -O "+csv_dir+"abuse.csv 'https://urlhaus.abuse.ch/downloads/csv/'"
os.system(command)

if check_if_tables_exsist():
    print("Found table")
    mydb = mysql.connector.connect(
        host=config.mydb_host,
        user=config.mydb_user,
        passwd=config.mydb_passwd,
        database=config.mydb_database
    ) 
    mycursor = mydb.cursor()
    #read data from csv file
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
                        dateadded = str(row[1])
                        protocol = str(row[2]).split('://')[0]
                        domain = str(row[2]).split('://')[-1].split('/')[0] 
                        link = str(row[2]).split('://')[-1].split('/', 1)[-1] 
                        url_status = str(row[3])
                        threat = str(row[4])
                        tags = str(row[5])
                        urlhaus_link = str(row[6])
                        reporter = str(row[7])
                        rline = (dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter)
                        val.append(rline) 
            for i in list(list_in_chunks(val, 1000)):
                sql = "INSERT INTO abuse (dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                mycursor.executemany(sql, i)                  
                # #if record not present in table, insert new record 
                # for i in val:
                #     dateadded = i[0]
                #     protocol = i[1]
                #     domain = i[2]
                #     link = i[3]
                #     url_status = i[4]
                #     threat = i[5]
                #     tags = i[6]
                #     urlhaus_link = i[7]
                #     reporter = i[8]                    
                #     against_fraze = dateadded+protocol+domain+link+url_status+threat+tags+urlhaus_link+reporter
                #     like_fraze = dateadded+protocol+domain+link+url_status+threat+tags+urlhaus_link+reporter
                #     query = (
                #         """
                #             SELECT count(abuseid) FROM abuse 
                #             WHERE MATCH (dateadded,protocol,domain,link,url_status,threat,tags,urlhaus_link,reporter) AGAINST (%s)
                #             AND CONCAT(dateadded,protocol,domain,link,url_status,threat,tags,urlhaus_link,reporter) LIKE %s
                #         """
                #     )
                #     mycursor.execute(query, (against_fraze, like_fraze))
                #     skaits = mycursor.fetchone()
                #     if skaits[0] == 0:
                #         sql = "INSERT INTO abuse (dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                #         mycursor.execute(sql % (dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter))
                # mydb.commit()
    query = "ALTER TABLE abuse ADD FULLTEXT(dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter);"
    mycursor.execute(query)
    mydb.commit()
else:
    print("Didn't found any tables")
    create_tables()
    mydb = mysql.connector.connect(
        host=config.mydb_host,
        user=config.mydb_user,
        passwd=config.mydb_passwd,
        database=config.mydb_database
    ) 
    mycursor = mydb.cursor()
    #read data from csv file
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
                        dateadded = str(row[1])
                        protocol = str(row[2]).split('://')[0]
                        domain = str(row[2]).split('://')[-1].split('/')[0] 
                        link = str(row[2]).split('://')[-1].split('/', 1)[-1] 
                        url_status = str(row[3])
                        threat = str(row[4])
                        tags = str(row[5])
                        urlhaus_link = str(row[6])
                        reporter = str(row[7])
                        rline = (dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter)
                        val.append(rline)
            for i in list(list_in_chunks(val, 1000)):
                sql = "INSERT INTO abuse (dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                mycursor.executemany(sql, i)
    query = "ALTER TABLE abuse ADD FULLTEXT(dateadded, protocol, domain, link, url_status, threat, tags, urlhaus_link, reporter);"
    mycursor.execute(query)
    mydb.commit()


end = time.time()
end_tuple = time.localtime()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", end_tuple)
print("Script ended: "+end_time)
print("Script running time: "+time.strftime('%H:%M:%S', time.gmtime(end - start)))