import time
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
# mycursor.execute("SELECT b.* FROM tmp_bookmarks as b inner join abuse as a on b.url = CONCAT(a.protocol,'://',a.domain,'/',a.link);")
mycursor.execute("SELECT b.* FROM bookmarks as b inner join abuse as a on b.url = CONCAT(a.protocol,'://',a.domain,'/',a.link);")
myresult = mycursor.fetchall()
for x in myresult:
    print("BOOKMARKS: ",x)
# mycursor.execute("SELECT d.* FROM tmp_downloads as d inner join abuse as a on d.link = CONCAT(a.protocol,'://',a.domain,'/',a.link);")
mycursor.execute("SELECT d.* FROM downloads as d inner join abuse as a on d.link = CONCAT(a.protocol,'://',a.domain,'/',a.link);")
myresult = mycursor.fetchall()
for x in myresult:
    print("DOWNLOADS: ",x)
# mycursor.execute("SELECT h.* FROM tmp_history as h inner join abuse as a on concat(h.protocol,h.domain,h.link) = concat(a.protocol,a.domain,a.link);")
mycursor.execute("SELECT h.* FROM history as h inner join abuse as a on concat(h.protocol,h.domain,h.link) = concat(a.protocol,a.domain,a.link);")
myresult = mycursor.fetchall()
for x in myresult:
    print("HISTORY: ",x)

end = time.time()
end_tuple = time.localtime()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", end_tuple)
print("Script ended: "+end_time)
print("Script running time: "+time.strftime('%H:%M:%S', time.gmtime(end - start)))
