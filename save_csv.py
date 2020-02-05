import time
import os
import csv
import config

csv_dir=config.abuse_data+'test/'
# print(csv_dir)
command="https_proxy="+config.proxy_settings+" wget -O "+csv_dir+"abuse.csv 'https://urlhaus.abuse.ch/downloads/csv/'"
os.system(command)

