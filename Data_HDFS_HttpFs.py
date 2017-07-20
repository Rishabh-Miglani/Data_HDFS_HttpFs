from flask import Flask
from flask import request
import requests
import pandas as pd
#import parquet
import json

from avro import schema, datafile, io
import pprint


app = Flask(__name__)


## Final request format : http://192.168.4.234:14000/webhdfs/v1/user/hdfs/2015_11_18/web_logs_1.csv?user.name=hdfs&op=OPEN
base_url = 'http://192.168.4.234:14000/webhdfs/v1'

@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/data_flat')
def data_access_dir():
    dir_location= request.args.get('Datapath')
    dir_url=base_url + dir_location + '?user.name=hdfs&op=OPEN'
    r=requests.get(dir_url)
    #print dir_url
    temp=r.content.split('\n')
    arr=[]
    for line in temp:
        row=line.split(',')
        arr.append(row)
    print type(arr)
    temp_df=pd.DataFrame(arr)
    print temp_df

    #df_temp=pd.DataFrame(temp)
    #print df_temp
    #df_temp=pd.DataFrame(r.content.split(','))
    return  temp_df.to_html()


@app.route('/data_binary_avro')
def data_access_dir_binary_avro():
    dir_location= request.args.get('datadir_avro')
    print dir_location
    dir_url=base_url + dir_location + '?user.name=hdfs&op=OPEN'
    r=requests.get(dir_url,stream=True)
    print r.status_code

    with open('p.avro','wb') as fo:
        for chunk in r:
            fo.write(chunk)

    fo.close()
    print "created"

    OUTFILE_NAME = 'p.avro'

    rec_reader = io.DatumReader()
    df_reader = datafile.DataFileReader(open(OUTFILE_NAME,'rb'),rec_reader)
# Read all records stored inside
    mydata=[]
    for record in df_reader:
        mydata.append(record)

    de=pd.DataFrame(mydata)
    #r=requests.get(dir_url)

    return de.to_html()


    #print dir_url
    # temp=r.content.split('\n')
    # arr=[]
    # for line in temp:
    #     row=line.split(',')
    #     arr.append(row)
    # print type(arr)
    # temp_df=pd.DataFrame(arr)
    # print temp_df
    ####READ te avro file from the location genrated above#####

# @app.route('/data_binary_parquet')
# def data_access_dir_binary_parquet():
#     dir_location= request.args.get('Data_dir_parquet')
#     print dir_location
#     dir_url=base_url + dir_location + '?user.name=hdfs&op=OPEN'
#     r=requests.get(dir_url,stream=True)
#     print r.status_code
#
#     with open('p.parquet','wb') as fo:
#         for chunk in r:
#             fo.write(chunk)
#
#     fo.close()
#     print "created"
#
#     OUTFILE_NAME = 'p.parquet'
#
#     mydata=[]
#     with open(OUTFILE_NAME) as fo:
#         for row in parquet.DictReader(fo):
#             mydata.append(record)
#
#     # Read all records stored inside
#
#
#     de=pd.DataFrame(mydata)
#     #r=requests.get(dir_url)
#
#     return de.to_html()




if __name__ == '__main__':
    app.run(debug=True)
