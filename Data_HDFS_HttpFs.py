from flask import Flask
from flask import request
import requests
import pandas as pd


app = Flask(__name__)


## Final request format : http://192.168.4.234:14000/webhdfs/v1/user/hdfs/2015_11_18/web_logs_1.csv?user.name=hdfs&op=OPEN
base_url = 'http://192.168.4.234:14000/webhdfs/v1'

@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/data')
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


if __name__ == '__main__':
    app.run(debug=True)
