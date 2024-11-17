
import json
import requests
import pandas as pd 
from pandas import json_normalize 

import pyodbc as odbc
import sqlalchemy
from sqlalchemy import create_engine


symbol = ['AAPL', 'MSFL','GOOG','TSLA','SHLRF']

#url = 'https://www.alphavantage.co/query?function=CASH_FLOW&symbol=IBM&apikey=H7PRX001BFGSU9B6'

data=[]
for id, item in enumerate(symbol):

    url2 = f"https://api.twelvedata.com/time_series?symbol={item}&interval=1day&apikey=3c0f9ed6cbff4039a1f086b7e22f63bc&source=docs"
    response2 = requests.get(url2).json()
    data.append(response2)

data=pd.json_normalize(data)

def convert_objects_to_strings(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Convert the `object` columns into `string`s so that SQLAlchemy can handle them"""
    for col, col_type in data.dtypes.items():
        if col_type == 'object':
            data_frame.loc[:, col] = data_frame.loc[:, col].astype('string')
    return data

#print(data)
#data = pd.json_normalize(data)



#establish connection to S


DRIVER = 'ODBC Driver 17 for SQL Server'
SERVER ='DESKTOP-4Q0I2G0\\SQLEXPRESS'
DATABASE = 'twelve'

conn = odbc.connect('DRIVER={ODBC Driver 17 for SQL Server} ; \
                   SERVER=' + SERVER + ';\
                   DATABASE=' + DATABASE + ';\
                   Trusted_Connection=yes;')


engine = sqlalchemy.create_engine(f'mssql+pyodbc://DESKTOP-4Q0I2G0\\SQLEXPRESS/twelve?trusted_connection=yes&driver=ODBC Driver 17 for SQL Server')

convert_objects_to_strings(data).to_sql("fact_stock_list", engine,if_exists="replace", index=False)

