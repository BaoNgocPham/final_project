from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

#Because of this error, have to use sqlalchemy : pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.

engine = create_engine(URL(
    user='baopham',
    password='Ngoc738218',
    account='gz74928.west-europe.azure',
    warehouse='COMPUTE_BAO',
    database='WILDWEST_9',
    schema='PROCESSED'
))

query = """
    select *,'business' as type from BMSG9901
    union all
    select *, 'business' as type from BMSG9902
    union all
    select *, 'business' as type from BMSG9903
    union all
    select *, 'business' as type from BMSG9904
    union all
    select *, 'business' as type from BMSG0001
    union all
    select *,'residental' as type from RMSG9906
"""
df = pd.read_sql(query, engine)

#concat area code, exchange and line to create telephone number
df["bill_num"] = df["bill_area_code"] + df["bill_exchange"] + df["bill_line"]
df = df.drop(['bill_area_code', 'bill_exchange','bill_line'], axis=1)

df["org_num"] = df["orig_area_code"] + df["orig_exchange"] + df["orig_line"]
df = df.drop(['orig_area_code', 'orig_exchange','orig_line'], axis=1)

df["term_num"] = df["term_area_code"] + df["term_exchange"] + df["term_line"]
df = df.drop(['term_area_code', 'term_exchange','term_line'], axis=1)

#create timestamp
df.loc[df['con_hour'] > '23', 'con_hour'] = '23'
df.loc[df['con_min'] > '59', 'con_min'] = '59'
df.loc[df['con_sec'] > '59', 'con_sec'] = '59'
df['con_timestamp'] = pd.to_datetime(df['con_date'] + ' ' + df[['con_hour', 'con_min', 'con_sec']].astype(str).agg(':'.join, axis=1),format='%y%m%d %H:%M:%S')
df = df.drop(['con_date', 'con_hour','con_min', 'con_sec'], axis=1)
df = df.iloc[:,[0,11,1,2,7,8,9,10,6,3,4,5]]

df['rev_total_sec'] = df['rev_min']*60 + df['rev_sec']

df = df.drop(['rev_min', 'rev_sec'], axis = 1)

#To numeric
df["rev_amt"] = pd.to_numeric(df["rev_amt"])
#df["rev_total_sec"] = pd.to_numeric(df["rev_total_sec"])
#TypeError: cannot convert the series to <class 'float'>. can not use to_numeric
#have to use lambda to change type to float
df["rev_total_sec"]= df["rev_total_sec"].apply(lambda x: float(x))

#remove trailing space
df['term_st'] = df['term_st'].str.strip()
df['term_cntry'] = df['term_cntry'].str.strip()

#SQL compilation error: maximum number of expressions in a list exceeded, expected at most 16,384, got 300,006
chunk_size = 10000  
df.to_sql('final_table', con=engine, index=False, if_exists='replace' , chunksize=chunk_size)

