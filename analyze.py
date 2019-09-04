import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import json
import numpy as np

engine = create_engine('postgresql://postgres:Anton1995@localhost/allerts')
df = pd.read_csv('vh.csv')
df['change sign'] = np.sign(df['liters'].diff().fillna(0)).shift(-1).fillna(0)
# for date in df.head()['timestamp']:
#    print(pd.to_datetime(date).timestamp())


def create_ids_json():
    unique_id = df['id'].unique().tolist()
    with open('unique_id', 'w') as f:
        json.dump(unique_id, f)
    return len(unique_id)


def one_ts_analyze(c_id):
    for_df = []
    rest_time = []
    refill_time = []
    consumption_time = []
    one_car = df.loc[df['id'] == c_id]
    for ind, row in one_car.iterrows():
        pass
    return one_car.loc[one_car['change sign'] == 1.0]
    # return (pd.to_datetime(row['timestamp']).timestamp() - pd.to_datetime(st_time).timestamp())/60


def one_ts_analyze_old(c_id):
    for_df = []
    rest_time = []
    refill_time = []
    consumption_time = []
    one_car = df.loc[df['id'] == c_id]
    st_liters = one_car['liters'][0]
    # st_time = one_car['timestamp'][0]

    for ind, row in one_car.iterrows():
        if row['liters'] - st_liters < 0:
            # start fuel consumption
            consumption_time.append(row['timestamp'])
            if row['liters'] - st_liters == 0 or row['liters'] - st_liters > 0:
                # pd.to_datetime(row['timestamp']).timestamp()
                pass
            st_liters = row['liters']
            continue
        if row['liters'] - st_liters > 0:
            st_time = row['timestamp']
            # start refill
            refill_time.append(pd.to_datetime(row['timestamp']).timestamp())
            pass
        else:
            # start rest
            rest_time.append(pd.to_datetime(row['timestamp']).timestamp())
            continue
            # return (pd.to_datetime(row['timestamp']).timestamp() - pd.to_datetime(st_time).timestamp())/60


print(one_ts_analyze("d28378aec6bd1214b87047f2af506f70"))
