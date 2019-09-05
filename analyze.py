import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import json
import numpy as np

engine = create_engine('postgresql://postgres:Anton1995@localhost/allerts')
df = pd.read_csv('vh.csv')
# df['change liters'] = df['liters'].diff().fillna(0).shift(-1).fillna(0)
df['unix time'] = pd.to_datetime(df['timestamp'])
df.to_csv('new_data.csv')


def create_ids_json():
    unique_id = df['id'].unique().tolist()
    with open('unique_id', 'w') as f:
        json.dump(unique_id, f)
    return len(unique_id)


def action_handler(data: list, consumption: bool = 0):
    temp_df = pd.DataFrame(data)
    start_period_date = temp_df['time'].min()
    min_date = start_period_date.timestamp()
    max_date = temp_df['time'].max().timestamp()

    liters_delta = temp_df['liters'].max() - temp_df['liters'].min()
    action = 'refill'

    if consumption:
        liters_delta = - liters_delta
        action = 'consumption'

    duration = max_date - min_date
    return {'start period': start_period_date, 'duration': duration, 'liters delta': liters_delta, 'action': action}


def one_ts_analyze(c_id):
    for_df = []
    temp = []
    one_car = df.loc[df['id'] == c_id]
    one_car['change liters'] = one_car['liters'].diff().fillna(0).shift(-1).fillna(0)
    sign = one_car['change sign'][0]
    for ind, row in one_car.iterrows():
        # для отбрасывания колебаний датчика
        if row['change sign'] != sign and len(temp) < 2:
            sign = row['change sign']
            continue
        temp.append({'time': pd.to_datetime(row['unix time']), 'liters': row['liters']})
        if row['change sign'] != sign and len(temp) > 1:
            # call func
            if sign == 1.0:
                # refill
                data_refill = action_handler(temp)
                for_df.append(data_refill)
                temp = []
                sign = row['change sign']
            elif sign == -1.0:
                # consumption
                data_cons = action_handler(temp, consumption=True)
                for_df.append(data_cons)
                temp = []
                sign = row['change sign']
            elif sign == 0.0:
                temp = []
                sign = row['change sign']

    pd.DataFrame(for_df).to_csv('final1.csv')
    return "OK"
    # return (pd.to_datetime(row['timestamp']).timestamp() - pd.to_datetime(st_time).timestamp())/60


# print(one_ts_analyze("d28378aec6bd1214b87047f2af506f70"))


# return (pd.to_datetime(row['timestamp']).timestamp() - pd.to_datetime(st_time).timestamp())/60


# print(one_ts_analyze("d28378aec6bd1214b87047f2af506f70"))
