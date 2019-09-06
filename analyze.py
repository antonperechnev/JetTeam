import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import json
import numpy as np

engine = create_engine('postgresql://postgres:Anton1995@localhost/allerts')

df = pd.read_csv('vh.csv', parse_dates=['timestamp'], index_col=False)
# df[df['id'] == "988c80e0861e328d70d1a9b4860c51f5"].to_csv('one_vh.csv', date_format='%Y-%m-%d %H:%M:%S', index=False)

# разница между временами
# df['time delta'] = pd.to_datetime(df['unix time']).diff().fillna(0).shift(-1).fillna(0)


def create_ids_json():
    unique_id = df['id'].unique().tolist()
    with open('unique_id.json', 'w') as f:
        json.dump(unique_id, f)
    return len(unique_id)


def action_handler(data: list, consumption=0):
    temp_df = pd.DataFrame(data)

    start_period_date = temp_df['time'].min()
    min_date = start_period_date  # .timestamp()
    max_date = temp_df['time'].max()  # .timestamp()

    liters_delta = temp_df['liters'].max() - temp_df['liters'].min()
    action = 'refill'

    if consumption:
        liters_delta = - liters_delta
        action = 'consumption'

    duration = max_date - min_date
    return {'start period': start_period_date, 'duration': duration, 'liters delta': liters_delta, 'action': action}


# not work
def ts_analyze(c_id):
    one_car = df[df['id'] == c_id]
    one_car['time delta'] = one_car['timestamp'].diff().fillna(0).shift(-1).fillna(0)
    one_car['change liters'] = one_car['liters'].diff().fillna(0).shift(-1).fillna(0)
    consum = one_car[(one_car['change liters'] <= 2) | (one_car['change liters'] >= 2)]
    return consum.head()


# print(ts_analyze("d28378aec6bd1214b87047f2af506f70"))


def one_ts_analyze(c_id):
    for_df = []
    consum = []
    refill = []
    rest = []

    one_car = df.loc[(df['id'] == c_id) & (df['liters'] != 0)].drop_duplicates('timestamp')
    one_car['time delta'] = one_car['timestamp'].diff().fillna(0).shift(-1).fillna(0)

    one_car['change liters'] = one_car['liters'].diff().fillna(0).shift(-1).fillna(0)
    one_car['change liters past'] = one_car['liters'].diff().fillna(0)

    # обработка состояния
    for ind, row in one_car.iterrows():
        if -2 <= row['change liters past'] <= 2:
            if refill:
                for_df.append(action_handler(refill))
                refill = []
            consum.append({'time': row['timestamp'], 'liters': row['liters']})
        # refill
        if (30 > row['change liters'] > 2) or (30 > row['change liters past'] > 2):
            if len(consum) > 1:
                for_df.append(action_handler(consum, consumption=1))
                consum = []
            refill.append({'time': row['timestamp'], 'liters': row['liters']})
        if abs(row['change liters']) >= 30 and row['time delta'] > pd.Timedelta(minutes=15):
            rest.append('|')
            rest.append({'time': row['timestamp'], 'liters': row['liters']})
            continue

    # pd.DataFrame(for_df).to_csv('final_new.csv', index=False)
    return for_df
    # return (pd.to_datetime(row['timestamp']).timestamp() - pd.to_datetime(st_time).timestamp())/60


def analyze_all():
    to_df = []
    with open('unique_id.json') as f:
        ids_list = json.load(f)
    for vh_id in ids_list:
        data = one_ts_analyze(vh_id)
        print(vh_id)
        to_df.extend(data)
    pd.DataFrame(to_df).to_csv('final_new.csv', index=False)


if __name__ == '__main__':
    print(analyze_all())
    # print(one_ts_analyze("d28378aec6bd1214b87047f2af506f70"))


