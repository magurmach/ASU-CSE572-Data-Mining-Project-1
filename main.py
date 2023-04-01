import csv
import pandas as pd
import numpy as np
from datetime import datetime

def hyperglycemia(row):
    if row['Sensor Glucose (mg/dL)'] > 180:
        return 1
    return 0


def hyperglycemia_critical(row):
    if row['Sensor Glucose (mg/dL)'] > 250:
        return 1
    return 0


def in_range(row):
    if row['Sensor Glucose (mg/dL)'] >=70 and row['Sensor Glucose (mg/dL)'] <= 180:
        return 1
    return 0


def in_range_secondary(row):
    if row['Sensor Glucose (mg/dL)'] >=70 and row['Sensor Glucose (mg/dL)'] <= 150:
        return 1
    return 0


def hypoglycemia_level_1(row):
    if row['Sensor Glucose (mg/dL)'] < 70:
        return 1
    return 0


def hypoglycemia_level_2(row):
    if row['Sensor Glucose (mg/dL)'] < 54:
        return 1
    return 0


def add_glucose_level(df: pd.DataFrame):
    df['g>180'] = df.apply(hyperglycemia, axis=1)
    df['g>250'] = df.apply(hyperglycemia_critical, axis=1)
    df['g-70-180'] = df.apply(in_range, axis=1)
    df['g-70-150'] = df.apply(in_range_secondary, axis=1)
    df['g<70'] = df.apply(hypoglycemia_level_1, axis=1)
    df['g<54'] = df.apply(hypoglycemia_level_2, axis=1)
    return df


def frame_date_time(row):
    date = row['Date'] + ' ' + row['Time']
    date_fmt = '%m/%d/%Y %H:%M:%S'
    return pd.Timestamp(datetime.strptime(date, date_fmt))


def add_date_time_column(df: pd.DataFrame):
    df['datetime'] = df.apply(frame_date_time, axis=1)
    return df


def delete_dates_with_nan(df: pd.DataFrame):
    df = df.drop(df[df['Sensor Glucose (mg/dL)'].isna()].index)
    return df


def cmg_data_frame() -> pd.DataFrame:
    ''' Returns a processed dataframe of CGM Data.

    CGM file contains a lot of columns without any non-nan value. Returns only values with at least one non-null entry.
    '''
    df = pd.read_csv('CGMData.csv', index_col='Index', low_memory=False)
    null_cols = df.columns[df.isnull().all()]
    df.drop(null_cols, axis=1, inplace=True)
    df = delete_dates_with_nan(df)
    df = df.replace({np.nan: None})
    df['Date'] = df['Date'].astype('string')
    df['Time'] = df['Time'].astype('string')
    df['Sensor Glucose (mg/dL)'] = df['Sensor Glucose (mg/dL)'].astype('int32')
    df = add_date_time_column(df)
    df = add_glucose_level(df)
    return df


def insulin_data_frame() -> pd.DataFrame:
    ''' Returns a processed dataframe of insulin data.

    Insulin file contains a lot of columns without any value. Returns only values with at least one non-null entry. Further column based processing is done.
    '''
    df = pd.read_csv('InsulinData.csv', index_col='Index', low_memory=False)
    null_cols = df.columns[df.isnull().all()]
    df.drop(null_cols, axis=1, inplace=True)
    df = df.replace({np.nan: None})
    df['Alarm'] = df['Alarm'].astype('string')
    df['Date'] = df['Date'].astype('string')
    df['Time'] = df['Time'].astype('string')
    df = add_date_time_column(df)
    return df


def earliest_auto_switch_time(insulin_data: pd.DataFrame) -> pd.Timestamp:
    '''Returns earliest time from insulin data which caused switching to auto mode.'''
    auto_mode_switch_marker = 'AUTO MODE ACTIVE PLGM OFF'
    data = insulin_data.loc[insulin_data['Alarm'] == auto_mode_switch_marker]
    return data['datetime'].min()


def divide_data_frame_in_manual_vs_auto(cgm_data: pd.DataFrame, manual_to_auto_ts: pd.Timestamp):
    '''Returns 2 data frame, first for manual, second for auto'''
    manual_data = cgm_data.loc[cgm_data['datetime'] <= manual_to_auto_ts]
    auto_data = cgm_data.loc[cgm_data['datetime'] > manual_to_auto_ts]
    return manual_data, auto_data


def metric_extraction(cgm_data: pd.DataFrame, total_event_cnt: int):
    cnt = len(cgm_data['Date'].unique())
    return [
        ((cgm_data.groupby(['Date'])['g>180'].sum() / total_event_cnt).sum() / cnt) * 100,
        ((cgm_data.groupby(['Date'])['g>250'].sum() / total_event_cnt).sum() / cnt) * 100,
        ((cgm_data.groupby(['Date'])['g-70-180'].sum() / total_event_cnt).sum() / cnt) * 100,
        ((cgm_data.groupby(['Date'])['g-70-150'].sum() / total_event_cnt).sum() / cnt) * 100,
        ((cgm_data.groupby(['Date'])['g<70'].sum() / total_event_cnt).sum() / cnt) * 100,
        ((cgm_data.groupby(['Date'])['g<54'].sum() / total_event_cnt).sum() / cnt) * 100
    ]


def time_based_extraction(cgm_data: pd.DataFrame):
    cgm_data = cgm_data.set_index('datetime')
    day_time_frame = cgm_data.between_time('06:00:00', '23:59:59')
    over_night_frame = cgm_data.between_time('00:00', '06:00')

    data = []
    data.extend(metric_extraction(over_night_frame, 288))
    data.extend(metric_extraction(day_time_frame, 288))
    data.extend(metric_extraction(cgm_data, 288))

    return data



def main():
    cgm_data = cmg_data_frame()
    insulin_data = insulin_data_frame()
    earliest_switchover_timestamp = earliest_auto_switch_time(insulin_data)
    manual_data, auto_data = divide_data_frame_in_manual_vs_auto(cgm_data, earliest_switchover_timestamp)
    metric = []
    metric.append(time_based_extraction(manual_data))
    metric.append(time_based_extraction(auto_data))

    with open('Result.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(metric)
    

if __name__ == '__main__':
    main()