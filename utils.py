import pandas as pd
import logging
import re
import boto3
from io import BytesIO
from sqlalchemy import create_engine

logging.basicConfig(
  format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger()
logger.setLevel(getattr(logging, 'INFO'))

def transform_cols(df2):
    s = '.'
    value_ = []
    wage_ = []
    height_ = []
    weight_ = []
    position_ = []

    for elt in df2['value']:
      mm = re.findall(r'\d+', elt)
      value_.append(float(s.join(mm)))

    for elt in df2['wage']:
      mm = re.findall(r'\d+', elt)
      wage_.append(float(s.join(mm)))

    for elt in df2['height']:
      mm = re.findall(r'\d+', elt)
      height_.append(float(s.join(mm)))

    for elt in df2['weight']:
      mm = re.findall(r'\d+', elt)
      weight_.append(float(s.join(mm)))

    for elt in df2['position']:
      mm = re.sub('[^A-Z]', '', elt)
      mm = mm.replace('.', '')
      position_.append(mm)

    logger.info('NewCol: new_value')
    logger.info('NewCol: new_wage')
    logger.info('NewCol: new_height')
    logger.info('NewCol: new_weight')
    logger.info('NewCol: new_position')
    df2['new_value'] = value_
    df2['new_wage'] = wage_
    df2['new_height'] = height_
    df2['new_weight'] = weight_
    df2['new_position'] = position_
    return df2

def dataframe_to_s3(s3_client, input_datafame, bucket_name, filepath, format):
        if format == 'parquet':
            out_buffer = BytesIO()
            input_datafame.to_parquet(out_buffer, index=False)
        elif format == 'csv':
            out_buffer = StringIO()
            input_datafame.to_parquet(out_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())

def aggregate_table(df, column):
    table = df.groupby([column]).mean()['new_wage'].to_frame()
    table['min_wage'] = df.groupby([column]).min()['new_wage']
    table['max_wage'] = df.groupby([column]).max()['new_wage']

    table['mean_value'] = df.groupby([column]).mean()['new_value']
    table['min_value'] = df.groupby([column]).min()['new_value']
    table['max_value'] = df.groupby([column]).max()['new_value']

    table['mean_overall'] = df.groupby([column]).mean()['overall']
    table['min_overall'] = df.groupby([column]).min()['overall']
    table['max_overall'] = df.groupby([column]).max()['overall']

    table['mean_height'] = df.groupby([column]).mean()['new_height']
    table['min_height'] = df.groupby([column]).min()['new_height']
    table['max_height'] = df.groupby([column]).max()['new_height']

    table['mean_weight'] = df.groupby([column]).mean()['new_weight']
    table['min_weight'] = df.groupby([column]).min()['new_weight']
    table['max_weight'] = df.groupby([column]).max()['new_weight']

    right_ = []
    left_ = []
    nations = df[column].unique()

    for pais in nations:
      left_.append(len(df[(df[column]== pais) & (df['preferred foot']=='Left')]))
      right_.append(len(df[(df[column]== pais) & (df['preferred foot']=='Right')]))

    table['right_foot'] = right_
    table['left_foot'] = left_
    return table