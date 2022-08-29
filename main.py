import pandas as pd
import logging
import re
import boto3
from io import BytesIO
from sqlalchemy import create_engine
from utils import *
pd.options.mode.chained_assignment = None 

# PARAMETERS
ACCESS_KEY=''
SECRET_KEY=''
SESSION_TOKEN=''
file = 'FIFA22_official_data.csv'
bucket_name = ''
format_ = ''
# DB connections
database = ''
user = ''
host = ''
password_ = ''  
port = 5432


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, 'INFO'))
    
    logger.info('='*40)
    logger.info('-- Proyecto Final Juan Castro --')
    logger.info('='*40)
    
    df = pd.read_csv(file)
    logger.info('-- Loading Data')
    logger.info('File: '+ file)
    logger.info(f'Shape: {df.shape}')
    
    logger.info('-- Subseting DataFrame')
    subset_ = ['ID', 'Name', 'Age', 'Nationality', 'Overall', 'Club','Value','Wage','Preferred Foot','Position','Height', 'Weight']
    df = df[subset_]
    logger.info(f'Columns: {subset_}')
    logger.info(f'New Shape: {df.shape}')
    
    logger.info('-- Searching Missing Values')
    logger.info(f'Is There NA: {df.isnull().values.any()}')
    logger.info(f'Total: {df.isnull().sum().sum()}')
    
    logger.info('Deleting Nan')
    df = df.dropna()
    logger.info(f'Is There NA: {df.isnull().values.any()}')
    logger.info(f'Final Shape: {df.shape}')
    
    logger.info('Lowering Column Names')
    df.columns = df.columns.str.strip().str.lower()
    logger.info('Columns: {}'.format(df.columns))
    
    logger.info('-- Transforming Data')
    df = transform_cols(df)
    
    logger.info('-- Creating Nationality Aggregate Table')
    nationality_table = aggregate_table(df, 'nationality')
    logger.info('-- Creating Club Aggregate Table')
    club_table = aggregate_table(df, 'club')
    logger.info('-- Creating Age Aggregate Table')
    age_table = aggregate_table(df, 'age')
    logger.info('-- Creating Age Position Table')
    position_table = aggregate_table(df, 'new_position')
    
    logger.info('-- Getting client to AWS')
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN
    )
    
    strg_ = f'postgresql://{user}:{password_}@{host}:{port}/{database}'
    logger.info('-- Creating Postgresql Connection')
    logger.info(f'-- Connection url: {strg_}')
    engine = create_engine(strg_)
    
    logger.info('Uploading Table nationality_table')
    filepath = 'final/nationality_table.parquet'
    dataframe_to_s3(client, nationality_table, bucket_name, filepath, format_)
    nationality_table.to_sql('nationality_table', engine, if_exists='replace')
    
    logger.info('Uploading Table club_table')
    filepath = 'final/club_table.parquet'
    dataframe_to_s3(client, club_table, bucket_name, filepath, format_)
    club_table.to_sql('club_table', engine, if_exists='replace')
    
    logger.info('Uploading Table age_table')
    filepath = 'final/age_table.parquet'
    dataframe_to_s3(client, age_table, bucket_name, filepath, format_)
    age_table.to_sql('age_table', engine, if_exists='replace')
    
    logger.info('Uploading Table position_table')
    filepath = 'final/position_table.parquet'
    dataframe_to_s3(client, position_table, bucket_name, filepath, format_)
    position_table.to_sql('position_table', engine, if_exists='replace')
    
    logger.info('='*40)
