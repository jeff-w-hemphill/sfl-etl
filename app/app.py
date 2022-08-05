import pandas as pd
from sqlalchemy import create_engine
import time

def main():
    raw_data = extract('./data/DATA.csv')
    transformed_data = transform(raw_data)
    
    # database params
    db_name = 'database'
    db_user = 'username'
    db_pass = 'secret'
    db_host = 'db'
    db_port = '5432'
    
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
    
    load(db_string, transformed_data)
    
    return 0


def extract(file):
    return pd.read_csv(file)

def transform(df):
    '''Takes in the raw dataframe and returns a tuple of the original df and the 2 new dfs - people and genders'''
    # find unique genders
    unique_genders = df['gender'].unique()

    # create genders dictionary to efficiently create gender_id column
    genders = {}
    for i, gender, in enumerate(unique_genders):
        genders[gender] = i + 1

    # create gender_ids list to add to df
    gender_ids = []
    for gender in df['gender'].array:
        gender_ids.append(genders[gender])

    # add gender_id column
    df['gender_id'] = gender_ids

    # create genders df table
    df_genders = df[['gender_id', 'gender']].drop_duplicates()

    # create people df table by dropping gender column
    df_people = df.drop(['gender'], axis=1)

    return df, df_genders, df_people

def load(db_string, dfs):
    '''Loads in the 2 supplied dataframes into the database'''
    df = dfs[0]
    df_genders = dfs[1]
    df_people = dfs[2]
    
    # Connect to the database
    while True:
        try:
            db = create_engine(db_string)

            # load database
            df.to_sql('raw_people', con=db, if_exists="replace", index=False)
            df_genders.to_sql('genders', con=db, if_exists='replace', index=False)
            df_people.to_sql('people', con=db, if_exists='replace', index=False)
            print('success')
            break
        except:
            print('waiting on postgres')
            time.sleep(1)
        
main()
    
