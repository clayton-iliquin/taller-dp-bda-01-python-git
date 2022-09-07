

# main.py
# ==================================================
# common
import os
import shutil
import json
from uuid import uuid4
from pathlib import Path
# requirements
import requests
import pendulum
import pandas as pd
import fire
# user defined
from modules.utils.logger import get_logger, get_inspect
# --------------------------------------------------

# => logger object
logger = get_logger()

# obtenemos la fecha
def current_date(region_city: str='America/Lima') -> str:
    c_date = pendulum.now(region_city).to_date_string()
    return c_date

# 
def request_data(api_url: str, headers: dict) -> dict:
    logger.info(get_inspect())
    
    try:
        response = requests.get(url=api_url, headers=headers, timeout=60)
        response.encoding = 'utf-8'
        api_data = json.loads(response.text)
    except Exception as e:
        print('[INFO] something went wrong...')
        raise
    return api_data

# 
def make_table(api_data: dict, serie: str, record_path: str='periods') -> 'pandas.DataFrame':
    logger.info(get_inspect())
    
    df = (
        pd
        .json_normalize(api_data, record_path=record_path)
        .rename(columns={'name': 'YEAR', 'values': serie.upper()})
    )
    df[serie] = df[serie].str[0].astype('float')
    return df

def save_table(df: 'pandas.DataFrame', save_path: str) -> bool:
    logger.info(get_inspect())
    
    save_folder = '/'.join(save_path.split('/')[:-1])
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    
    dfc = df.copy(deep=True)
    dfc.to_csv(save_path, sep=';', encoding='iso-8859-1', index=False)
    return True

def append_table(read_path: str) -> 'pandas.DataFrame':
    logger.info(get_inspect())
    
    curr_date = current_date() ; year = curr_date.split('-')[0]
    tdf = pd.DataFrame({'YEAR': range(1940, int(year)+1), 'LOAD_DATE': curr_date})
    tdf = tdf.set_index('YEAR')
    
    files = os.listdir(read_path)
    for f in files:
        df = pd.read_csv(f'{read_path}/{f}', sep=';', encoding='iso-8859-1')
        df = df.set_index('YEAR')
        tdf = tdf.join(df)
    
    tdf = tdf.reset_index()
    tdf = tdf.dropna(subset=tdf.columns[2:])
    return tdf

def export_table(df: 'pandas.DataFrame', export_path: str, **kwargs) -> bool:
    logger.info(get_inspect())
    
    sep = kwargs.get('sep', ';')
    encoding = kwargs.get('encoding', 'iso-8859-1')
    save_folder = '/'.join(export_path.split('/')[:-1])
        
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    df.to_csv(export_path, sep=sep, encoding=encoding, index=False)
    return True

def historic_exec(current_path: str, historic_path: str) -> bool:
    logger.info(get_inspect())
    
    shutil.move(current_path, historic_path)
    return True

def main(params: str=None):
    logger.info(f'''\n{15*'<>'} MAIN EXECUTION {15*'<>'}''')
    
    if params is None:
        raise Exception('[RAISED] "params" argument is required...')
    
    with open(params, 'r') as pfile:
        json_params = json.load(pfile)
    
    url = json_params['url']
    headers = json_params['headers']
    series = json_params['series']
    
    curr_date = current_date()
    year = curr_date.split('-')[0]
    exec_uuid = str(uuid4())
    logger.info(f'EXECUTION-UUID: {exec_uuid}')
    
    for s in series:
        logger.info(f'SERIE-NAME: {s}')
        
        save_path = f'./data/current/{curr_date}/{exec_uuid}/{s}.csv'
        api_url = (
            url
            .replace('$SERIE', s)
            .replace('$YEAR', year)
        )
        api_data = request_data(api_url, headers)
        df = make_table(api_data, s)
        _ = save_table(df, save_path)
        logger.info(f'COMPLETED-SERIE: {s}')
    
    read_path = '/'.join(save_path.split('/')[:-1])
    export_path = f'./data/output/{curr_date}/output_{exec_uuid}.csv'
    
    pdf = append_table(read_path)
    _ = export_table(pdf, export_path, sep='|')
    _ = historic_exec(read_path, read_path.replace('current', 'historic'))

if __name__ == '__main__':
    fire.Fire(main)
