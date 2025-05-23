from src import EnvManager, PreProcessor, DBManager, PipelineController
from apscheduler.schedulers.background import BackgroundScheduler
from tqdm import tqdm
import pandas as pd
import argparse 
import logging
import time
import schedule
import os

logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),# mode='w'),  # 로그를 파일에 기록
    ]
)
logging.basicConfig(filename='warnings.log', level=logging.WARNING)
logging.captureWarnings(True)

def main(args):
    logger = logging.getLogger(__name__)
    env_manager = EnvManager(args)
    preprocessor = PreProcessor()
    db_manager = DBManager(env_manager.db_config)
    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager)   
    pipe.set_env()

    yy, mm, dd = pipe.time_p.get_current_date()
    crawling_date = yy + mm + dd     # 20240301, 20241021 ... 
    try:
        filename = f'ibk-convlog_' + crawling_date + '.xlsx'
        input_data = pd.read_excel(os.path.join(args.data_path, filename))
    except:
        logger.info(f"해당 파일이 경로에 없습니다.")
        
    input_data = input_data[['date', 'q/a', 'content', 'user_id']]
    input_data.sort_values(by='date', inplace=True)
    input_data.reset_index(inplace=True, drop=True)
    print(f'len input: {len(input_data)}')
    conv_ids = []
    for idx in range(len(input_data)):   # 챗봇 대화 로그 데이터에 PK 추가 
        date_value = input_data['date'][idx]
        pk_date = f"{str(date_value.year)}{str(date_value.month).zfill(2)}{str(date_value.day).zfill(2)}"
        conv_id = pk_date + '_' + str(idx).zfill(5)
        conv_ids.append(conv_id)
    print(f'len conv_ids: {len(conv_ids)}')
    print(f'conv_ids: {conv_ids[:5]}')
    input_data.insert(0, 'conv_id', conv_ids)
    print(f'len input after: {len(input_data)}')
    for idx in range(len(input_data)):   # PostgreSQL 테이블에 데이터 저장
        if pipe.postgres.check_pk(pipe.env_manager.conv_tb_name, input_data['conv_id'][idx]):   # 데이터 존재 여부 확인
            # print(f"exist: {input_data['conv_id'][idx]}")
            # logger.info(f"해당 파일이 이미 존재합니다: {input_data['conv_id'][idx]}")
            continue
        data_set = tuple(input_data.iloc[idx].values)
        pipe.table_editor.edit_conv_table('insert', pipe.env_manager.conv_tb_name, data_type='raw', data=data_set)            
    pipe.postgres.db_connection.close()

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--data_path', type=str, default='./data/')
    cli_parser.add_argument('--file_name', type=str, default='conv_log-0705-0902.xlsx')
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--process', type=str, default='daily')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()

    scheduler = BackgroundScheduler()
    scheduler.add_job(main, 'cron', minute=2, args=[cli_args])
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.shutdown()
    
