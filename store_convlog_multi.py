from src import EnvManager, PreProcessor, DBManager, PipelineController
from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd
import argparse
import logging
import os

logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("store.log"),# mode='w'),  # 로그를 파일에 기록
        # logging.StreamHandler()  # 콘솔에도 출력
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

    start_date = datetime(2025, 7, 31)
    end_date = datetime(2025, 8, 30)
    delta = timedelta(days=1)

    current_date = start_date
    while current_date <= end_date:
        yy = str(current_date.year)
        mm = str(current_date.month).zfill(2)
        dd = str(current_date.day).zfill(2)
        crawling_date = yy + mm + dd
        input_data = pd.read_excel(os.path.join(args.data_path, f"ibk-convlog_" + crawling_date + '.xlsx'))
        print(crawling_date)
        
        input_data = input_data[['date', 'q/a', 'content', 'user_id']]
        conv_ids = []  
        for idx in tqdm(range(len(input_data))):    # 챗봇 대화 로그 데이터에 PK 추가 
            date_value = input_data['date'][idx]
            pk_date = f"{str(date_value.year)}{str(date_value.month).zfill(2)}{str(date_value.day).zfill(2)}"
            conv_id = pk_date + '_' + str(idx).zfill(5)
            conv_ids.append(conv_id)
        input_data.insert(0, 'conv_id', conv_ids)
        
        for idx in tqdm(range(len(input_data))):   # PostgreSQL 테이블에 데이터 저장
            if pipe.postgres.check_pk(pipe.env_manager.conv_tb_name, input_data['conv_id'][idx]):   # 데이터 존재 여부 확인
                logger.info(f"해당 파일이 이미 존재합니다: {input_data['conv_id'][idx]}")
                continue
            data_set = tuple(input_data.iloc[idx].values)
            pipe.table_editor.edit_conv_table('insert', pipe.env_manager.conv_tb_name, data_type='raw', data=data_set)
        current_date += delta
    pipe.postgres.db_connection.close()

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--data_path', type=str, default='data/')
    cli_parser.add_argument('--file_name', type=str, default='conv_log-0705-0902.xlsx')
    cli_parser.add_argument('--config_path', type=str, default='config/')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--process', type=str, default='daily')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    main(cli_args)
