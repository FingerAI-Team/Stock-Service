from src import EnvManager, PreProcessor, DBManager, APIPipeline, PipelineController
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv
import argparse 
import logging
import schedule
import time
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
    api_pipeline = APIPipeline(bearer_tok=env_manager.bearer_token) 

    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager)   
    pipe.set_env()

    if args.process == 'code-test':   # 저장할 파일명 지정   
        if args.file_name.split('.')[-1] == 'csv': 
            input_data = pd.read_csv(os.path.join(args.data_path, args.file_name))
        elif args.file_name.split('.')[-1] == 'xlsx':
            input_data = pd.read_excel(os.path.join(args.data_path, args.file_name))
    elif args.process == 'daily':    # 매일 12시 10분에 전일 데이터 저장
        yy, mm, dd = pipe.time_p.get_previous_day_date()
        crawling_date = yy + mm + dd     # 20240301, 20241021 ... 
        api_data = api_pipeline.get_data(date=crawling_date, tenant_id='ibk')
        input_data = api_pipeline.process_data(api_data)
        print(input_data.head())

    input_data = input_data[['date', 'q/a', 'content', 'user_id']]
    conv_ids = []
    for idx in tqdm(range(len(input_data))):   # 챗봇 대화 로그 데이터에 PK 추가 
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
    
    '''schedule.every().day.at("05:10").do(main, cli_args)
    while True:   # 스케줄러를 유지하는 루프
        schedule.run_pending()   # 대기 중인 작업 실행
        time.sleep(1)'''
    main(cli_args)