from src import EnvManager, PreProcessor, DBManager, ModelManager, LLMManager, PipelineController
import argparse 
import logging
import schedule
import time
import sys
import os
import re 

logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ner-model.log"),# mode='w'),  # 로그를 파일에 기록
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
    
    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager)   
    pipe.set_env()
    tickles = pipe.env_manager.tickle_list
    print(tickles[:3])
    data = pipe.postgres.get_total_data(pipe.env_manager.conv_tb_name)
    data_df = pipe.data_p.data_to_df(data, ['conv_id', 'date', 'qa', 'content', 'user_id'])
    print(data_df[data_df['qa'] == 'Q'].head())

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='/ibk/service/config/')
    cli_parser.add_argument('--process', type=str, default='daily')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    main(cli_args)