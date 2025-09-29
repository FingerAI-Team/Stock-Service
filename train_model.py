from src import EnvManager, PreProcessor, DBManager, ModelManager, LLMManager, PipelineController
from sklearn.model_selection import train_test_split
from datasets import Dataset, DatasetDict
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
import numpy as np 
import argparse
import logging
import schedule
import evaluate
import time
import os

logging.basicConfig(
    level=logging.INFO,  # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),# mode='w'),  # 로그를 파일에 기록
        # logging.StreamHandler()  # 콘솔에도 출력
    ]
)
logging.basicConfig(filename='warnings.log', level=logging.WARNING)
logging.captureWarnings(True)

def main(args):
    env_manager = EnvManager(args)
    preprocessor = PreProcessor()
    db_manager = DBManager(env_manager.db_config)
    model_manager = ModelManager(env_manager.model_config)
    tok_class, _ = model_manager.set_encoder(os.path.join(env_manager.model_config['model_path'], 'kfdeberta', 'model-update'))
    llm_manager = LLMManager(env_manager.model_config)
    
    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager, model_manager=model_manager, llm_manager=llm_manager)
    pipe.set_env()
    c_yy, c_mm, c_dd = pipe.time_p.get_current_date()
    current_date = c_yy + c_mm + c_dd 
    
    convlog_data = pipe.postgres.get_total_data(pipe.env_manager.conv_tb_name)
    cls_data = pipe.postgres.get_total_data(pipe.env_manager.cls_tb_name)
    stock_dict = model_manager.set_cls_trainset(convlog_data, cls_data, pipe.data_p)
    tokenized_stock = stock_dict.map(tok_class.tokenize_data, batched=True)
    trainer = model_manager.initialize_trainer(os.path.join(env_manager.model_config['model_path'], 'kfdeberta', 'model-update'), \
                                                        env_manager.model_config, tokenized_stock)
    trainer.train()
    save_model_path = os.path.join(env_manager.model_config['model_path'], 'kfdeberta', 'model-update')
    log_model_path = os.path.join(env_manager.model_config['model_path'], 'kfdeberta', f'trained-model_{current_date}')
    trainer.save_model(save_model_path)
    trainer.save_model(log_model_path)
    
if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='config/')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    '''schedule.every().monday.at("00:30").do(main, cli_args)
    while True:
        schedule.run_pending()  # 대기 중인 작업 실행
        time.sleep(1)  # 1초마다 체크'''
    main(cli_args)