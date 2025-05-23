from sklearn.metrics import classification_report, f1_score
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from src import DataProcessor
from openai import OpenAI
from transformers import pipeline
import pandas as pd
import numpy as np
import argparse
import warnings
import logging
import json
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,    # 로그 레벨 설정 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app-test.log"),# mode='w'),  # 로그를 파일에 기록
        # logging.StreamHandler()  # 콘솔에도 출력
    ]
)
logging.basicConfig(filename='warnings.log', level=logging.WARNING)
logging.captureWarnings(True)

def main(args):
    load_dotenv() 
    openai_api = os.getenv('OPENAI_API_KEY')
    logger = logging.getLogger(__name__)
    if not openai_api:
        raise ValueError("IP 주소나 OpenAI API 키가 제대로 로드되지 않았습니다.")
        
    with open(os.path.join(args.config_path, args.llm_config)) as f:
        llm_config = json.load(f)
    data_p = DataProcessor()
    answer_data = pd.read_csv(os.path.join(args.data_path, 'ibk-0901-0909-testdata.csv'))
    answer = answer_data['human-answer'].values.tolist()
    print(answer_data.head(), end='\n\n')
    answer_data.content = answer_data.content.apply(lambda x: str(x))
    text_list = answer_data.content.values.tolist()
    classifier = pipeline('sentiment-analysis', model=os.path.join(llm_config['model_path'], 'kfdeberta', 'model-update'), device=0)
    if args.query == None:
        response = classifier(text_list)
        pred = ['o' if r['label'] == 'stock' else 'x' for r in response]
        report_dict = classification_report(answer, pred, target_names=['o', 'x'])
        logger.info('\n' + report_dict)
    else:
        pass    

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--llm_config', type=str, default='llm_config.json')
    cli_parser.add_argument('--data_path', type=str, default='./predict/')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    main(cli_args)