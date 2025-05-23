from transformers import AutoModel, AutoModelForSequenceClassification, AutoTokenizer
from dotenv import load_dotenv
from src import KFDeBERTa, KFDeBERTaTokenizer
import datetime as dt
import pandas as pd
import numpy as np 
import argparse 
import warnings
import logging
import time
import json
import io
import os

def main(args):
    load_dotenv()
    openai_api = os.getenv('OPENAI_API_KEY')
    logger = logging.getLogger(__name__)
    if not openai_api:
        raise ValueError("OpenAI API 키가 제대로 로드되지 않았습니다.")
    with open(os.path.join(args.config_path, 'llm_config.json')) as f:
        llm_config = json.load(f)

    kf_tokenizer = KFDeBERTaTokenizer(os.path.join(llm_config['model_path'], 'kfdeberta', 'best_model'))
    text = '출석'
    tickles = pd.read_csv(os.path.join('./data','tickle', 'tickle-final.csv'))
    tickles.dropna(inplace=True)
    tickles.reset_index(inplace=True, drop=True)
    tickle_list = tickles['tickle'].values.tolist()
    print(f'토크나이저 업데이트 전: {kf_tokenizer.tokenize_data(text)}')
    kf_tokenizer.tokenizer.add_tokens(tickle_list)
    kf_tokenizer.save_tokenizer('./new_tokenizer')
    new_tokenizer = AutoTokenizer.from_pretrained('./new_tokenizer')
    print(f'토크나이저 업데이트 후: {(new_tokenizer.tokenize(text))}')


if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='config/')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    main(cli_args)