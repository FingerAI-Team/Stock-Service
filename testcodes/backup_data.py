from src import EnvManager, PreProcessor, DBManager, PipelineController
from dotenv import load_dotenv
import pandas as pd
import argparse 
import os

def main(args):
    env_manager = EnvManager(args)
    preprocessor = PreProcessor()
    db_manager = DBManager(env_manager.db_config)
    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager)   
    pipe.set_env()

    tb_name = 'ibk_convlog'
    tb_name2 = 'ibk_clicked_tb'
    tb_name3 = 'ibk_stock_cls'

    res = pipe.postgres.get_total_data(tb_name)
    res2 = pipe.postgres.get_total_data(tb_name2)
    res3 = pipe.postgres.get_total_data(tb_name3)
    conv_df = pd.DataFrame(res, columns=['conv_id', 'date', 'qa', 'content', 'user_id'])
    clicked_df = pd.DataFrame(res2, columns=['conv_id', 'clicked', 'user_id'])
    cls_df = pd.DataFrame(res3, columns=['conv_id', 'ensemble', 'gpt_res', 'enc_res'])
    conv_df.to_csv(os.path.join('./postgres-backup', 'ibk_convlog.csv'), index=False)
    clicked_df.to_csv(os.path.join('./postgres-backup', 'ibk_clicked_tb.csv'), index=False)
    cls_df.to_csv(os.path.join('./postgres-backup', 'ibk_stock_cls.csv'), index=False)
    print(len(conv_df), len(clicked_df), len(cls_df))
    

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--data_path', type=str, default="/ibk/data/")
    cli_parser.add_argument('--config_path', type=str, default="/ibk/config/")
    cli_parser.add_argument('--task_name', type=str, default="cls")
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    main(cli_args)
