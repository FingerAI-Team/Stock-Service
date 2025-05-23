from src import EnvManager, PreProcessor, DBManager, ModelManager, LLMManager, PipelineController
import argparse 
import schedule
import time

def main(args):
    env_manager = EnvManager(args)
    preprocessor = PreProcessor()
    db_manager = DBManager(env_manager.db_config)
    model_manager = ModelManager(env_manager.model_config)
    llm_manager = LLMManager(env_manager.model_config)
    
    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager, model_manager=model_manager, llm_manager=llm_manager)
    pipe.set_env()
    pipe.run(process=args.process, query=args.query)


if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--process', type=str, default='daily')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    
    '''schedule.every().day.at("05:15").do(main, cli_args)
    while True:
        schedule.run_pending()  # 대기 중인 작업 실행
        time.sleep(1)'''
    main(cli_args)
