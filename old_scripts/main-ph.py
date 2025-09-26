from src import EnvManager, PreProcessor, DBManager, ModelManager, LLMManager, PipelineController
from apscheduler.schedulers.background import BackgroundScheduler 
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

    scheduler = BackgroundScheduler()
    scheduler.add_job(main, 'cron', minute=3, args=[cli_args])
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.shutdown()
