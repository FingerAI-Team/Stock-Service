from src import EnvManager, PreProcessor, DBManager, ModelManager, LLMManager, PipelineController 
from apscheduler.schedulers.blocking import BlockingScheduler
from scheduler_config import get_schedule_config
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('main.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main(args):
    try:
        logger.info("=== Main Pipeline 시작 ===")
        env_manager = EnvManager(args)
        preprocessor = PreProcessor()
        db_manager = DBManager(env_manager.db_config)
        model_manager = ModelManager(env_manager.model_config)
        llm_manager = LLMManager(env_manager.model_config)
        
        pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager, model_manager=model_manager, llm_manager=llm_manager)
        pipe.set_env()
        pipe.run(process=args.process, query=args.query)
        logger.info("=== Main Pipeline 완료 ===")       
    except Exception as e:
        logger.error(f"Main Pipeline 실행 중 오류 발생: {str(e)}")
        raise

def run_scheduled():
    """스케줄된 작업 실행"""
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--process', type=str, default='code-test')  # 전체 데이터 저장 프로세스
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    main(cli_args)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        # 한 번만 실행
        cli_parser = argparse.ArgumentParser()
        cli_parser.add_argument('--config_path', type=str, default='./config/')
        cli_parser.add_argument('--process', type=str, default='daily')
        cli_parser.add_argument('--task_name', type=str, default='cls')
        cli_parser.add_argument('--query', type=str, default=None)
        cli_args = cli_parser.parse_args()
        main(cli_args)
    else:
        # 스케줄러로 매 시간 실행
        scheduler = BlockingScheduler()
        schedule_config = get_schedule_config('hourly_6min')    
        scheduler.add_job(
            run_scheduled,
            trigger=schedule_config['trigger'],
            id='main_pipeline_hourly',
            name=f"Main Pipeline {schedule_config['description']}",
            replace_existing=True
        )
        
        logger.info("데이터 수집은 매 정시 5분, 종목 예측은 매 정시 6분에 진행됩니다")
        logger.info(f"Main Pipeline 스케줄러 시작 - {schedule_config['description']}")
        logger.info("한 번만 실행하려면: python main.py --once")
        try:
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("스케줄러가 중단되었습니다")
            scheduler.shutdown()