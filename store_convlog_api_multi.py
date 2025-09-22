from src import EnvManager, PreProcessor, DBManager, APIPipeline, PipelineController
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv
import argparse 
import logging
import time
import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from scheduler_config import get_schedule_config, print_available_schedules

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
        start_date = "2025-09-16"
        end_date = "2025-09-21"
        api_data = api_pipeline.get_data_range(start_date=start_date, end_date=end_date, tenant_id='ibk')        
        if api_data:
            print(f"첫 번째 데이터 샘플: {api_data[0] if api_data else 'None'}")
        
        input_data = api_pipeline.process_data(api_data)
        print(f"처리된 데이터 shape: {input_data.shape}")        
        if input_data.empty:
            print("❌ 처리된 데이터가 비어있습니다. 다른 날짜를 시도해보세요.")
            return
        else:
            print(input_data.head())

    input_data = input_data[['date', 'q/a', 'content', 'user_id', 'tenant_id']]
    conv_ids = []
    for idx in tqdm(range(len(input_data))):   # 챗봇 대화 로그 데이터에 PK 추가 
        date_str = input_data['date'][idx]
        # 날짜 문자열을 datetime 객체로 변환
        if isinstance(date_str, str):
            date_value = datetime.fromisoformat(date_str)
        else:
            date_value = date_str
        pk_date = f"{str(date_value.year)}{str(date_value.month).zfill(2)}{str(date_value.day).zfill(2)}"
        
        # 더 안전한 conv_id 생성: 날짜 + 사용자ID + 내용 해시
        import hashlib
        content_hash = hashlib.md5(
            f"{input_data['user_id'][idx]}_{input_data['content'][idx]}_{input_data['q/a'][idx]}".encode()
        ).hexdigest()[:8]
        conv_id = f"{pk_date}_{input_data['user_id'][idx]}_{content_hash}"
        conv_ids.append(conv_id)
    input_data.insert(0, 'conv_id', conv_ids)
    
    # 중복 저장 방지 통계
    total_records = len(input_data)
    existing_records = 0
    new_records = 0
    
    for idx in tqdm(range(len(input_data))):   # PostgreSQL 테이블에 데이터 저장
        if pipe.postgres.check_pk(pipe.env_manager.conv_tb_name, input_data['conv_id'][idx]):   # 데이터 존재 여부 확인
            existing_records += 1
            logger.info(f"이미 존재하는 데이터: {input_data['conv_id'][idx]}")
            continue
        
        new_records += 1
        data_set = tuple(input_data.iloc[idx].values)
        pipe.table_editor.edit_conv_table('insert', pipe.env_manager.conv_tb_name, data_type='raw', data=data_set)
    
    # 저장 결과 요약
    print(f"\n📊 데이터 저장 결과:")
    print(f"   전체 레코드: {total_records}")
    print(f"   새로 저장된 레코드: {new_records}")
    print(f"   이미 존재하는 레코드: {existing_records}")
    print(f"   중복률: {(existing_records/total_records*100):.1f}%" if total_records > 0 else "   중복률: 0%")            
    pipe.postgres.db_connection.close()

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--data_path', type=str, default='./data/')
    cli_parser.add_argument('--file_name', type=str, default='conv_log-0705-0902.xlsx')
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--process', type=str, default='daily', 
                           help='실행 모드: daily(일회성), scheduled(스케줄링)')
    cli_parser.add_argument('--schedule_type', type=str, default='hourly',
                           help='스케줄 타입: hourly, daily, every_30min, every_15min, business_hours')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    
    # 스케줄링 모드 확인
    if cli_args.process == 'scheduled':
        # 사용 가능한 스케줄 옵션 출력
        print_available_schedules()
        
        # 스케줄 설정 가져오기
        schedule_config = get_schedule_config(cli_args.schedule_type)
        print(f"\n✅ 선택된 스케줄: {schedule_config['description']}")
        
        # APScheduler를 사용한 스케줄링
        scheduler = BackgroundScheduler()
        
        # 스케줄된 작업 추가
        scheduler.add_job(
            func=main,
            trigger=schedule_config['trigger'],
            args=[cli_args],
            id='data_collection_job',
            name=f'데이터 수집 ({cli_args.schedule_type})',
            replace_existing=True,
            max_instances=1  # 동시 실행 방지
        )
        
        # 스케줄러 시작
        scheduler.start()
        logger = logging.getLogger(__name__)
        logger.info(f"🕐 APScheduler가 시작되었습니다. {schedule_config['description']}")
        logger.info("📅 예정된 작업들:")
        for job in scheduler.get_jobs():
            logger.info(f"   - {job.name}: {job.next_run_time}")
        
        try:
            # 메인 스레드 유지
            while True:
                time.sleep(60)  # 1분마다 체크
        except KeyboardInterrupt:
            logger.info("⏹️ 스케줄러를 종료합니다...")
            scheduler.shutdown()
    else:
        # 일회성 실행
        main(cli_args)