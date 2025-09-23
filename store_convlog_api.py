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

# 로깅 설정 - 파일 핸들러와 콘솔 핸들러 모두 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 파일 핸들러 (모든 로그)
file_handler = logging.FileHandler("app.log", encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# 경고 전용 파일 핸들러
warning_handler = logging.FileHandler("warnings.log", encoding='utf-8')
warning_handler.setLevel(logging.WARNING)
warning_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
warning_handler.setFormatter(warning_formatter)

# 콘솔 핸들러 (스케줄러 실행 시 확인용)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# 핸들러 추가
logger.addHandler(file_handler)
logger.addHandler(warning_handler)
logger.addHandler(console_handler)

logging.captureWarnings(True)

def main(args):
    logger = logging.getLogger(__name__)
    logger.info("🚀 데이터 수집 작업이 시작되었습니다.")
    logger.info(f"📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    env_manager = EnvManager(args)
    preprocessor = PreProcessor()
    db_manager = DBManager(env_manager.db_config)
    api_pipeline = APIPipeline(bearer_tok=env_manager.bearer_token) 

    pipe = PipelineController(env_manager=env_manager, preprocessor=preprocessor, db_manager=db_manager)   
    pipe.set_env()

    # input_data 초기화
    input_data = None

    if args.process == 'code-test':   # 저장할 파일명 지정   
        file_extension = args.file_name.split('.')[-1].lower()
        file_path = os.path.join(args.data_path, args.file_name)
        
        if file_extension == 'csv': 
            input_data = pd.read_csv(file_path)
        elif file_extension == 'xlsx':
            input_data = pd.read_excel(file_path)
        else:
            logger.error(f"❌ 지원하지 않는 파일 형식입니다: {file_extension}")
            logger.error("지원 형식: csv, xlsx")
            return
    elif args.process == 'daily':    # 매일 12시 10분에 전일 데이터 저장
        yy, mm, dd = pipe.time_p.get_previous_day_date()
        start_date = yy + "-" + mm + "-" + dd
        logger.info(f"📅 전일 데이터 수집 시작: {start_date}")
        
        api_data = api_pipeline.get_data(date=start_date, tenant_id='ibk')        
        if api_data:
            print(f"첫 번째 데이터 샘플: {api_data[0] if api_data else 'None'}")
        
        input_data = api_pipeline.process_data(api_data)
        print(f"처리된 데이터 shape: {input_data.shape}")        
        if input_data.empty:
            logger.warning("❌ 처리된 데이터가 비어있습니다. 다른 날짜를 시도해보세요.")
            return
        else:
            print(input_data.head())
    elif args.process == 'scheduled':  # 스케줄링 모드 - 매시간 실행
        # 현재 시간 기준으로 데이터 수집 (실시간 또는 최근 데이터)
        current_time = datetime.now()
        # 매시간 실행이므로 현재 시간의 데이터를 수집
        start_date = current_time.strftime("%Y-%m-%d")
        logger.info(f"📅 스케줄링 모드 - 현재 시간 데이터 수집: {start_date}")
        
        api_data = api_pipeline.get_data(date=start_date, tenant_id='ibk')        
        if api_data:
            logger.info(f"API에서 {len(api_data)}개의 데이터를 가져왔습니다.")
            print(f"첫 번째 데이터 샘플: {api_data[0] if api_data else 'None'}")
        else:
            logger.warning("API에서 데이터를 가져오지 못했습니다.")
        
        input_data = api_pipeline.process_data(api_data)
        print(f"처리된 데이터 shape: {input_data.shape}")        
        if input_data.empty:
            logger.warning("❌ 처리된 데이터가 비어있습니다. 데이터가 없을 수 있습니다.")
            return
        else:
            print(input_data.head())
    else:
        logger.error(f"❌ 지원하지 않는 프로세스 타입입니다: {args.process}")
        logger.error("지원 타입: code-test, daily, scheduled")
        return

    # input_data가 None이거나 비어있는 경우 체크
    if input_data is None:
        logger.error("❌ input_data가 초기화되지 않았습니다.")
        return
        
    if input_data.empty:
        logger.warning("❌ input_data가 비어있습니다.")
        return

    # 필요한 컬럼이 있는지 확인
    required_columns = ['date', 'q/a', 'content', 'user_id', 'tenant_id']
    missing_columns = [col for col in required_columns if col not in input_data.columns]
    if missing_columns:
        logger.error(f"❌ 필요한 컬럼이 없습니다: {missing_columns}")
        logger.error(f"현재 컬럼: {list(input_data.columns)}")
        return

    input_data = input_data[required_columns]
    conv_ids = []
    content_hashes = []
    
    for idx in tqdm(range(len(input_data))):   # 챗봇 대화 로그 데이터에 PK 추가 
        date_str = input_data['date'][idx]
        # 날짜 문자열을 datetime 객체로 변환
        if isinstance(date_str, str):
            date_value = datetime.fromisoformat(date_str)
        else:
            date_value = date_str
        
        # UTC를 서울 시간(KST, UTC+9)으로 변환
        from datetime import timezone, timedelta
        kst = timezone(timedelta(hours=9))
        if date_value.tzinfo is None:
            # timezone 정보가 없으면 UTC로 가정
            date_value = date_value.replace(tzinfo=timezone.utc)
        kst_date = date_value.astimezone(kst)
        
        pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
        
        # 원래 방식: 순서 기반 conv_id (Q와 A가 같은 conv_id를 가져야 함)
        conv_id = pk_date + '_' + str(idx).zfill(5)
        conv_ids.append(conv_id)
        
        # 내용 기반 해시값 생성 (중복 체크용)
        # Q와 A는 같은 대화이므로 user_id, date, content만으로 해시 생성
        import hashlib
        content_hash = hashlib.md5(
            f"{input_data['user_id'][idx]}_{input_data['content'][idx]}_{input_data['date'][idx]}".encode()
        ).hexdigest()
        content_hashes.append(content_hash)
    input_data.insert(0, 'conv_id', conv_ids)
    input_data.insert(1, 'hash_value', content_hashes)  # 해시값 컬럼 추가
    
    # 중복 저장 방지 통계
    total_records = len(input_data)
    existing_records = 0
    new_records = 0
    
    for idx in tqdm(range(len(input_data))):   # PostgreSQL 테이블에 데이터 저장
        # 해시값 기준으로 중복 체크
        if pipe.postgres.check_hash_duplicate(pipe.env_manager.conv_tb_name, input_data['hash_value'][idx]):
            existing_records += 1
            logger.info(f"이미 존재하는 데이터 (해시: {input_data['hash_value'][idx][:8]}...): {input_data['conv_id'][idx]}")
            continue
        
        new_records += 1
        data_set = tuple(input_data.iloc[idx].values)
        pipe.table_editor.edit_conv_table('insert', pipe.env_manager.conv_tb_name, data_type='raw', data=data_set)
    
    # 저장 결과 요약
    summary_msg = f"📊 데이터 저장 완료 - 전체: {total_records}, 신규: {new_records}, 중복: {existing_records}"
    print(f"\n{summary_msg}")
    print(f"   전체 레코드: {total_records}")
    print(f"   새로 저장된 레코드: {new_records}")
    print(f"   이미 존재하는 레코드: {existing_records}")
    print(f"   중복률: {(existing_records/total_records*100):.1f}%" if total_records > 0 else "   중복률: 0%")
    
    logger.info(summary_msg)
    logger.info(f"✅ 데이터 수집 작업이 완료되었습니다. ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
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
        
        # 스케줄러 상태 확인을 위한 주기적 로그
        logger.info("⏰ 스케줄러가 실행 중입니다. 매시간 5분에 데이터 수집이 실행됩니다.")
        
        try:
            # 메인 스레드 유지 및 주기적 상태 확인
            check_count = 0
            while True:
                time.sleep(60)  # 1분마다 체크
                check_count += 1
                
                # 10분마다 스케줄러 상태 로그
                if check_count % 10 == 0:
                    jobs = scheduler.get_jobs()
                    if jobs:
                        next_run = jobs[0].next_run_time
                        logger.info(f"⏰ 스케줄러 실행 중 - 다음 실행 예정: {next_run}")
                    else:
                        logger.warning("⚠️ 등록된 작업이 없습니다!")
                        
        except KeyboardInterrupt:
            logger.info("⏹️ 스케줄러를 종료합니다...")
            scheduler.shutdown()
    else:
        # 일회성 실행
        main(cli_args)