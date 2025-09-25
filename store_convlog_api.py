from src import EnvManager, PreProcessor, DBManager, APIPipeline, PipelineController
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv
import argparse 
import logging
import time
import os
from datetime import datetime, timezone, timedelta
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

    pipe = PipelineController(env_ manager=env_manager, preprocessor=preprocessor, db_manager=db_manager)   
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
    elif args.process == 'daily':    # 매일 12시 10분에 당일 데이터 저장
        # 당일 날짜 기준으로 API 호출 (ibk, ibks 모두 수집)
        today = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"📅 당일 데이터 수집: {today}")
        
        # ibk와 ibks 두 tenant_id 모두 수집
        all_api_data = []
        tenant_ids = ['ibk', 'ibks']
        for tenant_id in tenant_ids:
            logger.info(f"🔍 {tenant_id} tenant 데이터 수집 중...")
            api_data = api_pipeline.get_data(date=today, tenant_id=tenant_id)
            if api_data:
                all_api_data.extend(api_data)
                logger.info(f"   ✅ {tenant_id}: {len(api_data)}개 레코드 수집")
            else:
                logger.info(f"   ⚠️ {tenant_id}: 데이터 없음")
        
        logger.info(f"📊 총 수집된 API 데이터: {len(all_api_data)}개")
        if not all_api_data:
            logger.warning("❌ 수집된 데이터가 없습니다.")
            return
        
        input_data = api_pipeline.process_data(all_api_data)
        print(f"처리된 데이터 shape: {input_data.shape}")        
        if input_data.empty:
            logger.warning("❌ 처리된 데이터가 비어있습니다.")
            return
        else:
            print(input_data.head())
    elif args.process == 'scheduled':  # 스케줄링 모드 - 매시간 실행
        # 현재 시간 기준으로 데이터 수집 (ibk, ibks 모두 수집)
        current_time = datetime.now()
        # 매시간 실행이므로 현재 시간의 데이터를 수집
        start_date = current_time.strftime("%Y-%m-%d")
        logger.info(f"📅 스케줄링 모드 - 현재 시간 데이터 수집: {start_date}")
        
        # ibk와 ibks 두 tenant_id 모두 수집
        all_api_data = []
        tenant_ids = ['ibk', 'ibks']        
        for tenant_id in tenant_ids:
            logger.info(f"🔍 {tenant_id} tenant 데이터 수집 중...")
            api_data = api_pipeline.get_data(date=start_date, tenant_id=tenant_id)
            if api_data:
                all_api_data.extend(api_data)
                logger.info(f"   ✅ {tenant_id}: {len(api_data)}개 레코드 수집")
            else:
                logger.info(f"   ⚠️ {tenant_id}: 데이터 없음")
        logger.info(f"📊 총 수집된 API 데이터: {len(all_api_data)}개")
        if not all_api_data:
            logger.warning("❌ 수집된 데이터가 없습니다.")
            return
        
        input_data = api_pipeline.process_data(all_api_data)
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
    required_columns = ['date', 'q/a', 'content', 'user_id', 'tenant_id', 'hash_value', 'hash_ref']
    missing_columns = [col for col in required_columns if col not in input_data.columns]
    if missing_columns:
        logger.error(f"❌ 필요한 컬럼이 없습니다: {missing_columns}")
        logger.error(f"현재 컬럼: {list(input_data.columns)}")
        return

    input_data = input_data[required_columns]
    
    # 날짜별 conv_id 카운터 초기화
    date_counters = {}
    
    # 기존 DB에서 각 날짜별 최대 conv_id 조회
    for date_str in input_data['date'].unique():
        date_value = datetime.fromisoformat(date_str)
        kst = timezone(timedelta(hours=9))
        if date_value.tzinfo is None:
            date_value = date_value.replace(tzinfo=timezone.utc)
        kst_date = date_value.astimezone(kst)
        pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
        
        try:
            pipe.postgres.db_connection.cur.execute(
                f"SELECT MAX(conv_id) FROM {pipe.env_manager.conv_tb_name} WHERE conv_id LIKE %s",
                (f"{pk_date}_%",)
            )
            max_conv_id = pipe.postgres.db_connection.cur.fetchone()[0]
            date_counters[pk_date] = int(max_conv_id.split('_')[1]) if max_conv_id else 0
        except:
            date_counters[pk_date] = 0
    
    # conv_id 생성 및 KST 변환
    conv_ids = []
    for idx in tqdm(range(len(input_data))):
        date_value = datetime.fromisoformat(input_data['date'][idx])
        kst = timezone(timedelta(hours=9))
        if date_value.tzinfo is None:
            date_value = date_value.replace(tzinfo=timezone.utc)
        kst_date = date_value.astimezone(kst)
        
        input_data.at[idx, 'date'] = kst_date.isoformat()
        pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
        date_counters[pk_date] += 1
        conv_ids.append(f"{pk_date}_{str(date_counters[pk_date]).zfill(5)}")
    
    # conv_id를 첫 번째 컬럼으로 삽입
    input_data.insert(0, 'conv_id', conv_ids)
    
    # DB 테이블 구조에 맞는 컬럼 순서로 재정렬
    input_data = input_data[['conv_id', 'date', 'q/a', 'content', 'user_id', 'tenant_id', 'hash_value', 'hash_ref']]
    
    # Q&A 연결 통계
    q_count = sum(1 for qa in input_data['q/a'] if qa == 'Q')
    a_count = sum(1 for qa in input_data['q/a'] if qa == 'A')
    a_with_ref = sum(1 for ref in input_data['hash_ref'] if ref is not None)
    print(f"📊 Q&A 연결 통계: Q {q_count}개, A {a_count}개, A에 hash_ref 있음 {a_with_ref}개")
    
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
        
        # 디버깅: 저장할 데이터 확인 (처음 3개만)
        if idx < 3:
            print(f"🔍 저장할 데이터 {idx}: {data_set}")
            print(f"   - conv_id: {data_set[0]}")
            print(f"   - date: {data_set[1]}")
            print(f"   - q/a: {data_set[2]}")
            print(f"   - content: {data_set[3][:50]}...")
            print(f"   - user_id: {data_set[4]}")
            print(f"   - tenant_id: {data_set[5]}")
            print(f"   - hash_value: {data_set[6]}")
            print(f"   - hash_ref: {data_set[7]}")
        
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