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
    elif args.process == 'daily':    # 날짜 범위 지정하여 데이터 저장
        # API 호출용 날짜 설정 (KST 변환 없이 그대로 사용)
        from_date = "2025-09-22"
        to_date = "2025-09-23"
        print(f"📅 API 요청 날짜: {from_date} ~ {to_date}")
        
        # 날짜 범위에 대해 API 호출 (ibk, ibks 모두 수집)
        all_api_data = []
        from datetime import datetime, timedelta
        tenant_ids = ['ibk', 'ibks']
        
        current_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"🔍 {date_str} 데이터 수집 중...")
            
            for tenant_id in tenant_ids:
                print(f"   📋 {tenant_id} tenant 수집 중...")
                api_data = api_pipeline.get_data(date=date_str, tenant_id=tenant_id)
                if api_data:
                    all_api_data.extend(api_data)
                    print(f"      ✅ {tenant_id}: {len(api_data)}개 레코드 수집")
                else:
                    print(f"      ⚠️ {tenant_id}: 데이터 없음")
            
            current_date += timedelta(days=1)
        
        print(f"📊 총 수집된 API 데이터: {len(all_api_data)}개")
        
        if not all_api_data:
            print("❌ 수집된 데이터가 없습니다.")
            return
        
        input_data = api_pipeline.process_data(all_api_data)
        print(f"처리된 데이터 shape: {input_data.shape}")        
        if input_data.empty:
            print("❌ 처리된 데이터가 비어있습니다.")
            return
        else:
            print(input_data.head())

    input_data = input_data[['date', 'q/a', 'content', 'user_id', 'tenant_id']]
    conv_ids = []
    content_hashes = []
    hash_refs = []
    
    # Q&A 쌍을 위한 임시 저장소
    qa_pairs = {}  # {qa_key: q_hash_value}
    
    # 날짜별 인덱스 카운터를 위한 딕셔너리 (기존 DB의 최대값부터 시작)
    date_counters = {}
    
    # 기존 데이터베이스에서 각 날짜별 최대 conv_id 번호 조회
    print("🔍 기존 데이터베이스의 conv_id 범위 확인 중...")
    for idx in range(len(input_data)):
        date_str = input_data['date'][idx]
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
        
        if pk_date not in date_counters:
            # 해당 날짜의 기존 최대 conv_id 번호 조회
            try:
                pipe.postgres.db_connection.cur.execute(
                    f"SELECT MAX(CAST(SUBSTRING(conv_id FROM 10) AS INTEGER)) FROM {pipe.env_manager.conv_tb_name} WHERE conv_id LIKE %s",
                    (f"{pk_date}_%",)
                )
                result = pipe.postgres.db_connection.cur.fetchone()
                max_existing = result[0] if result[0] is not None else -1
                date_counters[pk_date] = max_existing
                print(f"   {pk_date}: 기존 최대 번호 {max_existing}, 다음 번호부터 시작")
            except Exception as e:
                print(f"   {pk_date}: 기존 데이터 조회 실패, 0부터 시작 ({e})")
                date_counters[pk_date] = -1
    
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
        
        # date 컬럼에 저장할 값도 KST로 변환
        input_data.at[idx, 'date'] = kst_date.isoformat()
        
        pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
        
        # 날짜별로 고유한 인덱스 생성 (기존 최대값 + 1부터 시작)
        date_counters[pk_date] += 1
        
        # 날짜별 고유한 conv_id 생성
        conv_id = pk_date + '_' + str(date_counters[pk_date]).zfill(5)
        conv_ids.append(conv_id)
        
        # 내용 기반 해시값 생성 (중복 체크용)
        # Q와 A는 같은 대화이므로 user_id, date, content만으로 해시 생성
        import hashlib
        content_hash = hashlib.md5(
            f"{input_data['user_id'][idx]}_{input_data['content'][idx]}_{input_data['date'][idx]}".encode()
        ).hexdigest()
        content_hashes.append(content_hash)
        
        # Q&A 쌍 연결을 위한 hash_ref 생성
        qa_type = input_data['q/a'][idx]
        user_id = input_data['user_id'][idx]
        date_key = input_data['date'][idx]
        
        # Q&A 쌍을 구분하기 위한 키 생성 (user_id + date + 순서)
        qa_key = f"{user_id}_{date_key}_{idx//2}"  # 2개씩 쌍이므로 idx//2로 그룹핑
        
        if qa_type == 'Q':
            # Q인 경우: 자신의 해시값을 저장하고 hash_ref는 NULL
            qa_pairs[qa_key] = content_hash
            hash_refs.append(None)
        elif qa_type == 'A':
            # A인 경우: 해당하는 Q의 해시값을 hash_ref로 설정
            if qa_key in qa_pairs:
                hash_refs.append(qa_pairs[qa_key])
            else:
                # Q를 찾지 못한 경우 (데이터 순서 문제 등)
                hash_refs.append(None)
                print(f"⚠️ A에 대응하는 Q를 찾지 못함: {conv_id}")
        else:
            hash_refs.append(None)
    
    input_data.insert(0, 'conv_id', conv_ids)
    input_data.insert(1, 'hash_value', content_hashes)  # 해시값 컬럼 추가
    input_data.insert(2, 'hash_ref', hash_refs)  # Q&A 연결용 hash_ref 컬럼 추가
    
    # 디버깅: hash_ref 값 확인
    print(f"🔍 hash_ref 값 샘플 (처음 5개): {hash_refs[:5]}")
    print(f"🔍 hash_value 값 샘플 (처음 5개): {content_hashes[:5]}")
    print(f"🔍 input_data 컬럼 순서: {list(input_data.columns)}")
    print(f"🔍 input_data shape: {input_data.shape}")
    
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
            print(f"   - hash_value: {data_set[1]}")
            print(f"   - hash_ref: {data_set[2]}")
            print(f"   - q/a: {data_set[4]}")
        
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