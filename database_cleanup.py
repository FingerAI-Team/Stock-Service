#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 데이터베이스 정리 스크립트
- 백업 생성
- hash_value가 없는 데이터에 해시값 생성
- conv_id에 해시값이 포함된 잘못된 데이터 제거
- 중복 해시값 처리
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src import EnvManager, DBManager
import argparse
import logging
import hashlib
import pandas as pd
import re
from datetime import datetime
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database_cleanup.log"),
    ]
)

def generate_hash_value(user_id, content, date):
    """해시값 생성"""
    return hashlib.md5(
        f"{user_id}_{content}_{date}".encode()
    ).hexdigest()

def is_valid_conv_id(conv_id):
    """정상적인 conv_id 형식인지 확인 (YYYYMMDD_XXXXX)"""
    pattern = re.compile(r'^\d{8}_\d{5}$')
    return bool(pattern.match(conv_id))

def is_hash_containing_conv_id(conv_id):
    """conv_id에 해시값이 포함된 형식인지 확인"""
    # 정상 형식: 20250922_00002 (날짜_5자리숫자)
    # 잘못된 형식: 20250922_긴문자열_해시값 (날짜_긴문자열_해시)
    
    # 정상 형식이면 False 반환
    if re.match(r'^\d{8}_\d{5}$', conv_id):
        return False
    
    # 3개 부분으로 나뉘고, 중간 부분이 긴 문자열이면 잘못된 형식
    parts = conv_id.split('_')
    if len(parts) == 3:
        date_part, middle_part, hash_part = parts
        # 날짜 부분이 8자리 숫자이고, 중간 부분이 긴 문자열이면 잘못된 형식
        if (re.match(r'^\d{8}$', date_part) and 
            len(middle_part) > 10 and  # 중간 부분이 10자리보다 길면
            re.match(r'^[a-f0-9]+$', hash_part)):  # 마지막 부분이 해시값이면
            return True
    
    return False

def backup_data(postgres, table_name, backup_file):
    """데이터 백업"""
    logger = logging.getLogger(__name__)
    logger.info(f"💾 데이터 백업 중... ({backup_file})")
    
    # 전체 데이터 조회
    postgres.db_connection.cur.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in postgres.db_connection.cur.description]
    data = postgres.db_connection.cur.fetchall()
    
    # DataFrame으로 변환하여 CSV로 저장
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(backup_file, index=False, encoding='utf-8')
    
    logger.info(f"✅ 백업 완료: {len(data)}개 레코드 → {backup_file}")

def preview_changes(postgres, table_name):
    """변경사항 미리보기"""
    logger = logging.getLogger(__name__)
    logger.info("👀 변경사항 미리보기")
    
    # 1. hash_value가 없는 데이터
    postgres.db_connection.cur.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE hash_value IS NULL"
    )
    null_hash_count = postgres.db_connection.cur.fetchone()[0]
    logger.info(f"📋 hash_value가 없는 레코드: {null_hash_count}개")
    
    # 2. 잘못된 conv_id 형식
    postgres.db_connection.cur.execute(f"SELECT conv_id FROM {table_name}")
    all_conv_ids = [row[0] for row in postgres.db_connection.cur.fetchall()]
    
    invalid_conv_ids = [conv_id for conv_id in all_conv_ids if is_hash_containing_conv_id(conv_id)]
    logger.info(f"⚠️ 잘못된 형식의 conv_id: {len(invalid_conv_ids)}개")
    
    # 3. 샘플 데이터 출력
    if null_hash_count > 0:
        logger.info("📋 hash_value가 없는 샘플 데이터:")
        postgres.db_connection.cur.execute(
            f"SELECT conv_id, user_id, content, date FROM {table_name} WHERE hash_value IS NULL LIMIT 3"
        )
        for record in postgres.db_connection.cur.fetchall():
            logger.info(f"   {record}")
    
    if invalid_conv_ids:
        logger.info("⚠️ 잘못된 형식의 conv_id 샘플:")
        for conv_id in invalid_conv_ids[:5]:
            logger.info(f"   {conv_id}")
    
    # 4. 정상 형식 샘플
    valid_conv_ids = [conv_id for conv_id in all_conv_ids if is_valid_conv_id(conv_id)]
    if valid_conv_ids:
        logger.info("✅ 정상 형식의 conv_id 샘플:")
        for conv_id in valid_conv_ids[:5]:
            logger.info(f"   {conv_id}")

def cleanup_database(execute=False):
    """데이터베이스 정리 작업"""
    logger = logging.getLogger(__name__)
    
    # 환경 설정
    args = argparse.Namespace()
    args.config_path = './config/'
    env_manager = EnvManager(args)
    db_manager = DBManager(env_manager.db_config)
    
    # 데이터베이스 연결
    postgres, table_editor = db_manager.initialize_database()
    table_name = env_manager.conv_tb_name
    
    # 백업 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"backup_{table_name}_{timestamp}.csv"
    
    logger.info("🔍 데이터베이스 정리 작업 시작")
    
    if not execute:
        logger.info("📋 미리보기 모드 - 실제 변경은 하지 않습니다")
        preview_changes(postgres, table_name)
        postgres.db_connection.close()
        return
    
    # 1. 백업 생성
    backup_data(postgres, table_name, backup_file)
    
    # 2. conv_id 패턴 분석
    logger.info("🔍 conv_id 패턴 분석 중...")
    postgres.db_connection.cur.execute(f"SELECT conv_id FROM {table_name}")
    all_conv_ids = [row[0] for row in postgres.db_connection.cur.fetchall()]
    
    valid_conv_ids = [conv_id for conv_id in all_conv_ids if is_valid_conv_id(conv_id)]
    invalid_conv_ids = [conv_id for conv_id in all_conv_ids if is_hash_containing_conv_id(conv_id)]
    
    logger.info(f"📊 conv_id 분석 결과:")
    logger.info(f"   정상 형식: {len(valid_conv_ids)}개")
    logger.info(f"   잘못된 형식: {len(invalid_conv_ids)}개")
    
    # 3. 잘못된 conv_id 데이터 제거
    if invalid_conv_ids:
        logger.info("🗑️ 잘못된 형식의 conv_id 데이터 제거 중...")
        logger.info("⚠️ 제거될 데이터 샘플:")
        for conv_id in invalid_conv_ids[:5]:
            logger.info(f"   {conv_id}")
        
        deleted_count = 0
        for conv_id in tqdm(invalid_conv_ids, desc="잘못된 데이터 제거"):
            postgres.db_connection.cur.execute(
                f"DELETE FROM {table_name} WHERE conv_id = %s",
                (conv_id,)
            )
            deleted_count += 1
        
        postgres.db_connection.conn.commit()
        logger.info(f"✅ {deleted_count}개 잘못된 레코드 제거 완료")
    
    # 4. hash_value가 없는 데이터 처리
    logger.info("📋 hash_value가 없는 데이터 조회 중...")
    postgres.db_connection.cur.execute(
        f"SELECT conv_id, user_id, content, date FROM {table_name} WHERE hash_value IS NULL"
    )
    null_hash_records = postgres.db_connection.cur.fetchall()
    
    logger.info(f"✅ hash_value가 없는 레코드: {len(null_hash_records)}개")
    
    # 5. 기존 hash_value들 조회 (중복 체크용)
    logger.info("🔍 기존 hash_value 조회 중...")
    postgres.db_connection.cur.execute(
        f"SELECT hash_value FROM {table_name} WHERE hash_value IS NOT NULL"
    )
    existing_hashes = set(row[0] for row in postgres.db_connection.cur.fetchall())
    logger.info(f"✅ 기존 hash_value: {len(existing_hashes)}개")
    
    # 6. hash_value 업데이트 (중복 시 스킵)
    if null_hash_records:
        logger.info("🔄 hash_value 생성 및 업데이트 중...")
        updated_count = 0
        skipped_count = 0
        
        for record in tqdm(null_hash_records, desc="해시값 업데이트"):
            conv_id, user_id, content, date = record
            
            # 기본 해시값 생성
            hash_value = generate_hash_value(user_id, content, date)
            
            # 중복 체크
            if hash_value in existing_hashes:
                # 중복된 해시값인 경우 스킵
                skipped_count += 1
                continue
            
            # 해시값을 기존 해시 세트에 추가 (다음 레코드 중복 체크용)
            existing_hashes.add(hash_value)
            
            # 업데이트
            postgres.db_connection.cur.execute(
                f"UPDATE {table_name} SET hash_value = %s WHERE conv_id = %s",
                (hash_value, conv_id)
            )
            updated_count += 1
        
        postgres.db_connection.conn.commit()
        logger.info(f"✅ {updated_count}개 레코드의 hash_value 업데이트 완료")
        if skipped_count > 0:
            logger.info(f"⏭️ {skipped_count}개 레코드는 중복 해시값으로 인해 스킵됨")
    
    # 7. 최종 통계
    postgres.db_connection.cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_count = postgres.db_connection.cur.fetchone()[0]
    
    postgres.db_connection.cur.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE hash_value IS NOT NULL"
    )
    hash_count = postgres.db_connection.cur.fetchone()[0]
    
    postgres.db_connection.cur.execute(
        f"SELECT COUNT(DISTINCT hash_value) FROM {table_name} WHERE hash_value IS NOT NULL"
    )
    unique_hash_count = postgres.db_connection.cur.fetchone()[0]
    
    # 8. 최종 conv_id 형식 확인
    postgres.db_connection.cur.execute(f"SELECT conv_id FROM {table_name}")
    final_conv_ids = [row[0] for row in postgres.db_connection.cur.fetchall()]
    
    final_valid_count = sum(1 for conv_id in final_conv_ids if is_valid_conv_id(conv_id))
    final_invalid_count = len(final_conv_ids) - final_valid_count
    
    logger.info("📊 최종 통계:")
    logger.info(f"   전체 레코드: {total_count}")
    logger.info(f"   hash_value 있는 레코드: {hash_count}")
    logger.info(f"   hash_value 없는 레코드: {total_count - hash_count}")
    logger.info(f"   고유한 hash_value: {unique_hash_count}")
    logger.info(f"   정상 conv_id: {final_valid_count}")
    logger.info(f"   잘못된 conv_id: {final_invalid_count}")
    logger.info(f"   백업 파일: {backup_file}")
    
    if final_invalid_count > 0:
        logger.warning(f"⚠️ 여전히 {final_invalid_count}개의 잘못된 conv_id가 남아있습니다!")
        # 잘못된 conv_id 샘플 출력
        invalid_remaining = [conv_id for conv_id in final_conv_ids if not is_valid_conv_id(conv_id)]
        logger.warning("잘못된 conv_id 샘플:")
        for conv_id in invalid_remaining[:5]:
            logger.warning(f"   {conv_id}")
    
    # 데이터베이스 연결 종료
    postgres.db_connection.close()
    logger.info("🎉 데이터베이스 정리 작업 완료")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='통합 데이터베이스 정리 스크립트')
    parser.add_argument('--preview', action='store_true', 
                       help='변경사항 미리보기 (실제 변경하지 않음)')
    parser.add_argument('--execute', action='store_true',
                       help='백업 후 실제 정리 작업 실행')
    
    args = parser.parse_args()
    
    if args.preview:
        cleanup_database(execute=False)
    elif args.execute:
        cleanup_database(execute=True)
    else:
        print("📋 통합 데이터베이스 정리 스크립트")
        print("=" * 50)
        print("사용법:")
        print("  python database_cleanup.py --preview   # 변경사항 미리보기")
        print("  python database_cleanup.py --execute   # 백업 후 정리 작업 실행")
        print("")
        print("🔧 기능:")
        print("  - 자동 백업 생성")
        print("  - hash_value가 없는 데이터에 해시값 생성")
        print("  - conv_id에 해시값이 포함된 잘못된 데이터 제거")
        print("  - 중복 해시값 처리 (중복 시 스킵)")
        print("  - 상세한 통계 및 로깅")
        print("")
        print("⚠️ 주의사항:")
        print("  - --execute 옵션 사용 시 데이터가 변경됩니다")
        print("  - 실행 전 자동으로 백업이 생성됩니다")
        print("  - 백업 파일은 CSV 형식으로 저장됩니다")
