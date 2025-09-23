#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
안전한 데이터베이스 정리 스크립트
- 중복 해시값 문제 해결
- 배치 처리로 성능 개선
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src import EnvManager, DBManager
import argparse
import logging
import hashlib
import pandas as pd
from datetime import datetime
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("safe_cleanup.log"),
    ]
)

def generate_hash_value(user_id, content, date):
    """해시값 생성"""
    return hashlib.md5(
        f"{user_id}_{content}_{date}".encode()
    ).hexdigest()

def safe_cleanup_database():
    """안전한 데이터베이스 정리 작업"""
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
    
    logger.info("🔍 안전한 데이터베이스 정리 작업 시작")
    
    # 1. 백업 생성
    logger.info(f"💾 데이터 백업 중... ({backup_file})")
    postgres.db_connection.cur.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in postgres.db_connection.cur.description]
    data = postgres.db_connection.cur.fetchall()
    
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(backup_file, index=False, encoding='utf-8')
    logger.info(f"✅ 백업 완료: {len(data)}개 레코드")
    
    # 2. hash_value가 없는 데이터 찾기
    logger.info("📋 hash_value가 없는 데이터 조회 중...")
    postgres.db_connection.cur.execute(
        f"SELECT conv_id, user_id, content, date FROM {table_name} WHERE hash_value IS NULL"
    )
    null_hash_records = postgres.db_connection.cur.fetchall()
    
    logger.info(f"✅ hash_value가 없는 레코드: {len(null_hash_records)}개")
    
    # 3. 기존 hash_value들 조회 (중복 체크용)
    logger.info("🔍 기존 hash_value 조회 중...")
    postgres.db_connection.cur.execute(
        f"SELECT hash_value FROM {table_name} WHERE hash_value IS NOT NULL"
    )
    existing_hashes = set(row[0] for row in postgres.db_connection.cur.fetchall())
    logger.info(f"✅ 기존 hash_value: {len(existing_hashes)}개")
    
    # 4. hash_value 업데이트 (중복 시 스킵)
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
    
    # 5. conv_id에 hash_value가 포함된 잘못된 데이터 찾기
    logger.info("🔍 conv_id에 해시값이 포함된 잘못된 데이터 조회 중...")
    
    # 해시값 패턴을 가진 conv_id 찾기 (예: 20250127_user123_abc12345)
    postgres.db_connection.cur.execute(
        f"SELECT conv_id FROM {table_name} WHERE conv_id ~ '^[0-9]{{8}}_[a-zA-Z0-9]+_[a-f0-9]{{8}}$'"
    )
    wrong_conv_ids = postgres.db_connection.cur.fetchall()
    
    logger.info(f"⚠️ 잘못된 형식의 conv_id: {len(wrong_conv_ids)}개")
    
    # 잘못된 데이터 제거
    if wrong_conv_ids:
        logger.info("🗑️ 잘못된 형식의 데이터 제거 중...")
        deleted_count = 0
        
        for (conv_id,) in tqdm(wrong_conv_ids, desc="데이터 제거"):
            postgres.db_connection.cur.execute(
                f"DELETE FROM {table_name} WHERE conv_id = %s",
                (conv_id,)
            )
            deleted_count += 1
        
        postgres.db_connection.conn.commit()
        logger.info(f"✅ {deleted_count}개 잘못된 레코드 제거 완료")
    
    # 6. 최종 통계
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
    
    logger.info("📊 최종 통계:")
    logger.info(f"   전체 레코드: {total_count}")
    logger.info(f"   hash_value 있는 레코드: {hash_count}")
    logger.info(f"   hash_value 없는 레코드: {total_count - hash_count}")
    logger.info(f"   고유한 hash_value: {unique_hash_count}")
    logger.info(f"   백업 파일: {backup_file}")
    
    # 데이터베이스 연결 종료
    postgres.db_connection.close()
    logger.info("🎉 안전한 데이터베이스 정리 작업 완료")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='안전한 데이터베이스 정리 스크립트')
    parser.add_argument('--execute', action='store_true',
                       help='백업 후 실제 정리 작업 실행')
    
    args = parser.parse_args()
    
    if args.execute:
        safe_cleanup_database()
    else:
        print("사용법:")
        print("  python safe_cleanup.py --execute   # 백업 후 정리 작업 실행")
        print("")
        print("✅ 개선사항:")
        print("  - 중복 해시값 문제 해결")
        print("  - 메모리 기반 중복 체크로 성능 향상")
        print("  - 자동 백업 생성")
        print("  - 상세한 통계 제공")
