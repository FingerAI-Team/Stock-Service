#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
데이터베이스 정리 스크립트
1. hash_value가 없는 데이터에 해시값 생성
2. conv_id에 hash_value가 포함된 잘못된 데이터 제거
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src import EnvManager, DBManager
import argparse
import logging
import hashlib
from datetime import datetime
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cleanup.log"),
    ]
)

def generate_hash_value(user_id, content, date):
    """해시값 생성"""
    return hashlib.md5(
        f"{user_id}_{content}_{date}".encode()
    ).hexdigest()

def cleanup_database():
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
    
    logger.info("🔍 데이터베이스 정리 작업 시작")
    
    # 1. hash_value가 없는 데이터 찾기
    logger.info("📋 hash_value가 없는 데이터 조회 중...")
    postgres.db_connection.cur.execute(
        f"SELECT conv_id, user_id, content, date FROM {table_name} WHERE hash_value IS NULL"
    )
    null_hash_records = postgres.db_connection.cur.fetchall()
    
    logger.info(f"✅ hash_value가 없는 레코드: {len(null_hash_records)}개")
    
    # hash_value 업데이트
    if null_hash_records:
        logger.info("🔄 hash_value 생성 및 업데이트 중...")
        updated_count = 0
        
        for record in tqdm(null_hash_records, desc="해시값 업데이트"):
            conv_id, user_id, content, date = record
            
            # 해시값 생성
            hash_value = generate_hash_value(user_id, content, date)
            
            # 업데이트
            postgres.db_connection.cur.execute(
                f"UPDATE {table_name} SET hash_value = %s WHERE conv_id = %s",
                (hash_value, conv_id)
            )
            updated_count += 1
        
        postgres.db_connection.conn.commit()
        logger.info(f"✅ {updated_count}개 레코드의 hash_value 업데이트 완료")
    
    # 2. conv_id에 hash_value가 포함된 잘못된 데이터 찾기
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
    
    # 3. 중복 데이터 확인 (hash_value 기준)
    logger.info("🔍 hash_value 기준 중복 데이터 확인 중...")
    postgres.db_connection.cur.execute(
        f"""
        SELECT hash_value, COUNT(*) as count 
        FROM {table_name} 
        WHERE hash_value IS NOT NULL 
        GROUP BY hash_value 
        HAVING COUNT(*) > 1
        """
    )
    duplicate_hashes = postgres.db_connection.cur.fetchall()
    
    if duplicate_hashes:
        logger.warning(f"⚠️ 중복된 hash_value: {len(duplicate_hashes)}개")
        for hash_value, count in duplicate_hashes:
            logger.warning(f"   해시 {hash_value[:8]}...: {count}개 중복")
    else:
        logger.info("✅ 중복 데이터 없음")
    
    # 4. 최종 통계
    postgres.db_connection.cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_count = postgres.db_connection.cur.fetchone()[0]
    
    postgres.db_connection.cur.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE hash_value IS NOT NULL"
    )
    hash_count = postgres.db_connection.cur.fetchone()[0]
    
    logger.info("📊 최종 통계:")
    logger.info(f"   전체 레코드: {total_count}")
    logger.info(f"   hash_value 있는 레코드: {hash_count}")
    logger.info(f"   hash_value 없는 레코드: {total_count - hash_count}")
    
    # 데이터베이스 연결 종료
    postgres.db_connection.close()
    logger.info("🎉 데이터베이스 정리 작업 완료")

def preview_changes():
    """변경사항 미리보기 (실제 변경하지 않음)"""
    logger = logging.getLogger(__name__)
    
    # 환경 설정
    args = argparse.Namespace()
    args.config_path = './config/'
    env_manager = EnvManager(args)
    db_manager = DBManager(env_manager.db_config)
    
    # 데이터베이스 연결
    postgres, table_editor = db_manager.initialize_database()
    table_name = env_manager.conv_tb_name
    
    logger.info("👀 변경사항 미리보기")
    
    # 1. hash_value가 없는 데이터
    postgres.db_connection.cur.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE hash_value IS NULL"
    )
    null_hash_count = postgres.db_connection.cur.fetchone()[0]
    logger.info(f"📋 hash_value가 없는 레코드: {null_hash_count}개")
    
    # 2. 잘못된 conv_id 형식
    postgres.db_connection.cur.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE conv_id ~ '^[0-9]{{8}}_[a-zA-Z0-9]+_[a-f0-9]{{8}}$'"
    )
    wrong_conv_count = postgres.db_connection.cur.fetchone()[0]
    logger.info(f"⚠️ 잘못된 형식의 conv_id: {wrong_conv_count}개")
    
    # 3. 샘플 데이터 출력
    if null_hash_count > 0:
        logger.info("📋 hash_value가 없는 샘플 데이터:")
        postgres.db_connection.cur.execute(
            f"SELECT conv_id, user_id, content, date FROM {table_name} WHERE hash_value IS NULL LIMIT 3"
        )
        for record in postgres.db_connection.cur.fetchall():
            logger.info(f"   {record}")
    
    if wrong_conv_count > 0:
        logger.info("⚠️ 잘못된 형식의 conv_id 샘플:")
        postgres.db_connection.cur.execute(
            f"SELECT conv_id FROM {table_name} WHERE conv_id ~ '^[0-9]{{8}}_[a-zA-Z0-9]+_[a-f0-9]{{8}}$' LIMIT 3"
        )
        for (conv_id,) in postgres.db_connection.cur.fetchall():
            logger.info(f"   {conv_id}")
    
    postgres.db_connection.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='데이터베이스 정리 스크립트')
    parser.add_argument('--preview', action='store_true', 
                       help='변경사항 미리보기 (실제 변경하지 않음)')
    parser.add_argument('--execute', action='store_true',
                       help='실제 정리 작업 실행')
    
    args = parser.parse_args()
    
    if args.preview:
        preview_changes()
    elif args.execute:
        cleanup_database()
    else:
        print("사용법:")
        print("  python cleanup_database.py --preview   # 변경사항 미리보기")
        print("  python cleanup_database.py --execute   # 실제 정리 작업 실행")
