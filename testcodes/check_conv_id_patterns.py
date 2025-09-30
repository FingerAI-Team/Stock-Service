#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
conv_id 패턴 확인 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src import EnvManager, DBManager
import argparse
import logging
import re
from collections import Counter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def check_conv_id_patterns():
    """conv_id 패턴 분석"""
    logger = logging.getLogger(__name__)
    
    # 환경 설정
    args = argparse.Namespace()
    args.config_path = './config/'
    env_manager = EnvManager(args)
    db_manager = DBManager(env_manager.db_config)
    
    # 데이터베이스 연결
    postgres, table_editor = db_manager.initialize_database()
    table_name = env_manager.conv_tb_name
    
    logger.info("🔍 conv_id 패턴 분석 시작")
    
    # 1. 모든 conv_id 조회
    postgres.db_connection.cur.execute(f"SELECT conv_id FROM {table_name} ORDER BY conv_id")
    conv_ids = [row[0] for row in postgres.db_connection.cur.fetchall()]
    
    logger.info(f"📊 전체 conv_id 개수: {len(conv_ids)}")
    
    # 2. 패턴 분석
    patterns = {
        'normal': [],      # 20250127_00001 형식
        'with_hash': [],   # 20250127_user123_abc12345 형식
        'other': []        # 기타 형식
    }
    
    normal_pattern = re.compile(r'^\d{8}_\d{5}$')
    hash_pattern = re.compile(r'^\d{8}_[a-zA-Z0-9]+_[a-f0-9]{8}$')
    
    for conv_id in conv_ids:
        if normal_pattern.match(conv_id):
            patterns['normal'].append(conv_id)
        elif hash_pattern.match(conv_id):
            patterns['with_hash'].append(conv_id)
        else:
            patterns['other'].append(conv_id)
    
    # 3. 결과 출력
    logger.info("📋 패턴 분석 결과:")
    logger.info(f"   정상 형식 (YYYYMMDD_XXXXX): {len(patterns['normal'])}개")
    logger.info(f"   해시값 포함 형식: {len(patterns['with_hash'])}개")
    logger.info(f"   기타 형식: {len(patterns['other'])}개")
    
    # 4. 샘플 데이터 출력
    if patterns['with_hash']:
        logger.info("⚠️ 해시값이 포함된 conv_id 샘플:")
        for conv_id in patterns['with_hash'][:10]:
            logger.info(f"   {conv_id}")
    
    if patterns['other']:
        logger.info("❓ 기타 형식의 conv_id 샘플:")
        for conv_id in patterns['other'][:10]:
            logger.info(f"   {conv_id}")
    
    # 5. 정상 형식 샘플
    if patterns['normal']:
        logger.info("✅ 정상 형식의 conv_id 샘플:")
        for conv_id in patterns['normal'][:5]:
            logger.info(f"   {conv_id}")
    
    # 6. 날짜별 분포 확인
    date_counter = Counter()
    for conv_id in conv_ids:
        if conv_id.startswith('2025') and '_' in conv_id:
            date_part = conv_id.split('_')[0]
            date_counter[date_part] += 1
    
    logger.info("📅 날짜별 conv_id 분포 (상위 10개):")
    for date, count in date_counter.most_common(10):
        logger.info(f"   {date}: {count}개")
    
    postgres.db_connection.close()
    logger.info("🎉 conv_id 패턴 분석 완료")

if __name__ == '__main__':
    check_conv_id_patterns()
