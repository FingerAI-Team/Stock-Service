#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
해시값 기반 중복 체크 테스트 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import hashlib
import pandas as pd
from datetime import datetime

def test_hash_generation():
    """해시값 생성 로직을 테스트합니다."""
    
    # 테스트 데이터
    test_data = [
        {"user_id": "user123", "content": "삼성전자 주가 알려주세요", "date": "2025-01-27T10:00:00", "q/a": "Q"},
        {"user_id": "user123", "content": "삼성전자 주가는 70,000원입니다.", "date": "2025-01-27T10:00:00", "q/a": "A"},
        {"user_id": "user456", "content": "삼성전자 주가 알려주세요", "date": "2025-01-27T10:00:00", "q/a": "Q"},
        {"user_id": "user123", "content": "삼성전자 주가 알려주세요", "date": "2025-01-27T11:00:00", "q/a": "Q"},  # 다른 시간
    ]
    
    print("🧪 해시값 생성 테스트")
    print("=" * 50)
    
    for i, data in enumerate(test_data):
        # 해시값 생성 (Q와 A 구분 없이)
        content_hash = hashlib.md5(
            f"{data['user_id']}_{data['content']}_{data['date']}".encode()
        ).hexdigest()
        
        print(f"데이터 {i+1}:")
        print(f"  사용자: {data['user_id']}")
        print(f"  내용: {data['content']}")
        print(f"  날짜: {data['date']}")
        print(f"  Q/A: {data['q/a']}")
        print(f"  해시: {content_hash[:16]}...")
        print()
    
    print("📋 분석:")
    print("- 같은 사용자, 같은 내용, 같은 시간 → 같은 해시 (중복)")
    print("- 다른 사용자, 같은 내용, 같은 시간 → 다른 해시 (중복 아님)")
    print("- 같은 사용자, 같은 내용, 다른 시간 → 다른 해시 (중복 아님)")

def test_conv_id_generation():
    """conv_id 생성 로직을 테스트합니다."""
    
    print("\n🔗 conv_id 생성 테스트")
    print("=" * 50)
    
    # 테스트 데이터 (Q와 A 쌍)
    test_pairs = [
        {"date": "2025-01-27T10:00:00", "user_id": "user123", "content": "삼성전자 주가 알려주세요", "q/a": "Q"},
        {"date": "2025-01-27T10:00:00", "user_id": "user123", "content": "삼성전자 주가는 70,000원입니다.", "q/a": "A"},
    ]
    
    for i, data in enumerate(test_pairs):
        date_value = datetime.fromisoformat(data['date'])
        pk_date = f"{str(date_value.year)}{str(date_value.month).zfill(2)}{str(date_value.day).zfill(2)}"
        conv_id = pk_date + '_' + str(i).zfill(5)
        
        print(f"데이터 {i+1}:")
        print(f"  conv_id: {conv_id}")
        print(f"  Q/A: {data['q/a']}")
        print(f"  내용: {data['content']}")
        print()
    
    print("✅ Q와 A가 연속된 conv_id를 가지므로 연결 가능")

if __name__ == "__main__":
    test_hash_generation()
    test_conv_id_generation()
