#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 디버깅 테스트 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src import APIPipeline
from dotenv import load_dotenv

def test_api_connection():
    """API 연결을 테스트합니다."""
    
    load_dotenv()
    bearer_token = os.getenv("BEARER_TOKEN")
    
    if not bearer_token:
        print("❌ BEARER_TOKEN이 설정되지 않았습니다.")
        return
    
    print(f"✅ BEARER_TOKEN 확인: {bearer_token[:10]}...")
    
    # APIPipeline 초기화
    api_pipeline = APIPipeline(bearer_token)
    
    # 테스트 날짜 (get_data_api.py에서 사용한 날짜)
    test_date = "2025-09-18"
    print(f"📅 테스트 날짜: {test_date}")
    
    # API 데이터 가져오기
    print("\n🔍 API 데이터 가져오기...")
    api_data = api_pipeline.get_data(date=test_date, tenant_id='ibk')
    
    if api_data and len(api_data) > 0:
        print(f"✅ API에서 {len(api_data)}개의 데이터를 받았습니다.")
        
        # 첫 번째 데이터 샘플 출력
        if len(api_data) > 0:
            print(f"\n📋 첫 번째 데이터 샘플:")
            print(f"   키: {list(api_data[0].keys())}")
            print(f"   값: {api_data[0]}")
        
        # 데이터 처리 테스트
        print(f"\n🔄 데이터 처리 중...")
        processed_data = api_pipeline.process_data(api_data)
        print(f"✅ 처리된 데이터 shape: {processed_data.shape}")
        
        if not processed_data.empty:
            print(f"\n📊 처리된 데이터 샘플:")
            print(processed_data.head())
        else:
            print("❌ 처리된 데이터가 비어있습니다.")
    else:
        print("❌ API에서 데이터를 받지 못했습니다.")

if __name__ == "__main__":
    test_api_connection()
