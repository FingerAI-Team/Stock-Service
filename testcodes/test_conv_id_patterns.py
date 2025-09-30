#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
conv_id 패턴 테스트 스크립트
"""

import re

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

def test_conv_id_patterns():
    """conv_id 패턴 테스트"""
    
    test_cases = [
        # 정상 데이터 (유지해야 함)
        "20250922_00002",
        "20250922_00004", 
        "20250922_00005",
        "20250127_00001",
        "20250127_12345",
        
        # 잘못된 데이터 (제거해야 함)
        "20250922_oITQ3kOOniCWCOUpyWz6CQkAcHuJ5i8ARoOBarJjnB0nqTOJgfIi3g8z0SFRO71xFlNGX0EzlRsPDBdj09JmLw==_34135619",
        "20250922_oITQ3kOOniCWCOUpyWz6CQkAcHuJ5i8ARoOBarJjnB0nqTOJgfIi3g8z0SFRO71xFlNGX0EzlRsPDBdj09JmLw==_2a75cec4",
        "20250922_oITQ3kOOniCWCOUpyWz6CQkAcHuJ5i8ARoOBarJjnB0nqTOJgfIi3g8z0SFRO71xFlNGX0EzlRsPDBdj09JmLw==_16c5f553",
        "20250922_oITQ3kOOniCWCOUpyWz6CQkAcHuJ5i8ARoOBarJjnB0nqTOJgfIi3g8z0SFRO71xFlNGX0EzlRsPDBdj09JmLw==_b32fb3f5",
        
        # 기타 테스트 케이스
        "20250922_user123_abc12345",  # 잘못된 형식
        "20250922_00001",             # 정상 형식
        "invalid_format",             # 잘못된 형식
    ]
    
    print("🧪 conv_id 패턴 테스트")
    print("=" * 80)
    
    for conv_id in test_cases:
        is_valid = is_valid_conv_id(conv_id)
        is_hash_containing = is_hash_containing_conv_id(conv_id)
        
        if is_valid:
            status = "✅ 정상 (유지)"
        elif is_hash_containing:
            status = "❌ 잘못된 형식 (제거)"
        else:
            status = "⚠️ 기타 형식"
        
        print(f"{conv_id:<80} → {status}")
    
    print("\n📋 요약:")
    print("✅ 정상 형식: YYYYMMDD_XXXXX (날짜_5자리숫자)")
    print("❌ 잘못된 형식: YYYYMMDD_긴문자열_해시값")
    print("⚠️ 기타 형식: 위 두 패턴에 해당하지 않는 형식")

if __name__ == "__main__":
    test_conv_id_patterns()
