from dotenv import load_dotenv
import requests
import os 
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta, timezone

load_dotenv()
bearer_token = os.getenv("BEARER_TOKEN")
url = "https://chat-api.ibks.onelineai.com/api/ibk_securities/admin/logs?tenant_id=ibks"

# API 호출용 날짜 설정 (KST 변환 없이 그대로 사용)
from_date = "2025-09-22"
to_date = "2025-09-23"

# KST timezone 정의 (데이터 변환용)
kst = timezone(timedelta(hours=9))

print(f"📅 API 요청 날짜: {from_date} ~ {to_date}")

request_url = f"{url}&from_date_utc={from_date}&to_date_utc={to_date}"
headers = {
    "Authorization": f"Bearer {bearer_token}"
}

response = requests.get(request_url, headers=headers)
data = response.json()
print(len(data))
print(data[1].keys())
print(data[1]['tenant_id'])

import hashlib

records = []
for r in data:
    # tenant_id 처리 (ibk, ibks 모두 지원)
    tenant_id = r.get("tenant_id", "ibk")  # 기본값은 ibk
    if tenant_id not in ["ibk", "ibks"]:
        tenant_id = "ibk"  # 알 수 없는 경우 ibk로 설정
    
    # Q와 A의 해시값을 미리 생성
    q_hash = hashlib.md5(f"{r['user_id']}_{r['Q']}_{r['date']}".encode()).hexdigest()
    a_hash = hashlib.md5(f"{r['user_id']}_{r['A']}_{r['date']}".encode()).hexdigest()
    
    records.append({
        "date": r["date"], 
        "q/a": "Q", 
        "content": r["Q"], 
        "user_id": r["user_id"], 
        "tenant_id": tenant_id,
        "hash_value": q_hash,
        "hash_ref": None  # Q는 hash_ref가 NULL
    })
    records.append({
        "date": r["date"], 
        "q/a": "A", 
        "content": r["A"], 
        "user_id": r["user_id"], 
        "tenant_id": tenant_id,
        "hash_value": a_hash,
        "hash_ref": q_hash  # A는 Q의 hash_value를 hash_ref로
    })

input_data = pd.DataFrame(records, columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
input_data = input_data[['date', 'q/a', 'content', 'user_id', 'tenant_id', 'hash_value', 'hash_ref']]

print(input_data[['q/a', 'content', 'hash_value', 'hash_ref']].head())

# 날짜별 독립적인 카운터를 위한 딕셔너리
date_counters = {}

# conv_id 생성 및 KST 변환
conv_ids = []
for idx in tqdm(range(len(input_data))):
    date_value = datetime.fromisoformat(input_data['date'][idx])
    
    # UTC를 한국 시간(KST)으로 변환
    if date_value.tzinfo is None:
        date_value = date_value.replace(tzinfo=timezone.utc)
    kst_date = date_value.astimezone(kst)
    
    # date 컬럼에 저장할 값도 KST로 변환
    input_data.at[idx, 'date'] = kst_date.isoformat()
    
    # 한국 시간 기준으로 날짜별 독립적인 conv_id 생성
    pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
    
    # 날짜별로 독립적인 카운터 사용
    if pk_date not in date_counters:
        date_counters[pk_date] = 0
    else:
        date_counters[pk_date] += 1
    
    conv_id = pk_date + '_' + str(date_counters[pk_date]).zfill(5)
    conv_ids.append(conv_id)

input_data.insert(0, 'conv_id', conv_ids)

# Q&A 연결 통계
q_count = sum(1 for qa in input_data['q/a'] if qa == 'Q')
a_count = sum(1 for qa in input_data['q/a'] if qa == 'A')
a_with_ref = sum(1 for ref in input_data['hash_ref'] if ref is not None)
print(f"📊 Q&A 연결 통계: Q {q_count}개, A {a_count}개, A에 hash_ref 있음 {a_with_ref}개")

print(f"🔍 최종 데이터 shape: {input_data.shape}")
print(f"🔍 컬럼: {list(input_data.columns)}")
print(f"🔍 conv_id 샘플: {conv_ids[:3]} ... {conv_ids[-3:]}") 