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

records = []
for r in data:
    # tenant_id 처리 (ibk, ibks 모두 지원)
    tenant_id = r.get("tenant_id", "ibk")  # 기본값은 ibk
    if tenant_id not in ["ibk", "ibks"]:
        tenant_id = "ibk"  # 알 수 없는 경우 ibk로 설정
    
    records.append({"date": r["date"], "q/a": "Q", "content": r["Q"], "user_id": r["user_id"], "tenant_id": tenant_id})
    records.append({"date": r["date"], "q/a": "A", "content": r["A"], "user_id": r["user_id"], "tenant_id": tenant_id})

input_data = pd.DataFrame(records, columns=["date", "q/a", "content", "user_id", "tenant_id"])
input_data = input_data[['date', 'q/a', 'content', 'user_id', 'tenant_id']]

# 날짜별 독립적인 카운터를 위한 딕셔너리
date_counters = {}

conv_ids = []
for idx in tqdm(range(len(input_data))):   # 챗봇 대화 로그 데이터에 PK 추가 
    date_str = input_data['date'][idx]
    date_value = datetime.fromisoformat(date_str)
    
    # UTC를 한국 시간(KST)으로 변환
    if date_value.tzinfo is None:
        # timezone 정보가 없으면 UTC로 가정
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

print(conv_ids[-3:])
print(conv_ids[:3])
# tenant_id, Q, A, date, user_id 