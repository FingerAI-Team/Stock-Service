from dotenv import load_dotenv
import requests
import os 
from tqdm import tqdm
import pandas as pd
from datetime import datetime

load_dotenv()
bearer_token = os.getenv("BEARER_TOKEN")
url = "https://chat-api.ibks.onelineai.com/api/ibk_securities/admin/logs?tenant_id=ibk"

from_date_utc = "2025-09-1"
to_date_utc = "2025-09-3"

request_url = f"{url}&from_date_utc={from_date_utc}&to_date_utc={to_date_utc}"
headers = {
    "Authorization": f"Bearer {bearer_token}"
}

response = requests.get(request_url, headers=headers)
data = response.json()
print(len(data))
print(data[1].keys())

records = []
for r in data:
    records.append({"date": r["date"], "q/a": "Q", "content": r["Q"], "user_id": r["user_id"]})
    records.append({"date": r["date"], "q/a": "A", "content": r["A"], "user_id": r["user_id"]})

input_data = pd.DataFrame(records, columns=["date", "q/a", "content", "user_id"])
input_data = input_data[['date', 'q/a', 'content', 'user_id']]

conv_ids = []
for idx in tqdm(range(len(input_data))):   # 챗봇 대화 로그 데이터에 PK 추가 
    # print(input_data['date'][idx])
    date_str = input_data['date'][idx]
    date_value = datetime.fromisoformat(date_str)
    pk_date = f"{str(date_value.year)}{str(date_value.month).zfill(2)}{str(date_value.day).zfill(2)}"
    conv_id = pk_date + '_' + str(idx).zfill(5)
    conv_ids.append(conv_id)

print(conv_ids[:20])
# tenant_id, Q, A, date, user_id 