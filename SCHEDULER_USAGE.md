# 📅 APScheduler를 이용한 자동 데이터 수집

## 🚀 사용법

### 1. 일회성 실행 (기존 방식)
```bash
python store_convlog_api.py --process daily
```

### 2. 스케줄링 실행

#### 매시간 실행 (기본값)
```bash
python store_convlog_api.py --process scheduled --schedule_type hourly
```

#### 30분마다 실행
```bash
python store_convlog_api.py --process scheduled --schedule_type every_30min
```

#### 15분마다 실행
```bash
python store_convlog_api.py --process scheduled --schedule_type every_15min
```

#### 매일 특정 시간 실행
```bash
python store_convlog_api.py --process scheduled --schedule_type daily
```

#### 업무시간만 실행 (9-17시, 30분마다)
```bash
python store_convlog_api.py --process scheduled --schedule_type business_hours
```

## 📋 사용 가능한 스케줄 옵션

| 옵션 | 설명 | 실행 시간 |
|------|------|-----------|
| `hourly` | 매시간 5분에 실행 | 00:05, 01:05, 02:05, ... |
| `daily` | 매일 5시 10분에 실행 | 05:10 |
| `every_30min` | 30분마다 실행 | 00:00, 00:30, 01:00, 01:30, ... |
| `every_15min` | 15분마다 실행 | 00:00, 00:15, 00:30, 00:45, ... |
| `business_hours` | 업무시간 30분마다 | 09:00, 09:30, 10:00, ..., 17:00 |

## 🔧 고급 설정

### 사용자 정의 스케줄
`scheduler_config.py` 파일을 수정하여 새로운 스케줄 옵션을 추가할 수 있습니다.

```python
'custom_schedule': {
    'trigger': CronTrigger(hour=14, minute=30),  # 매일 14시 30분
    'description': '매일 오후 2시 30분에 실행'
}
```

## 📊 모니터링

### 로그 확인
```bash
tail -f app.log
```

### 실행 상태 확인
스케줄러 실행 시 다음과 같은 정보가 출력됩니다:
```
📅 사용 가능한 스케줄 옵션들:
   hourly: 매시간 5분에 실행
   daily: 매일 5시 10분에 실행
   ...

✅ 선택된 스케줄: 매시간 5분에 실행
🕐 APScheduler가 시작되었습니다. 매시간 5분에 데이터를 수집합니다.
📅 예정된 작업들:
   - 데이터 수집 (hourly): 2025-01-27 15:05:00
```

## ⚠️ 주의사항

1. **중복 실행 방지**: `max_instances=1` 설정으로 동시 실행을 방지합니다.
2. **중복 데이터 방지**: 이미 존재하는 데이터는 자동으로 건너뛰어집니다.
3. **로그 관리**: `app.log` 파일이 계속 커지므로 주기적으로 로그 로테이션을 설정하세요.

## 🛑 스케줄러 종료

`Ctrl+C`를 눌러 스케줄러를 안전하게 종료할 수 있습니다.

## 🔄 백그라운드 실행 (Linux/Mac)

```bash
# nohup으로 백그라운드 실행
nohup python store_convlog_api.py --process scheduled --schedule_type hourly > scheduler.log 2>&1 &

# 프로세스 ID 확인
ps aux | grep store_convlog_api

# 프로세스 종료
kill <PID>
```

## 🐳 Docker에서 실행

```bash
# Docker 컨테이너에서 스케줄러 실행
docker exec -d <container_name> python store_convlog_api.py --process scheduled --schedule_type hourly
```
