#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
스케줄러 설정 파일
"""

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# 스케줄링 옵션들
SCHEDULE_OPTIONS = {
    'hourly': {
        'trigger': CronTrigger(minute=5),  # 매시 5분
        'description': '매시간 5분에 실행'
    },
    'hourly_6min': {
        'trigger': CronTrigger(minute=6),  # 매시 6분
        'description': '매시간 6분에 실행'
    },
    'daily': {
        'trigger': CronTrigger(hour=5, minute=10),  # 매일 5시 10분
        'description': '매일 5시 10분에 실행'
    },
    'every_30min': {
        'trigger': CronTrigger(minute='*/30'),  # 30분마다
        'description': '30분마다 실행'
    },
    'every_15min': {
        'trigger': CronTrigger(minute='*/15'),  # 15분마다
        'description': '15분마다 실행'
    },
    'business_hours': {
        'trigger': CronTrigger(hour='9-17', minute='*/30'),  # 업무시간(9-17시) 30분마다
        'description': '업무시간(9-17시) 30분마다 실행'
    },
    'custom': {
        'trigger': None,  # 사용자 정의
        'description': '사용자 정의 스케줄'
    }
}

def get_schedule_config(schedule_type='hourly'):
    """
    스케줄 타입에 따른 설정을 반환합니다.
    
    Args:
        schedule_type (str): 스케줄 타입
        
    Returns:
        dict: 스케줄 설정
    """
    return SCHEDULE_OPTIONS.get(schedule_type, SCHEDULE_OPTIONS['hourly'])

def print_available_schedules():
    """사용 가능한 스케줄 옵션들을 출력합니다."""
    print("📅 사용 가능한 스케줄 옵션들:")
    for key, config in SCHEDULE_OPTIONS.items():
        print(f"   {key}: {config['description']}")

if __name__ == "__main__":
    print_available_schedules()
