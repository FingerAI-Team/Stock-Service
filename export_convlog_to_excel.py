#!/usr/bin/env python3
"""
convlog 데이터를 엑셀 파일로 내보내는 스크립트
"""

import json
import pandas as pd
from datetime import datetime
from src.database import DBConnection

def export_convlog_to_excel(output_file=None, limit=None, date_filter=None):
    """
    convlog 데이터를 엑셀 파일로 내보내기
    
    Args:
        output_file (str): 출력 파일명 (기본값: convlog_export_YYYYMMDD_HHMMSS.xlsx)
        limit (int): 가져올 레코드 수 제한 (기본값: None, 전체)
        date_filter (str): 날짜 필터 (YYYY-MM-DD 형식, 기본값: None, 전체)
    """
    
    # DB 설정 로드
    with open('config/db_config.json', 'r') as f:
        db_config = json.load(f)
    
    # DB 연결
    db_conn = DBConnection(db_config)
    db_conn.connect()
    
    try:
        # 기본 쿼리
        query = """
        SELECT 
            conv_id,
            date,
            qa,
            content,
            user_id,
            tenant_id,
            hash_value,
            hash_ref
        FROM ibk_convlog
        """
        
        # 날짜 필터 추가
        if date_filter:
            query += f" WHERE DATE(date) = '{date_filter}'"
        
        # 정렬
        query += " ORDER BY date DESC, conv_id"
        
        # 제한 추가
        if limit:
            query += f" LIMIT {limit}"
        
        print(f"🔍 쿼리 실행 중...")
        print(f"📋 쿼리: {query}")
        
        # 데이터 조회
        db_conn.cur.execute(query)
        results = db_conn.cur.fetchall()
        
        if not results:
            print("❌ 조회된 데이터가 없습니다.")
            return
        
        # 컬럼명 정의
        columns = [
            'conv_id', 'date', 'qa', 'content', 'user_id', 
            'tenant_id', 'hash_value', 'hash_ref'
        ]
        
        # DataFrame 생성
        df = pd.DataFrame(results, columns=columns)
        
        # 기본 출력 파일명 생성
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"convlog_export_{timestamp}.xlsx"
        
        # 엑셀 파일로 저장
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 전체 데이터 시트
            df.to_excel(writer, sheet_name='전체데이터', index=False)
            
            # Q&A 연결 통계 시트
            q_count = len(df[df['qa'] == 'Q'])
            a_count = len(df[df['qa'] == 'A'])
            a_with_ref = len(df[(df['qa'] == 'A') & (df['hash_ref'].notna())])
            
            stats_data = {
                '항목': ['전체 레코드', '질문(Q)', '답변(A)', '답변 중 hash_ref 있음', '답변 중 hash_ref 없음'],
                '개수': [len(df), q_count, a_count, a_with_ref, a_count - a_with_ref]
            }
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='통계', index=False)
            
            # Q&A 쌍 분석 시트
            qa_pairs = []
            for _, row in df.iterrows():
                if row['qa'] == 'Q':
                    # 해당 Q에 대한 A 찾기
                    corresponding_a = df[
                        (df['qa'] == 'A') & 
                        (df['hash_ref'] == row['hash_value'])
                    ]
                    
                    if not corresponding_a.empty:
                        a_row = corresponding_a.iloc[0]
                        qa_pairs.append({
                            'conv_id_q': row['conv_id'],
                            'conv_id_a': a_row['conv_id'],
                            'date': row['date'],
                            'user_id': row['user_id'],
                            'tenant_id': row['tenant_id'],
                            'question': row['content'][:100] + '...' if len(row['content']) > 100 else row['content'],
                            'answer': a_row['content'][:100] + '...' if len(a_row['content']) > 100 else a_row['content'],
                            'hash_value_q': row['hash_value'],
                            'hash_value_a': a_row['hash_value'],
                            'hash_ref': a_row['hash_ref']
                        })
            
            if qa_pairs:
                qa_pairs_df = pd.DataFrame(qa_pairs)
                qa_pairs_df.to_excel(writer, sheet_name='Q&A쌍', index=False)
            
            # 연결되지 않은 A 레코드 시트
            unlinked_a = df[(df['qa'] == 'A') & (df['hash_ref'].isna())]
            if not unlinked_a.empty:
                unlinked_a.to_excel(writer, sheet_name='연결되지않은A', index=False)
        
        print(f"✅ 엑셀 파일 생성 완료: {output_file}")
        print(f"📊 내보낸 데이터:")
        print(f"   - 전체 레코드: {len(df):,}개")
        print(f"   - 질문(Q): {q_count:,}개")
        print(f"   - 답변(A): {a_count:,}개")
        print(f"   - 답변 중 hash_ref 있음: {a_with_ref:,}개")
        print(f"   - 답변 중 hash_ref 없음: {a_count - a_with_ref:,}개")
        print(f"   - Q&A 쌍: {len(qa_pairs):,}개")
        
        if date_filter:
            print(f"📅 날짜 필터: {date_filter}")
        if limit:
            print(f"🔢 제한: {limit:,}개")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        db_conn.close()

def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='convlog 데이터를 엑셀로 내보내기')
    parser.add_argument('--output', '-o', help='출력 파일명')
    parser.add_argument('--limit', '-l', type=int, help='가져올 레코드 수 제한')
    parser.add_argument('--date', '-d', help='날짜 필터 (YYYY-MM-DD 형식)')
    
    args = parser.parse_args()
    
    export_convlog_to_excel(
        output_file=args.output,
        limit=args.limit,
        date_filter=args.date
    )

if __name__ == "__main__":
    main()
