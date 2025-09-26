#!/usr/bin/env python3
"""
DB에 저장된 hash_value와 hash_ref 데이터 확인 스크립트
"""

import json
import psycopg2
from src.database import DBConnection

def check_hash_data():
    """DB에서 hash_value와 hash_ref 데이터 상태 확인"""
    
    # DB 설정 로드
    with open('config/db_config.json', 'r') as f:
        db_config = json.load(f)
    
    # DB 연결
    db_conn = DBConnection(db_config)
    db_conn.connect()
    
    try:
        # 1. 전체 레코드 수 확인
        db_conn.cur.execute("SELECT COUNT(*) FROM ibk_convlog")
        total_count = db_conn.cur.fetchone()[0]
        print(f"📊 전체 레코드 수: {total_count:,}")
        
        # 2. hash_value가 NULL인 레코드 수
        db_conn.cur.execute("SELECT COUNT(*) FROM ibk_convlog WHERE hash_value IS NULL")
        null_hash_count = db_conn.cur.fetchone()[0]
        print(f"❌ hash_value가 NULL인 레코드: {null_hash_count:,}")
        
        # 3. hash_ref가 NULL인 레코드 수
        db_conn.cur.execute("SELECT COUNT(*) FROM ibk_convlog WHERE hash_ref IS NULL")
        null_ref_count = db_conn.cur.fetchone()[0]
        print(f"❌ hash_ref가 NULL인 레코드: {null_ref_count:,}")
        
        # 4. Q&A 타입별 통계
        db_conn.cur.execute("""
            SELECT qa, 
                   COUNT(*) as total,
                   COUNT(hash_value) as has_hash_value,
                   COUNT(hash_ref) as has_hash_ref
            FROM ibk_convlog 
            GROUP BY qa
        """)
        qa_stats = db_conn.cur.fetchall()
        print(f"\n📈 Q/A 타입별 통계:")
        for qa, total, has_hash, has_ref in qa_stats:
            print(f"   {qa}: 전체 {total:,}개, hash_value {has_hash:,}개, hash_ref {has_ref:,}개")
        
        # 5. 최근 데이터 샘플 확인 (Q&A 쌍)
        print(f"\n🔍 최근 데이터 샘플 (Q&A 쌍):")
        db_conn.cur.execute("""
            SELECT conv_id, qa, hash_value, hash_ref, 
                   LEFT(content, 50) as content_preview,
                   user_id, date
            FROM ibk_convlog 
            ORDER BY date DESC 
            LIMIT 10
        """)
        recent_data = db_conn.cur.fetchall()
        
        for row in recent_data:
            conv_id, qa, hash_value, hash_ref, content_preview, user_id, date = row
            print(f"   {conv_id} | {qa} | hash_value: {hash_value[:8] if hash_value else 'NULL'}... | hash_ref: {hash_ref[:8] if hash_ref else 'NULL'}... | {content_preview}...")
        
        # 6. Q&A 연결 상태 확인
        print(f"\n🔗 Q&A 연결 상태 확인:")
        db_conn.cur.execute("""
            SELECT 
                COUNT(CASE WHEN qa = 'Q' THEN 1 END) as q_count,
                COUNT(CASE WHEN qa = 'A' AND hash_ref IS NOT NULL THEN 1 END) as a_with_ref,
                COUNT(CASE WHEN qa = 'A' AND hash_ref IS NULL THEN 1 END) as a_without_ref
            FROM ibk_convlog
        """)
        link_stats = db_conn.cur.fetchone()
        q_count, a_with_ref, a_without_ref = link_stats
        print(f"   질문(Q): {q_count:,}개")
        print(f"   답변(A) - hash_ref 있음: {a_with_ref:,}개")
        print(f"   답변(A) - hash_ref 없음: {a_without_ref:,}개")
        
        # 7. 연결되지 않은 Q&A 쌍 찾기
        print(f"\n⚠️ 연결되지 않은 Q&A 쌍 샘플:")
        db_conn.cur.execute("""
            SELECT conv_id, qa, hash_value, hash_ref, 
                   LEFT(content, 30) as content_preview
            FROM ibk_convlog 
            WHERE qa = 'A' AND hash_ref IS NULL
            ORDER BY date DESC 
            LIMIT 5
        """)
        unlinked_a = db_conn.cur.fetchall()
        
        for row in unlinked_a:
            conv_id, qa, hash_value, hash_ref, content_preview = row
            print(f"   {conv_id} | {qa} | hash_ref: NULL | {content_preview}...")
        
        # 8. 중복 hash_value 확인
        print(f"\n🔄 중복 hash_value 확인:")
        db_conn.cur.execute("""
            SELECT hash_value, COUNT(*) as count
            FROM ibk_convlog 
            WHERE hash_value IS NOT NULL
            GROUP BY hash_value 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 5
        """)
        duplicates = db_conn.cur.fetchall()
        
        if duplicates:
            print(f"   중복된 hash_value 발견:")
            for hash_val, count in duplicates:
                print(f"   {hash_val[:8]}... : {count}개")
        else:
            print(f"   중복된 hash_value 없음 ✅")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        db_conn.close()

if __name__ == "__main__":
    check_hash_data()
