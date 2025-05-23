from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta
from datasets import Dataset, DatasetDict
import pandas as pd
import re

class DataProcessor:
    def data_to_df(self, dataset, columns):
        if isinstance(dataset, list):
            return pd.DataFrame(dataset, columns=columns)
       
    def df_to_hfdata(self, df):
        return Dataset.from_pandas(df)

    def merge_data(self, df1, df2, how='inner', on=None):
        return pd.merge(df1, df2, how='inner')

    def filter_data(self, df, col, val):
        return df[df[col]==val].reset_index(drop=True)
    
    def remove_keywords(self, df, col, keyword=None, exceptions=None):
        if exceptions != None: 
            if keyword != None:
                # pattern = r'(?<![\w가-힣])(?:' + '|'.join(map(re.escape, keyword)) + r')(?![\w가-힣])'
                pattern = re.compile(r'(?<![\w가-힣])(' + '|'.join(map(re.escape, val)) + r')(?=[^가-힣]|$)')
            else:
                pattern = r'(?<![\w가-힣])(\S*주)(?![\w가-힣])'    # 테마주 같은 함정 증권 종목 제거 
            mask = df[col].str.contains(pattern, na=False) & ~df[col].str.contains('|'.join(map(re.escape, exceptions)), na=False)
            df = df[~mask]
            return df.reset_index(drop=True)
        else: 
            keyword_idx = df[df[col].str.contains(keyword, na=False)].index 
            df.drop(keyword_idx, inplace=True)
            return df.reset_index(drop=True)
        
    def train_test_split(self, dataset, x_col, y_col, test_size, val_test_size, random_state=42):
        X, X_test, y, y_test = train_test_split(dataset[x_col], dataset[y_col], test_size=0.2, stratify=dataset[y_col], random_state=random_state)
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=val_test_size, stratify=y, random_state=random_state)
        return X, X_val, X_test, y, y_val, y_test  


class TextProcessor:
    def count_pattern(self, text, patterns):
        cnt = 0 
        for pattern in sorted(patterns, reverse=True):
            if pattern in text: 
                cnt += 1 
                text = text.replace(pattern, '', 1)
        return cnt 
                   
    def remove_duplications(self, text):
        '''
        줄바꿈 문자를 비롯한 특수 문자 중복을 제거합니다.
        '''
        text = re.sub(r'(\n\s*)+\n+', '\n\n', text)    # 다중 줄바꿈 문자 제거
        text = re.sub(r"\·{1,}", " ", text)    
        return re.sub(r"\.{1,}", ".", text)
        
    def remove_patterns(self, text, pattern):
        '''
        입력된 패턴을 제거합니다. 
        pattern:
        r'^\d+\.\s*': "숫자 + ." 
        r"(뉴스|주식|정보|분석)$": 삼성전자뉴스 -> 삼성전자
        '''
        return re.sub(pattern, '', text)
    
    def check_expr(self, expr, text):
        '''
        expr 값이 text에 있는지 검사합니다. 있다면 True를, 없다면 False를 반환합니다. 
        '''
        return bool(re.search(expr, text))
    
    def get_val(self, val, text):
        '''
        expr 값이 text에 있으면 반환합니다.  
        '''
        if isinstance(val, str):
            return re.search(rf'\b{re.escape(val)}\b')
        elif isinstance(val, list):
            return [re.search(rf'\b{re.escape(v)}\b') for v in val]
    
    def get_val_with_indices(self, val, text):
        '''
        val 값이 text에 있으면 시작과 끝 위치 정보와 함께 값을 반환합니다.
        '''
        found_stocks = []
        if isinstance(val, str):
            pattern = rf'(^|[^a-zA-Z0-9가-힣]){re.escape(val)}($|[^a-zA-Z0-9가-힣])'
            matches = list(re.finditer(pattern, text))
            for match in matches:
                # 매칭된 텍스트에서 실제 단어의 시작과 끝 위치 조정
                start = match.start() + (1 if match.group(1) else 0)
                end = match.end() - (1 if match.group(2) else 0)
                found_stocks.append((val, start, end))
            return found_stocks
        elif isinstance(val, list):
            pattern = re.compile(r'\b(?:' + '|'.join(map(re.escape, val)) + r')\b')
            matches = [(match.group(), match.start(), match.end()) for match in pattern.finditer(text)]
            return matches 
            
    def check_l2_threshold(self, txt, threshold, value):
        '''   
        threshold 보다 값이 높은 경우, 모르는 정보로 간주합니다. 
        '''
        print(f'Euclidean Distance: {value}, Threshold: {threshold}')
        return "모르는 정보입니다." if value > threshold else txt

class VecProcessor:
    '''
    임베딩 유사도 계산 및 임계
    '''
    pass

class TimeProcessor:
    def get_previous_day_date(self):
        '''
        전일 연도, 월, 일을 반환합니다.   
        returns: 
        20240103
        '''
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        return str(yesterday.year), str(yesterday.month).zfill(2), str(yesterday.day).zfill(2)

    def get_current_date(self):
        '''
        현재 연도, 월, 일을 반환합니다.
        '''
        now = datetime.now()
        return str(now.year), str(now.month).zfill(2), str(now.day).zfill(2)




class ETC:
    '''
    당장 쓰이지 않는 메서드 정의
    '''
    def get_model_response(self, df, user_id, query):
        qa_pairs = []
        current_question = None
        question_time = None

        user_df = df[df['user_id'] == user_id].reset_index(drop=True)
        user_df = user_df.sort_values('date')
        # user_df.to_csv('./debug_user.csv', index=False)
        
        for i, row in user_df.iterrows():   # 질문-응답 매칭 루프
            if row['q/a'] == 'Q' and row['content'] == query:
                current_question = row['content']
                question_time = row['date']
            elif row['q/a'] == 'A' and current_question is not None:
                response_time = row['date']   # 질문에 대한 응답을 기록
                time_diff = response_time - question_time   # 시간 차이가 많이 나지 않는 경우에만 질문과 응답을 매칭
                if time_diff.seconds < 300:  # 5분 이내
                    qa_pairs.append({
                        'question': current_question,
                        'answer': row['content'],
                        'question_time': question_time,
                        'answer_time': response_time
                    })
                current_question = None   # 응답 처리 후 질문 초기화
                question_time = None
        return qa_pairs
