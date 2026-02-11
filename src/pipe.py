from .preprocessor import DataProcessor, TextProcessor, VecProcessor, TimeProcessor
from .encoder import KFDeBERTaTokenizer, KFDeBERTa, ModelTrainer, ModelPredictor
from .database import PostgresDB, DBConnection, TableEditor
from .llm import LLMOpenAI
from datasets import Dataset, DatasetDict
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
import time
import json
import os
import requests
import logging
from datetime import datetime, timezone, timedelta


class EnvManager:
    def __init__(self, args):
        load_dotenv()
        self.args = args 
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.bearer_token = os.getenv("BEARER_TOKEN")
        if not self.openai_api_key:
            raise ValueError("OpenAI API 키가 로드되지 않았습니다.")
        
        self.tickle_list = self.__load_tickle_list()
        self.model_config, self.db_config = self.__load_configs()
        self.conv_tb_name, self.cls_tb_name, self.clicked_tb_name = 'ibk_convlog', 'ibk_stock_cls', 'ibk_clicked_tb'   

    def __load_configs(self):
        '''
        llm 모델과 db 설정 파일을 로드합니다.
        '''
        with open(os.path.join(self.args.config_path, 'llm_config.json')) as f:
            llm_config = json.load(f)
 
        with open(os.path.join(self.args.config_path, 'db_config.json')) as f:
            db_config = json.load(f)
        return llm_config, db_config

    def __load_tickle_list(self):
        '''
        국내 증권 종목 & 해외 증권 종목 리스트를 로드합니다. 
        returns:
        list[str]: 증권 종목 이름, 코드 값
        '''
        tickles = pd.read_csv(os.path.join('./', 'tickle', 'tickle-final.csv'))
        tickles.dropna(inplace=True)
        return tickles['tickle'].values.tolist()

class PreProcessor:
    def initialize_processor(self):
        return DataProcessor(), TextProcessor(), VecProcessor(), TimeProcessor()
        
class DBManager:
    def __init__(self, db_config):
        self.db_config = db_config

    def initialize_database(self):
        db_connection = DBConnection(self.db_config)
        db_connection.connect()
        postgres = PostgresDB(db_connection)
        table_editor = TableEditor(db_connection)
        return postgres, table_editor


class APIPipeline:
    def __init__(self, bearer_tok):
        self.bearer_tok = bearer_tok 

    def get_data_range(self, start_date, end_date, tenant_id='ibk'):
        url = f"https://chat-api.ibks.onelineai.com/api/ibk_securities/admin/logs?tenant_id={tenant_id}"
        # 다음 날을 to_date로 설정 (get_data_api.py와 동일한 방식)
        request_url = f"{url}&from_date_utc={start_date}&to_date_utc={end_date}"
        headers = {
            "Authorization": f"Bearer {self.bearer_tok}"
        }
        print(f"API 요청 URL: {request_url}")
        print(f"Bearer Token: {self.bearer_tok[:10]}..." if self.bearer_tok else "Bearer Token 없음")
        try:
            response = requests.get(request_url, headers=headers)
            print(f"API 응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"API 응답 데이터 타입: {type(data)}")
                return data
            else:
                print(f"API 요청 실패: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            print(f"API 요청 중 오류 발생: {str(e)}")
            return []
    
    def get_data(self, date, tenant_id='ibk'):
        url = f"https://chat-api.ibks.onelineai.com/api/ibk_securities/admin/logs?tenant_id={tenant_id}"
        # 다음 날을 to_date로 설정 (get_data_api.py와 동일한 방식)
        from datetime import datetime, timedelta
        from_date = datetime.strptime(date, "%Y-%m-%d")
        end_date = from_date + timedelta(days=1)
        request_url = f"{url}&from_date_utc={from_date}&to_date_utc={end_date}"
        headers = {
            "Authorization": f"Bearer {self.bearer_tok}"
        }
        print(f"API 요청 URL: {request_url}")
        print(f"Bearer Token: {self.bearer_tok[:10]}..." if self.bearer_tok else "Bearer Token 없음")
        try:
            response = requests.get(request_url, headers=headers)
            print(f"API 응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"API 응답 데이터 타입: {type(data)}")
                return data
            else:
                print(f"API 요청 실패: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"API 요청 중 오류 발생: {str(e)}")
            return []
    
    def process_data(self, data):
        '''
        api로 받은 데이터를 postgres db에 저장 가능한 형태로 변경  
        date, qa, content, user_id, tenant_id, hash_value, hash_ref: ibk, msty 
        '''
        if not data:
            print("API에서 받은 데이터가 비어있습니다.")
            return pd.DataFrame(columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
            
        import hashlib
        records = []
        for d in data:
            if "Q" in d and "A" in d and "date" in d and "user_id" in d:
                # user_id가 None인 경우 처리
                user_id = d["user_id"] if d["user_id"] is not None else ""
                tenant_id = d.get("tenant_id") if d.get("tenant_id") is not None else None
                
                # Q와 A의 해시값을 미리 생성 (user_id가 None이면 빈 문자열 사용)
                q_hash = hashlib.md5(f"{user_id}_{d['Q']}_{d['date']}".encode()).hexdigest()
                a_hash = hashlib.md5(f"{user_id}_{d['A']}_{d['date']}".encode()).hexdigest()
                
                records.append({
                    "date": d["date"], 
                    "q/a": "Q", 
                    "content": d["Q"], 
                    "user_id": d["user_id"],  # None 값도 그대로 저장 (DB에서 nullable)
                    "tenant_id": tenant_id,
                    "hash_value": q_hash,
                    "hash_ref": None  # Q는 hash_ref가 NULL
                })
                records.append({
                    "date": d["date"], 
                    "q/a": "A", 
                    "content": d["A"], 
                    "user_id": d["user_id"],  # None 값도 그대로 저장 (DB에서 nullable)
                    "tenant_id": tenant_id,
                    "hash_value": a_hash,
                    "hash_ref": q_hash  # A는 Q의 hash_value를 hash_ref로
                })
            else:
                print(f"데이터 구조가 예상과 다릅니다: {d.keys()}")
        if not records:
            print("처리 가능한 레코드가 없습니다.")
            return pd.DataFrame(columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
            
        input_data = pd.DataFrame(records, columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
        print(f"처리된 레코드 수: {len(input_data)}")
        return input_data


class ModelManager:
    def __init__(self, model_config):
        self.model_config = model_config
   
    def set_cls_trainset(self, dataset, dataset2, data_processor):
        '''
        db에서 입력받은 데이터를 학습 데이터세트로 변환합니다. 
        '''
        convlog_data = data_processor.data_to_df(dataset, columns=['conv_id', 'date', 'qa', 'content', 'userid'])
        cls_data = data_processor.data_to_df(dataset2, columns=['conv_id', 'ensemble', 'gpt', 'encoder']) 
        convlog_q = data_processor.filter_data(convlog_data, 'qa', 'Q')
        convlog_trainset = data_processor.merge_data(convlog_q, cls_data, on='conv_id')
        convlog_trainset['label'] = convlog_trainset['ensemble'].apply(lambda x: 'stock' if x == 'o' else 'nstock')
        convlog_trainset = convlog_trainset[['content', 'label']]
        X_train, X_val, X_test, y_train, y_val, y_test = data_processor.train_test_split(convlog_trainset, 'content', 'label', \
                                                                                    0.2, 0.1, self.model_config['random_state'])
        train_df = data_processor.data_to_df(list(zip(X_train, y_train)), columns=['text', 'label']).reset_index(drop=True)
        val_df = data_processor.data_to_df(list(zip(X_val, y_val)), columns=['text', 'label']).reset_index(drop=True)
        test_df = data_processor.data_to_df(list(zip(X_test, y_test)), columns=['text', 'label']).reset_index(drop=True)
        print(len(train_df), len(val_df), len(test_df))
        stock_dict = DatasetDict({
            "train": Dataset.from_pandas(train_df), 
            "val": Dataset.from_pandas(val_df),
            "test": Dataset.from_pandas(test_df)
        })
        return stock_dict

    def set_val_tokenizer(self, val_tok_path):
        '''
        토큰 개수 검사 토크나이저를 로드합니다. 
        '''
        return KFDeBERTaTokenizer(val_tok_path)
    
    def set_encoder(self, model_path):
        return KFDeBERTaTokenizer(model_path), KFDeBERTa(model_path)
        
    def initialize_trainer(self, model_path, model_config, dataset):
        '''
        증권 종목 예측에 사용되는 인코더 모델을 로드합니다. 
        '''
        tokenizer = KFDeBERTaTokenizer(model_path).tokenizer
        kfdeberta = KFDeBERTa(model_path)
        model = kfdeberta.model
        kfdeberta.set_training_config(model_config)
        training_config = kfdeberta.training_args
        trainer = ModelTrainer(tokenizer, model, training_config)
        trainer.setup_trainer(dataset)
        return trainer

    def initialize_predictor(self, model_path):
        tokenizer = KFDeBERTaTokenizer(model_path).tokenizer
        model = KFDeBERTa(model_path).model
        return ModelPredictor(tokenizer=tokenizer, model=model)


class LLMManager:
    def __init__(self, model_config):
        self.model_config = model_config

    def initialize_openai_llm(self):
        '''
        ChatGPT 인스턴스를 생성하고 반환합니다. 
        '''
        openai_llm = LLMOpenAI(self.model_config)
        openai_llm.set_generation_config()
        return openai_llm
        

class PipelineController:
    def __init__(self, env_manager=None, preprocessor=None, db_manager=None, model_manager=None, llm_manager=None):
        self.env_manager = env_manager 
        self.db_manager = db_manager 
        self.preprocessor = preprocessor 
        self.model_manager = model_manager 
        self.llm_manager = llm_manager 
    
    def set_env(self):
        self.tickle_list = self.env_manager.tickle_list 
        self.postgres, self.table_editor = self.db_manager.initialize_database()
        self.data_p, self.text_p, self.vec_p, self.time_p = self.preprocessor.initialize_processor()
        # print(self.model_manager)
        if self.model_manager != None:
            self.val_tokenizer = self.model_manager.set_val_tokenizer(os.path.join(self.env_manager.model_config['model_path'], 'val-tokenizer'))
            self.predictor = self.model_manager.initialize_predictor(os.path.join(self.env_manager.model_config['model_path'], 'kfdeberta', 'model-update'))
            self.openai_llm = self.llm_manager.initialize_openai_llm()

    def process_data(self, input_data):
        '''
        대화 기록을 보고, 해당 대화가 증권 종목 분석 질문인지 아닌지 분류한 후 PostgreSQL 데이터베이스에 저장합니다.  
        증권 종목 분석 질문이거나, 사용자가 앱 내 버튼을 클릭해서 대화를 시작한 경우, 증권 종목 이름을 추출합니다 (향후 구현)
        args:
        input_data (db table): ibk 투자 증권 챗봇을 이용한 사용자들의 대화 로그 [conv_id (pk), date, qa, content, user id]
        
        process:
        Step 1. qa 타입이 'a' (챗봇의 응답) 이거나 이미 데이터베이스에 존재하는 데이터인지 체크한다. 
        Step 2. 그 이외의 경우, 사용자 질문에 대해 encoder 모델, gpt 모델을 활용해 증권 종목 질문 여부를 분류한다. 
          Step 2.1. 성능 개선을 위해, 사용자 질문이 단일 토큰으로 이루어진 경우, tickle list와 매핑해 tickle 관련 질문인지 추가 검사한다.
        Step 3. 사용자가 앱 내 버튼을 클릭한 후 질문을 할 경우, (KR: 333333) 같은 표현값이 대화 기록에 남는다. 이를 활용해 사용자가 
                버튼을 클릭해 들어온 사용자인지 아닌지 분류한다.
        Step 4. 생성한 데이터세트를 PostgreSQL 각 테이블에 저장한다.
        '''
        for idx in tqdm(range(len(input_data))):    
            if input_data[idx][2] == 'A':
                continue
            if self.postgres.check_pk(self.env_manager.cls_tb_name, input_data[idx][0]):    # 데이터베이스에 이미 존재하는 데이터인 경우
                continue

            query = input_data[idx][3]
            print(f'query: {query}')
            encoder_response = self.predictor.predict(query)
            enc_res = 'o' if encoder_response == 'stock' else 'x'

            if len(self.val_tokenizer.tokenize_data(query)) == 1:
                cleaned_word = self.text_p.remove_patterns(query, r"(뉴스|주식|정보|분석)$")    # 불필요한 단어 제거
                enc_res = 'o' if cleaned_word in self.tickle_list else 'x'
            cls_pred_set = (input_data[idx][0], enc_res)  
            TICKLE_PATTERN = r"\b\w+\(KR:\d+\)"
            clicked = 'o' if self.text_p.check_expr(TICKLE_PATTERN, query) else 'x'
            u_id = input_data[idx][4]
            clicked_set = (input_data[idx][0], clicked, u_id)

            self.table_editor.edit_cls_table('insert', self.env_manager.cls_tb_name, data_type='raw', data=cls_pred_set)
            self.table_editor.edit_clicked_table('insert', self.env_manager.clicked_tb_name, data_type='raw', data=clicked_set)

    def run(self, process='daily', query=None):
        '''
        args:
        '''
        if query:
            self.openai_llm.set_stock_guideline()
            if len(self.val_tokenizer.tokenize_data(query)) == 1:
                cleaned_word = self.text_p.remove_patterns(query, r"(뉴스|주식|정보|분석)$")    # 불필요한 단어 제거
                ensembled_res = 'o' if cleaned_word in self.tickle_list else 'x'
                print(f'해당 쿼리는 종목 분석 {ensembled_res} 질문입니다.')
            response = self.openai_llm.get_response(query=query, role=self.openai_llm.system_role, sub_role=self.openai_llm.stock_role)
            print(f'해당 쿼리는 {response} 질문입니다.')
        else:
            yy, mm, dd = self.time_p.get_current_date()
            crawling_date = yy + mm + dd    # 20240201 형식
            input_data = self.postgres.get_total_data(self.env_manager.conv_tb_name) if process == 'code-test' else \
                self.postgres.get_day_data(self.env_manager.conv_tb_name, crawling_date)
            self.process_data(input_data)


class UnifiedPipeline:
    """데이터 수집과 분석을 통합한 파이프라인"""
    
    def __init__(self, args):
        self.args = args
        self.env_manager = EnvManager(args)
        self.preprocessor = PreProcessor()
        self.db_manager = DBManager(self.env_manager.db_config)
        self.model_manager = ModelManager(self.env_manager.model_config)
        self.llm_manager = LLMManager(self.env_manager.model_config)
        self.api_pipeline = APIPipeline(bearer_tok=self.env_manager.bearer_token)
        
        # 통합 파이프라인 컨트롤러
        self.pipe = PipelineController(
            env_manager=self.env_manager,
            preprocessor=self.preprocessor,
            db_manager=self.db_manager,
            model_manager=self.model_manager,
            llm_manager=self.llm_manager
        )
        self.pipe.set_env()
    
    def collect_data(self):
        """데이터 수집 단계"""
        logger = logging.getLogger(__name__)
        logger.info("🚀 데이터 수집 작업이 시작되었습니다.")
        logger.info(f"📅 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.args.process in ['daily', 'scheduled']:
            # API를 통한 데이터 수집
            current_time = datetime.now()
            start_date = current_time.strftime("%Y-%m-%d")
            logger.info(f"📅 데이터 수집 날짜: {start_date}")
            
            # ibk와 ibks 두 tenant_id 모두 수집
            all_api_data = []
            tenant_ids = ['ibk', 'ibks']
            for tenant_id in tenant_ids:
                logger.info(f"🔍 {tenant_id} tenant 데이터 수집 중...")
                api_data = self.api_pipeline.get_data(date=start_date, tenant_id=tenant_id)
                if api_data:
                    all_api_data.extend(api_data)
                    logger.info(f"   ✅ {tenant_id}: {len(api_data)}개 레코드 수집")
                else:
                    logger.info(f"   ⚠️ {tenant_id}: 데이터 없음")
            
            logger.info(f"📊 총 수집된 API 데이터: {len(all_api_data)}개")
            if not all_api_data:
                logger.warning("❌ 수집된 데이터가 없습니다.")
                return None
            
            input_data = self.api_pipeline.process_data(all_api_data)
            logger.info(f"처리된 데이터 shape: {input_data.shape}")
            
            if input_data.empty:
                logger.warning("❌ 처리된 데이터가 비어있습니다.")
                return None
            
            return input_data
        
        elif self.args.process == 'code-test':
            # 기존 파일에서 데이터 로드
            logger.info("📁 기존 파일에서 데이터를 로드합니다.")
            if self.args.file_name.split('.')[-1] == 'csv':
                input_data = pd.read_csv(f"{self.args.data_path}/{self.args.file_name}")
            elif self.args.file_name.split('.')[-1] == 'xlsx':
                input_data = pd.read_excel(f"{self.args.data_path}/{self.args.file_name}")
            else:
                logger.error("❌ 지원하지 않는 파일 형식입니다.")
                return None
            
            # 필요한 컬럼만 선택
            required_columns = ['date', 'q/a', 'content', 'user_id']
            if all(col in input_data.columns for col in required_columns):
                input_data = input_data[required_columns]
            else:
                logger.error(f"❌ 필요한 컬럼이 없습니다: {required_columns}")
                return None
            
            return input_data
        
        else:
            logger.error(f"❌ 지원하지 않는 프로세스 타입입니다: {self.args.process}")
            return None
    
    def process_and_store_data(self, input_data):
        """데이터 처리 및 저장 단계"""
        logger = logging.getLogger(__name__)
        if input_data is None or input_data.empty:
            logger.warning("❌ 저장할 데이터가 없습니다.")
            return False
        
        logger.info("💾 데이터 처리 및 저장을 시작합니다.")
        
        # API 데이터인 경우 추가 컬럼 처리
        if self.args.process in ['daily', 'scheduled']:
            required_columns = ['date', 'q/a', 'content', 'user_id', 'tenant_id', 'hash_value', 'hash_ref']
            missing_columns = [col for col in required_columns if col not in input_data.columns]
            if missing_columns:
                logger.error(f"❌ 필요한 컬럼이 없습니다: {missing_columns}")
                return False
            
            input_data = input_data[required_columns]
            
            # 날짜별 카운터 초기화
            date_counters = {}
            for date_str in input_data['date'].unique():
                date_value = datetime.fromisoformat(date_str)
                kst = timezone(timedelta(hours=9))
                if date_value.tzinfo is None:
                    date_value = date_value.replace(tzinfo=timezone.utc)
                kst_date = date_value.astimezone(kst)
                pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
                
                try:
                    self.pipe.postgres.db_connection.cur.execute(
                        f"SELECT MAX(conv_id) FROM {self.env_manager.conv_tb_name} WHERE conv_id LIKE %s",
                        (f"{pk_date}_%",)
                    )
                    max_conv_id = self.pipe.postgres.db_connection.cur.fetchone()[0]
                    date_counters[pk_date] = int(max_conv_id.split('_')[1]) if max_conv_id else 0
                except:
                    date_counters[pk_date] = 0
            
            # conv_id 생성 및 KST 변환
            conv_ids = []
            for idx in tqdm(range(len(input_data))):
                date_value = datetime.fromisoformat(input_data['date'][idx])
                kst = timezone(timedelta(hours=9))
                if date_value.tzinfo is None:
                    date_value = date_value.replace(tzinfo=timezone.utc)
                kst_date = date_value.astimezone(kst)
                
                input_data.at[idx, 'date'] = kst_date.isoformat()
                pk_date = f"{str(kst_date.year)}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
                date_counters[pk_date] += 1
                conv_ids.append(f"{pk_date}_{str(date_counters[pk_date]).zfill(5)}")
            
            input_data.insert(0, 'conv_id', conv_ids)
            input_data = input_data[['conv_id', 'date', 'q/a', 'content', 'user_id', 'tenant_id', 'hash_value', 'hash_ref']]
            
        else:
            # 기존 파일 데이터 처리
            conv_ids = []
            for idx in tqdm(range(len(input_data))):
                date_value = input_data['date'][idx]
                pk_date = f"{str(date_value.year)}{str(date_value.month).zfill(2)}{str(date_value.day).zfill(2)}"
                conv_id = pk_date + '_' + str(idx).zfill(5)
                conv_ids.append(conv_id)
            input_data.insert(0, 'conv_id', conv_ids)
        
        # 통계 출력
        if 'q/a' in input_data.columns:
            q_count = sum(1 for qa in input_data['q/a'] if qa == 'Q')
            a_count = sum(1 for qa in input_data['q/a'] if qa == 'A')
            logger.info(f"📊 Q&A 통계: Q {q_count}개, A {a_count}개")
            
            if 'hash_ref' in input_data.columns:
                a_with_ref = sum(1 for ref in input_data['hash_ref'] if ref is not None)
                logger.info(f"📊 A에 hash_ref 있음: {a_with_ref}개")
        
        # 데이터베이스에 저장
        total_records = len(input_data)
        existing_records = 0
        new_records = 0
        
        for idx in tqdm(range(len(input_data))):
            # 중복 체크 (API 데이터인 경우 해시값으로, 파일 데이터인 경우 PK로)
            if self.args.process in ['daily', 'scheduled'] and 'hash_value' in input_data.columns:
                if self.pipe.postgres.check_hash_duplicate(self.env_manager.conv_tb_name, input_data['hash_value'][idx]):
                    existing_records += 1
                    logger.info(f"이미 존재하는 데이터 (해시: {input_data['hash_value'][idx][:8]}...): {input_data['conv_id'][idx]}")
                    continue
            else:
                if self.pipe.postgres.check_pk(self.env_manager.conv_tb_name, input_data['conv_id'][idx]):
                    existing_records += 1
                    logger.info(f"이미 존재하는 데이터: {input_data['conv_id'][idx]}")
                    continue
            
            new_records += 1
            data_set = tuple(input_data.iloc[idx].values)
            self.pipe.table_editor.edit_conv_table('insert', self.env_manager.conv_tb_name, data_type='raw', data=data_set)
        
        # 저장 결과 요약
        summary_msg = f"📊 데이터 저장 완료 - 전체: {total_records}, 신규: {new_records}, 중복: {existing_records}"
        logger.info(summary_msg)
        logger.info(f"   중복률: {(existing_records/total_records*100):.1f}%" if total_records > 0 else "   중복률: 0%")
        
        return True
    
    def run_analysis(self):
        """분석 단계 실행"""
        logger = logging.getLogger(__name__)
        logger.info("🔍 데이터 분석 작업을 시작합니다.")
        
        try:
            # 분석 프로세스 실행
            self.pipe.run(process=self.args.process, query=self.args.query)
            logger.info("✅ 데이터 분석 작업이 완료되었습니다.")
            return True
        except Exception as e:
            logger.error(f"❌ 데이터 분석 중 오류 발생: {str(e)}")
            return False
    
    def run_full_pipeline(self):
        """전체 파이프라인 실행"""
        logger = logging.getLogger(__name__)
        logger.info("=== 통합 파이프라인 시작 ===")
        
        try:
            # 1단계: 데이터 수집
            input_data = self.collect_data()
            if input_data is None:
                logger.warning("⚠️ 데이터 수집 실패로 파이프라인을 종료합니다.")
                return False
            
            # 2단계: 데이터 처리 및 저장
            if not self.process_and_store_data(input_data):
                logger.warning("⚠️ 데이터 저장 실패로 파이프라인을 종료합니다.")
                return False
            
            # 3단계: 데이터 분석 (main.py의 기능)
            if not self.run_analysis():
                logger.warning("⚠️ 데이터 분석 실패")
                return False
            
            logger.info("=== 통합 파이프라인 완료 ===")
            return True
            
        except Exception as e:
            logger.error(f"❌ 통합 파이프라인 실행 중 오류 발생: {str(e)}")
            return False
        finally:
            # 데이터베이스 연결 종료
            if hasattr(self.pipe, 'postgres') and hasattr(self.pipe.postgres, 'db_connection'):
                self.pipe.postgres.db_connection.close()
