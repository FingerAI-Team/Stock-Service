from abc import ABC, abstractmethod 
from transformers import TrainingArguments, Trainer, EarlyStoppingCallback
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import DataCollatorWithPadding
import numpy as np 
import evaluate
import torch
import os

class BaseTokenizer(ABC):
    def __init__(self, tokenizer_path):
        self.tokenizer = None
        self.load_tokenizer(tokenizer_path)
    
    @abstractmethod
    def load_tokenizer(self, tokenizer_path):
        pass 

    @abstractmethod
    def tokenize_data(self, dataset):
        pass 


class BaseModel(ABC):
    def __init__(self, model_path):
        self.model = None 
        self.load_model(model_path)

    @abstractmethod
    def load_model(self, model_path):
        pass

    @abstractmethod 
    def set_training_config(self):
        pass

class EmbModel():    
    def set_gpu(self, model):
        self.device = torch.device('cuda') if torch.cuda.is_available() else "cpu"
        model.to(self.device)

    def set_embbeding_config(self, batch_size=12, max_length=1024):
        self.emb_config = {
            "batch_size": batch_size,
            "max_length": max_length
        }
    
    def bge_embed_data(self, text):
        '''
        텍스트를 임베딩(bge-m3)한 후 반환합니다.
        returns:
        list[float32]: bge-m3 모델로 인코딩된 결과값 (dense_vecs, lexical_weights, colbert_vecs)
        '''
        from FlagEmbedding import BGEM3FlagModel
        model = BGEM3FlagModel('BAAI/bge-m3',  use_fp16=True)
        if isinstance(text, str):
            return list(map(np.float32, model.encode(text, batch_size=self.emb_config['batch_size'], \
                    max_length=self.emb_config['max_length'])['dense_vecs']))
        else:       
            return list(map(np.float32, model.encode(list(text), batch_size=self.emb_config['batch_size'],\
                    max_length=self.emb_config['max_length'])['dense_vecs']))  
        
    def calc_emb_similarity(self, emb1, emb2, metric='L2'):
        '''
        임베딩 값의 유사도를 계산합니다. 현재는 (Euclidean distance) 활용
        args:
        emb1 (list[float]): 텍스트 a 임베딩 값 
        emb2 (list[float]): 텍스트 b 임베딩 값

        returns:
        float: 두 임베딩 값의 유사도  
        '''
        if metric == 'L2':   # Euclidean distance
            return np.linalg.norm(emb1 - emb2)
            
    @abstractmethod
    def get_hf_encoder(self):
        pass

    @abstractmethod 
    def get_cohere_encoder(self, cohere_api):
        pass
        

class KFDeBERTaTokenizer(BaseTokenizer):
    def load_tokenizer(self, tokenizer_path):
        self.id2label = {0: "stock", 1: "nstock"}
        self.label2id = {"stock": 0, "nstock": 1}
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
        # print(f'1: {self.tokenizer}')

    def tokenize_data(self, dataset):
        '''
        데이터세트를 토큰화하여 모델의 입력 형식에 맞게 변환 후 반환합니다.
        args: 
        dataset(dict): {'text': "", 'label': ""}

        returns:
        tokenized data
        '''
        if isinstance(dataset, str):
            return self.tokenizer.tokenize(dataset)
        else:
            tokenized_inputs = self.tokenizer(dataset['text'], padding=True, truncation=True, return_tensors='pt')
            if 'label' in dataset: 
                dataset['label'] = list(map(lambda x: self.label2id[x], dataset['label']))
                tokenized_inputs['label'] = torch.tensor(dataset['label']) 
            return tokenized_inputs
    
    def save_tokenizer(self, tokenizer_path):
        self.tokenizer.save_pretrained(tokenizer_path)
        

class KFDeBERTa(BaseModel):
    def load_model(self, model_path):
        self.id2label = {0: "stock", 1: "nstock"}
        self.label2id = {"stock": 0, "nstock": 1}
        self.model = AutoModelForSequenceClassification.from_pretrained(model_path, num_labels=2,\
                                                    id2label=self.id2label, label2id=self.label2id)
    
    def set_training_config(self, model_config):
        self.training_args = TrainingArguments(
            output_dir=os.path.join(model_config['model_path'], 'kfdeberta'),   # "my_awesome_model",
            learning_rate=2e-5,
            per_device_train_batch_size=16,
            per_device_eval_batch_size=16,
            num_train_epochs=10,
            weight_decay=0.01,
            eval_strategy="no",
            eval_steps=1000,
            save_steps=1000,
            save_strategy="no",
            load_best_model_at_end=True,
            push_to_hub=False,
            metric_for_best_model='eval_loss',
        )
    
    def save_model(self, model_path):
        self.model.save_pretrained(model_path)

class ModelTrainer:
    def __init__(self, tokenizer=None, model=None, training_args=None):
        self.tokenizer = tokenizer 
        self.model = model
        self.training_args = training_args
        self.trainer = None 
        self.id2label = {0: "stock", 1: "nstock"}
        self.label2id = {"stock": 0, "nstock": 1}
        self.accuracy = evaluate.load("accuracy")

    def setup_trainer(self, dataset):
        self.data_collator = DataCollatorWithPadding(tokenizer=self.tokenizer, padding=True)
        self.trainer = Trainer(
            model = self.model, 
            args = self.training_args, 
            train_dataset = dataset['train'],
            eval_dataset = dataset['val'],
            data_collator = self.data_collator,
            compute_metrics = self.compute_metrics, 
        )
    
    def compute_metrics(self, eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        return self.accuracy.compute(predictions=predictions, references=labels)

    def train(self):
        self.trainer.train()
        # self.trainer.save_model(self.training_args.output_dir)

    def save_model(self, model_path):
        self.model.save_pretrained(model_path)

    
class ModelPredictor:
    def __init__(self, tokenizer, model):
        self.tokenizer = tokenizer
        self.model = model 
        self.id2label = {0: "stock", 1: "nstock"}
        self.label2id = {"stock": 0, "nstock": 1}
        self.accuracy = evaluate.load("accuracy")
    
    def predict(self, text):
        '''
        text가 증권 종목 분석 질문인지 아닌지 예측합니다.
        args:
        text (str)

        returns:
        str: 증권 종목인 경우 stock, 증권 종목이 아닌 경우 nstock 반환
        '''
        inputs = self.tokenizer(text, truncation=True, return_tensors='pt')
        # print(f'len of toks: {len(self.tokenize_txt(text))}')   # 앞과 뒤에 SPECIAL TOKEN 추가됨 (+2)
        model_output = self.model(**inputs)
        response = torch.argmax(model_output.logits, dim=1).item()   # 0, 1 
        return self.id2label[response]

    def predict_proba(self, text): 
        '''
        text가 증권 종목 분석 질문인지 아닌지에 대한 확률 값을 반환합니다. 
        args: text (str)

        returns: 
        list: 합한 값이 1이 되는 레이블별 확률 값 리스트
        '''
        import torch.nn.functional as F
        inputs = self.tokenizer(text, truncation=True, return_tensors='pt')
        model_output = self.model(**inputs)
        # print(f'shape of model output: {np.shape(model_output.last_hidden_state)}')
        return F.softmax(model_output.logits, dim=-1)[0].tolist()

    def compute_metrics(self, eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=1)
        return self.accuracy.compute(predictions=predictions, references=labels)