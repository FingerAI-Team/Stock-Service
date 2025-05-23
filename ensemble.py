import numpy as np 

class WeightedEnsemble():
    def __init__(self, gpt_model, kfdeberta_model, lightgbm_model, weights):
        """
        모델의 예측 결과 값을 앙상블할 때 사용하는 모델들을 초기화합니다. 
        args:
        models: openai model, encoder model, machine learning model (이후 변경 가능성 다분)
        weights(list[float]): 총합은 1이어야 함
        """
        self.gpt_model = gpt_model
        self.kfdeberta_model = kfdeberta_model
        self.lightgbm_model = lightgbm_model
        self.weights = weights
    
    def predict(self, X_text, X_features):
        '''
        주어진 데이터가 증권 종목 분석 질문인지 각 모델이 예측한 후, 결과값을 앙상블합니다. 현재는 사용하지 않고 있습니다. 
        args:
        X_text (str): 텍스트 값
        X_features (list[float]): 텍스트를 tf-idf 라이브러리를 사용해서 변환한 피처 값 
        
        returns:
        str: 증권 종목 질문이면 'o', 비증권 종목 질문이면 'x'
        '''
        gpt_response = self.gpt_model.get_response(query=X_text, role=self.gpt_model.system_role, sub_role=self.gpt_model.stock_role)
        gpt_proba = np.array([0, 1]) if gpt_response == '종목' else np.array([1, 0])
        kfdeberta_proba = np.array(self.kfdeberta_model.predict_proba(X_text)).flatten()
        lightgbm_proba = np.array(self.lightgbm_model.predict_proba(X_features)).flatten()

        print(f'gpt_proba: {gpt_proba}')
        print(f'kfdeberta_proba: {kfdeberta_proba}')
        print(f'lightgbm_proba: {lightgbm_proba}')        
        weighted_preds = (
            self.weights[0] * gpt_proba + 
            self.weights[1] * kfdeberta_proba + 
            self.weights[2] * lightgbm_proba
        )
        print(weighted_preds)   # 0: 종목 x, 1: 종목
        final_preds = np.argmax(weighted_preds)
        return 'o' if final_preds == 1 else 'x'   