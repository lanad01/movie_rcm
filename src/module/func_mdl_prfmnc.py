############################################################################################################
## 1. 패키지 import
############################################################################################################
import pandas as pd
import numpy as np

# DB연결
def func_mdl_prfmnc(
      df_in
    , sctn   = 1
    , cutoff = None
) :
    df = df_in.copy()
    
    # 컬럼명 조정
    df.columns = ['Y_Real' , 'Y_Prob']
    
    # 컬럼 타입 조정
    df['Y_Real'] = df['Y_Real'].astype('int32')
    
    # 성능 측정 결과 리스트 선언
    list_prfmnc = []
    
    # 하나의 cutoff에 대한 성능 측정
    if sctn == 1 :
        st_pnt = 1
    else : 
        st_pnt = sctn
    
    for i in range(st_pnt, 100, sctn) :
        tmp_Y = df.copy()
        
        tmp_Y['cutoff'] = i
        
        # 예측여부 판단
        tmp_Y['Y_Pred'] = np.where(tmp_Y['Y_Prob'] >= i * 0.01, 1, 0)
        
        # 성능 판단
        tmp_Y['RP11'] = np.where((tmp_Y['Y_Real'] == 1) & (tmp_Y['Y_Pred'] == 1), 1, 0)
        tmp_Y['RP10'] = np.where((tmp_Y['Y_Real'] == 1) & (tmp_Y['Y_Pred'] == 0), 1, 0)
        tmp_Y['RP01'] = np.where((tmp_Y['Y_Real'] == 0) & (tmp_Y['Y_Pred'] == 1), 1, 0)
        tmp_Y['RP00'] = np.where((tmp_Y['Y_Real'] == 0) & (tmp_Y['Y_Pred'] == 0), 1, 0)
        
        # 집계
        tmp_Y_2 = (
            tmp_Y
            .groupby('cutoff', as_index = False)
            .agg(
                  R1   = ('Y_Real', 'sum')
                , P1   = ('Y_Pred', 'sum')  
                , RP11 = ('RP00', 'sum')  
                , RP10 = ('RP01', 'sum')  
                , RP01 = ('RP10', 'sum')  
                , RP00 = ('RP11', 'sum')  
            )
        )
        
        # 성능 산출
        tmp_Y_2['precision'] = tmp_Y_2['RP11'] / (tmp_Y_2['RP11'] + tmp_Y_2['RP01'])
        tmp_Y_2['recall'   ] = tmp_Y_2['RP11'] / (tmp_Y_2['RP11'] + tmp_Y_2['RP10'])
        tmp_Y_2['f1score'  ] = 2* tmp_Y_2['precision'] * tmp_Y_2['recall'] / (tmp_Y_2['precision'] + tmp_Y_2['recall'])
        
        list_prfmnc.append(tmp_Y_2)
        
    df_prfmnc = pd.concat(list_prfmnc)
    
    return df_prfmnc