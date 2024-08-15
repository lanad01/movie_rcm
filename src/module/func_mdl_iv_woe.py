import pandas as pd
import numpy as np
import math

def func_mdl_iv_woe(
      df_in
    , list_pk = ['기준년월', '회원번호', 'y']
) :
    df = df_in.fillna(0)

    # 변수 리스트 생성
    df_var_list = pd.DataFrame(df_in.dtypes, columns = ["변수유형"]).reset_index().rename(columns = {"index" : "변수명"})

    # 연속형 변수 리스트 생성
    list_num = (
        df_var_list[(df_var_list['변수유형'] != 'object') & ~(df_var_list['변수명'].isin(list_pk))]['변수명'].tolist()
    )

    ##################################################################################################################
    # 연속형 변수 구간화
    ##################################################################################################################
    for i, col in enumerate(list_num) :
        if i % 10 == 0 : print(f'[LOG] func_mdl_pre_iv > 연속형 변수 Binning : {i} / {len(list_num)}')
        
        # quantile 정의
        list_q = [0, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1]
        
        # quantile별 값 산출
        list_qcut = (
            pd.qcut(df[col], q = list_q, duplicates='drop', precision=0)
            .value_counts()
            .sort_index()
            .reset_index().iloc[:, 0]
            .apply(lambda x : str(x).replace('(', '').replace(']', '').replace(',', '_'))
            .tolist()
        )
        
        df[col] = pd.qcut(
             df[col]
            , q          = list_q
            , precision  = 0
            , duplicates ='drop'
            , labels     = [str(f'{x+1:0>2}') + '.' + str(list_qcut[x]) for x in range(len(list_qcut))]        
        ).astype(object).fillna('99.Miss')
        
    ##################################################################################################################
    # IV WOE 산출
    ##################################################################################################################
    # 이벤트/논이벤트 건수 집계를 위한 타겟변수 타입 변경
    df['y1'] = df['y'].astype(int)

    # 이벤트/논이벤트 건수 집계를 위한 y변수 분리
    df['y0'] = np.where(df['y1'] == 0, 1, 0)

    # 산출할 대상 변수 리스트화
    list_ftr = set(df.columns) - set(list_pk + ['y0', 'y1'])

    # 변수 IV 산출 시작
    list_iv_tot = []

    for i, varNm in enumerate(list_ftr) :
        if i % 10 == 0 : print(f'[LOG] func_mdl_pre_iv > IV 산출 : {i} / {len(list_ftr)}')
        
        df_iv_tmp                = df.groupby(varNm)[['y0','y1']].sum().reset_index().set_axis(['varDesc', 'y0', 'y1'], axis = 'columns')
        df_iv_tmp['varNm' ]      = varNm
        df_iv_tmp['cnt' ]        = df.groupby(varNm)[['회원번호']].count().reset_index()['회원번호']
        df_iv_tmp['y1_tot_cnt' ] = df_iv_tmp.y1.sum()
        df_iv_tmp['y0_tot_cnt' ] = df_iv_tmp.y0.sum()
        df_iv_tmp['y1_dist' ]    = (df_iv_tmp.y1 + 0.5) / df_iv_tmp.y1.sum()
        df_iv_tmp['y0_dist' ]    = (df_iv_tmp.y0 + 0.5) / df_iv_tmp.y0.sum()
        df_iv_tmp['y1_rt' ]      = df_iv_tmp['y1'] / df_iv_tmp['cnt']
        df_iv_tmp['y0_rt' ]      = df_iv_tmp['y0'] / df_iv_tmp['cnt']
        df_iv_tmp['woe' ]        = 0
        df_iv_tmp['iv' ]         = 0
        
        for i_stn in range(len(df_iv_tmp)) :
            # 구간별 Y1 및 Y0 구성비
            v_y1_dstrb = df_iv_tmp.loc[i_stn, 'y1_dist']
            v_y0_dstrb = df_iv_tmp.loc[i_stn, 'y0_dist']
            
            # IV 산출 
            df_iv_tmp.loc[i_stn, 'woe'] = math.log(v_y0_dstrb / v_y1_dstrb)
            df_iv_tmp.loc[i_stn, 'iv']  = (v_y0_dstrb - v_y1_dstrb) * (math.log(v_y0_dstrb / v_y1_dstrb))
            
        df_iv_tmp['iv_sum'] = df_iv_tmp.groupby('varNm')[['iv']].sum()['iv'].values[0]

        list_iv_tot.append(df_iv_tmp)

    df_iv_step2 = pd.concat(list_iv_tot)

    df_iv_step3 = (
        df_iv_step2
        .sort_values(['iv_sum', 'varNm'], ascending = [False, True])
    )

    df_res = df_iv_step3[['varNm', 'varDesc', 'cnt', 'y1', 'y1_tot_cnt', 'y1_dist', 'y1_rt', 'y0', 'y0_tot_cnt', 'y0_dist', 'y0_rt', 'woe', 'iv', 'iv_sum']]

    return df_res