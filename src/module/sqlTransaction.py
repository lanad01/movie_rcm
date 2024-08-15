############################################################################################################
## 1. 패키지 import
############################################################################################################
import pandas as pd
import sqlite3
import os
import time
from dateutil.relativedelta import relativedelta

# 사용 예시
from module.logger import log_message

############################################################################################################
## 2. 초기설정
############################################################################################################
# 경로설정
dir_work = f'c:/Users/user/OneDrive - 파인트리파트너스(주)/movie'
dir_func = f'{dir_work}/src/module'
dir_data = f'{dir_work}/data'
dir_srcmov = f'{dir_work}/data/srcmov'


# DB연결
conn = sqlite3.connect(f"{dir_data}/pine_movie.db", isolation_level=None)

# 데이터 중복 제거 함수
def df_to_table(df, pk, tblName, chunksize):
    df.drop_duplicates(pk).to_sql(
      name      = tblName
    , con       = conn
    , if_exists = 'append'
    , index     = False
    , method    = "multi"
    , chunksize = chunksize
    )  
    
# DB 및 DF 적재 건수 확인 함수
def check_count(df, pk, tblName) : 
  DB건수 = pd.read_sql(
    f"""
        select count(1) as 건수
        from {tblName}
    """
  , conn
  ).values[0][0]
  DF건수 = len(df.drop_duplicates(pk))
  print(f"[LOG] 대상테이블 = {tblName} | DB 및 DF 적재 건수 탐색 시작")
  print(f"[LOG] 대상테이블 = {tblName} | DB적재건수   = {DB건수}")
  print(f"[LOG] 대상테이블 = {tblName} | DF적재건수   = {DF건수}")
  print(f"[LOG]  -------------------------------------------------------------------")
  return {'DB건수' : DB건수, 'DF건수' : DF건수}

# 전체 테이블 데이터 추출
def get_all_data(tblName) : 
  res = pd.read_sql(
    f"""
        select *
        from {tblName}
    """
  , conn
  )
  print(f"[LOG] 대상테이블 = {tblName} | 전체 테이블 데이터 추출 시작")
  print(f"[LOG] 대상테이블 = {tblName} | 추출 성공 | 데이터 크기 : {len(res)}")
  print(f"[LOG] ----------------------------------------------------------")
  return res

#
def get_all_data(tblName) : 
  print(f"[LOG] 대상테이블 = {tblName} | 전체 테이블 데이터 추출 시작")
