############################################################################################################
## 1. 패키지 import
############################################################################################################
import pandas as pd
import sqlite3
import os, sys
from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta

# 사용 예시
from module.logger import log_message
import module.sqlTransaction as sqlT
import importlib
importlib.reload(sqlT)

############################################################################################################
## 2. 초기설정
############################################################################################################
# 경로설정
dir_work   = f'c:/Users/user/OneDrive - 파인트리파트너스(주)/movie'
dir_func   = f'{dir_work}/src/module'
dir_data   = f'{dir_work}/data'

# DB연결
conn = sqlite3.connect(f"{dir_data}/pine_movie.db", isolation_level=None)
cur = conn.cursor()

# 함수 호출
# exec(open(f"{dir_func}/sqlTransaction.py"      , encoding= 'utf-8').read() )
# exec(open(f"{dir_func}/logger.py"              , encoding= 'utf-8').read() )

############################################################################################################
## 3. 파라미터 추출
############################################################################################################
# 입력 파라미터 추출
try :
    bas_ym = sys.argv[1]
    if not bas_ym.isdigit() : raise
except:
    bas_ym = '201208'
    
list_bas_ym = [datetime.strftime(datetime.strptime(bas_ym, '%Y%m') - relativedelta(months = 5 ), '%Y%m') for i in range(5)]
bas_ym_bfr_2m = datetime.strftime(datetime.strptime(bas_ym, '%Y%m') - relativedelta(months = 2 ), '%Y%m')

st_tm = str(datetime.now()).split(' ')[1].split('.')[0]
print(f'[LOG] 기준년월 = {bas_ym} 피쳐마트 적재 시작 {st_tm}')

############################################################################################################
## 4. 피쳐마트 적재
############################################################################################################
cur.execute(f"""delete from INPUT_MART where 기준년월 = {bas_ym}""")
cur.execute(f"""
    INSERT INTO INPUT_MART
    SELECT
          {bas_ym}                                                             AS 기준년월
        , T1.회원번호

        , T2.대륙번호
        , T2.연령대
        , T2.통근거리
        , T2.신용잔액구간
        , T2.학력
        , T2.성별
        , T2.가구규모
        , T2.수입구간
        , T2.잔고부족발생여부
        , T2.직장유형
        , T2.부동산담보대출및임대지불지연발생여부
        , T2.혼인유형
        , T2.부동산담보대출금액존재여부
        , T2.보유차량수
        , T2.주거유형
        , T2.세그먼트번호
        , T2.총직장근속년수
        , T2.회원유지기간
        , T2.거주기간

        , T3.최근1개월_영화시청건수
        , T3.최근1개월_개봉년_1950년이전_시청건수
        , T3.최근1개월_개봉년_1950년대_시청건수
        , T3.최근1개월_개봉년_1960년대_시청건수
        , T3.최근1개월_개봉년_1970년대_시청건수
        , T3.최근1개월_개봉년_1980년대_시청건수
        , T3.최근1개월_개봉년_1990년대_시청건수
        , T3.최근1개월_개봉년_2000년대_시청건수
        , T3.최근1개월_개봉년_2010년이후_시청건수
        , T3.최근1개월_제작비_0원_시청건수
        , T3.최근1개월_제작비_5백만이하_시청건수
        , T3.최근1개월_제작비_5백_1천만_시청건수
        , T3.최근1개월_제작비_1_2천만_시청건수
        , T3.최근1개월_제작비_2_3천만_시청건수
        , T3.최근1개월_제작비_3_5천만_시청건수
        , T3.최근1개월_제작비_5천만_1억_시청건수
        , T3.최근1개월_제작비_1억이상_시청건수
        , T3.최근1개월_박스오피스_0원_시청건수
        , T3.최근1개월_박스오피스_1천만이하_시청건수
        , T3.최근1개월_박스오피스_1_2천만_시청건수
        , T3.최근1개월_박스오피스_2_3천만_시청건수
        , T3.최근1개월_박스오피스_3_5천만_시청건수
        , T3.최근1개월_박스오피스_5천만_1억_시청건수
        , T3.최근1개월_박스오피스_1_2억_시청건수
        , T3.최근1개월_박스오피스_2억이상_시청건수

        , T4.최근1개월_전체_평가건수
        , T4.최근1개월_전체_시청시작건수
        , T4.최근1개월_전체_시청완료건수
        , T4.최근1개월_전체_탐색건수
        , T4.최근1개월_전체_구매건수

        , T5.최근3개월_평균평점_역사
        , T5.최근3개월_평균평점_애니메이션
        , T5.최근3개월_평균평점_드라마
        , T5.최근3개월_평균평점_코미디
        , T5.최근3개월_평균평점_액션
        , T5.최근3개월_평균평점_스릴러
        , T5.최근3개월_평균평점_모험
        , T5.최근3개월_평균평점_판타지
        , T5.최근3개월_평균평점_범죄
        , T5.최근3개월_평균평점_가족
        , T5.최근3개월_평균평점_로맨스
        , T5.최근3개월_평균평점_음악
        , T5.최근3개월_평균평점_공포
        , T5.최근3개월_평균평점_전쟁
        , T5.최근3개월_평균평점_서부극
        , T5.최근3개월_평균평점_미스테리
        , T5.최근3개월_평균평점_단막극
        , T5.최근3개월_평균평점_뮤지컬
        , T5.최근3개월_평균평점_스포츠
        , T5.최근3개월_평균평점_공상과학
        , T5.최근3개월_평균평점_전기

        , T6.최근1개월_액션_탐색횟수
        , T6.최근1개월_액션_평가횟수
        , T6.최근1개월_액션_시청완료횟수
        , T6.최근1개월_액션_시청시작횟수
        , T6.최근1개월_액션_구매횟수
        , T6.최근1개월_드라마_탐색횟수
        , T6.최근1개월_드라마_평가횟수
        , T6.최근1개월_드라마_시청완료횟수
        , T6.최근1개월_드라마_시청시작횟수
        , T6.최근1개월_드라마_구매횟수
        , T6.최근1개월_코미디_탐색횟수
        , T6.최근1개월_코미디_평가횟수
        , T6.최근1개월_코미디_시청완료횟수
        , T6.최근1개월_코미디_시청시작횟수
        , T6.최근1개월_코미디_구매횟수
        , T6.최근1개월_스릴러_탐색횟수
        , T6.최근1개월_스릴러_평가횟수
        , T6.최근1개월_스릴러_시청완료횟수
        , T6.최근1개월_스릴러_시청시작횟수
        , T6.최근1개월_스릴러_구매횟수
        , T6.최근1개월_로맨스_탐색횟수
        , T6.최근1개월_로맨스_평가횟수
        , T6.최근1개월_로맨스_시청완료횟수
        , T6.최근1개월_로맨스_시청시작횟수
        , T6.최근1개월_로맨스_구매횟수
        , T6.최근1개월_공포_탐색횟수
        , T6.최근1개월_공포_평가횟수
        , T6.최근1개월_공포_시청완료횟수
        , T6.최근1개월_공포_시청시작횟수
        , T6.최근1개월_공포_구매횟수
        , T6.최근1개월_공상과학_탐색횟수
        , T6.최근1개월_공상과학_평가횟수
        , T6.최근1개월_공상과학_시청완료횟수
        , T6.최근1개월_공상과학_시청시작횟수
        , T6.최근1개월_공상과학_구매횟수
        , T6.최근1개월_범죄_탐색횟수
        , T6.최근1개월_범죄_평가횟수
        , T6.최근1개월_범죄_시청완료횟수
        , T6.최근1개월_범죄_시청시작횟수
        , T6.최근1개월_범죄_구매횟수
        , T6.최근1개월_판타지_탐색횟수
        , T6.최근1개월_판타지_평가횟수
        , T6.최근1개월_판타지_시청완료횟수
        , T6.최근1개월_판타지_시청시작횟수
        , T6.최근1개월_판타지_구매횟수
        , T6.최근1개월_가족_탐색횟수
        , T6.최근1개월_가족_평가횟수
        , T6.최근1개월_가족_시청완료횟수
        , T6.최근1개월_가족_시청시작횟수
        , T6.최근1개월_가족_구매횟수
        , T6.최근1개월_모험_탐색횟수
        , T6.최근1개월_모험_평가횟수
        , T6.최근1개월_모험_시청완료횟수
        , T6.최근1개월_모험_시청시작횟수
        , T6.최근1개월_모험_구매횟수
        , T6.최근1개월_전쟁_탐색횟수
        , T6.최근1개월_전쟁_평가횟수
        , T6.최근1개월_전쟁_시청완료횟수
        , T6.최근1개월_전쟁_시청시작횟수
        , T6.최근1개월_전쟁_구매횟수
        , T6.최근1개월_음악_탐색횟수
        , T6.최근1개월_음악_평가횟수
        , T6.최근1개월_음악_시청완료횟수
        , T6.최근1개월_음악_시청시작횟수
        , T6.최근1개월_음악_구매횟수
        , T6.최근1개월_뮤지컬_탐색횟수
        , T6.최근1개월_뮤지컬_평가횟수
        , T6.최근1개월_뮤지컬_시청완료횟수
        , T6.최근1개월_뮤지컬_시청시작횟수
        , T6.최근1개월_뮤지컬_구매횟수
        , T6.최근1개월_전기_탐색횟수
        , T6.최근1개월_전기_평가횟수
        , T6.최근1개월_전기_시청완료횟수
        , T6.최근1개월_전기_시청시작횟수
        , T6.최근1개월_전기_구매횟수
        , T6.최근1개월_스포츠_탐색횟수
        , T6.최근1개월_스포츠_평가횟수
        , T6.최근1개월_스포츠_시청완료횟수
        , T6.최근1개월_스포츠_시청시작횟수
        , T6.최근1개월_스포츠_구매횟수
        , T6.최근1개월_서부극_탐색횟수
        , T6.최근1개월_서부극_평가횟수
        , T6.최근1개월_서부극_시청완료횟수
        , T6.최근1개월_서부극_시청시작횟수
        , T6.최근1개월_서부극_구매횟수
        , T6.최근1개월_애니메이션_탐색횟수
        , T6.최근1개월_애니메이션_평가횟수
        , T6.최근1개월_애니메이션_시청완료횟수
        , T6.최근1개월_애니메이션_시청시작횟수
        , T6.최근1개월_애니메이션_구매횟수
        , T6.최근1개월_다큐멘터리_탐색횟수
        , T6.최근1개월_다큐멘터리_평가횟수
        , T6.최근1개월_다큐멘터리_시청완료횟수
        , T6.최근1개월_다큐멘터리_시청시작횟수
        , T6.최근1개월_다큐멘터리_구매횟수
        , T6.최근1개월_미스테리_탐색횟수
        , T6.최근1개월_미스테리_평가횟수
        , T6.최근1개월_미스테리_시청완료횟수
        , T6.최근1개월_미스테리_시청시작횟수
        , T6.최근1개월_미스테리_구매횟수
        , T6.최근1개월_단막극_탐색횟수
        , T6.최근1개월_단막극_평가횟수
        , T6.최근1개월_단막극_시청완료횟수
        , T6.최근1개월_단막극_시청시작횟수
        , T6.최근1개월_단막극_구매횟수
        , T6.최근1개월_역사_탐색횟수
        , T6.최근1개월_역사_평가횟수
        , T6.최근1개월_역사_시청완료횟수
        , T6.최근1개월_역사_시청시작횟수
        , T6.최근1개월_역사_구매횟수

    FROM (
        SELECT
              DISTINCT 회원번호
        FROM MOVIE_FACT
        WHERE 기준년월 BETWEEN {bas_ym_bfr_2m} AND {bas_ym}
    ) T1

    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- 고객기본정보 T2
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    LEFT JOIN (
        SELECT
              회원번호
            , 대륙번호
            , CASE WHEN 나이 <  20                                              THEN 1
                   WHEN 나이 <  30 AND 나이 >= 20                               THEN 2
                   WHEN 나이 <  40 AND 나이 >= 30                               THEN 3
                   WHEN 나이 <  50 AND 나이 >= 40                               THEN 4
                   WHEN 나이 <  60 AND 나이 >= 50                               THEN 5
                   WHEN 나이 <  70 AND 나이 >= 60                               THEN 6
                   WHEN 나이 <  80 AND 나이 >= 70                               THEN 7
                   WHEN 나이 >= 80                                              THEN 8
              END                                                                                                                                               AS 연령대
            , CASE WHEN 통근거리 <   5                                          THEN 1  -- 단거리
                   WHEN 통근거리 <  15 AND 통근거리 >= 5                        THEN 2  -- 중거리
                   WHEN 통근거리 >= 15                                          THEN 3  -- 장거리
              END                                                                                                                                               AS 통근거리
            , CASE WHEN 신용잔액 =    0                                         THEN 1
                   WHEN 신용잔액 <  100  AND 신용잔액 >    0                    THEN 2
                   WHEN 신용잔액 <  200  AND 신용잔액 >= 100                    THEN 3
                   WHEN 신용잔액 <  500  AND 신용잔액 >= 200                    THEN 4
                   WHEN 신용잔액 >= 500                                         THEN 5
              END                                                                                                                                               AS 신용잔액구간
            , CASE WHEN 학력 = 'UNKNOWN'                                        THEN 1  -- 알수없음
                   WHEN 학력 = 'LESSTHANHS'                                     THEN 2  -- 중졸
                   WHEN 학력 = 'HIGH SCHOOL'                                    THEN 3  -- 고졸
                   WHEN 학력 = 'ASSOCIATES'                                     THEN 4  -- 전문대졸
                   WHEN 학력 = 'BACHELORS'                                      THEN 5  -- 학사
                   WHEN 학력 = 'MASTERS'                                        THEN 6  -- 석사
                   WHEN 학력 = 'DOCTORATE'                                      THEN 7  -- 박사
              END                                                                                                                                               AS 학력
            , CASE WHEN 성별 = 'MALE'                                           THEN 0  -- 남자
                   WHEN 성별 = 'FEMALE'                                         THEN 1  -- 여자
              END                                                                                                                                               AS 성별
            , CASE WHEN 가구규모 = 1                                            THEN 1  -- 1인
                   WHEN 가구규모 = 2                                            THEN 2  -- 2인가족(부부)
                   WHEN 가구규모 = 3                                            THEN 3  -- 3인가족(자녀 1인)
                   WHEN 가구규모 = 4                                            THEN 4  -- 4인가족(자녀 2인)
                   WHEN 가구규모 = 5                                            THEN 5  -- 5인가족(자녀 3인)
                   WHEN 가구규모 > 5                                            THEN 6  -- 6인이상(대가족)
              END                                                                                                                                               AS 가구규모
            , CASE WHEN 수입구간 = 'A: BELOW 30,000'                            THEN 1
                   WHEN 수입구간 = 'B: 30,000 - 49,999'                         THEN 2
                   WHEN 수입구간 = 'C: 50,000 - 69,999'                         THEN 3
                   WHEN 수입구간 = 'D: 70,000 - 89,999'                         THEN 4
                   WHEN 수입구간 = 'E: 90,000 - 109,999'                        THEN 5
                   WHEN 수입구간 = 'F: 110,000 - 129,999'                       THEN 6
              END                                                                                                                                               AS 수입구간
            , CASE WHEN 잔고부족발생건수 > 0                                    THEN 1  -- 잔고부족발생건수 존재
                   WHEN 잔고부족발생건수 = 0                                    THEN 0  -- 잔고부족발생건수 없음
              END                                                                                                                                               AS 잔고부족발생여부
            , CASE WHEN 직장유형 = 'NONE'                                       THEN 1  -- 무직
                   WHEN 직장유형 = 'CLERICAL'                                   THEN 2  -- 사무직
                   WHEN 직장유형 = 'SERVICES'                                   THEN 3  -- 서비스직
                   WHEN 직장유형 = 'SCIENTIFIC'                                 THEN 4  -- 연구직
                   WHEN 직장유형 = 'SALES'                                      THEN 5  -- 영업
                   WHEN 직장유형 = 'LABOR'                                      THEN 6  -- 노동직
                   WHEN 직장유형 = 'SKILLED'                                    THEN 7  -- 기술직
                   WHEN 직장유형 = 'PROFESSIONAL'                               THEN 8  -- 전문직
                   WHEN 직장유형 = 'BUSINESS'                                   THEN 9  -- 비즈니스
              END                                                                                                                                               AS 직장유형
            , CASE WHEN 부동산담보대출및임대지불지연건수 > 0                    THEN 1  -- 부담대 및 임대 지불 지연 건수 존재
                   WHEN 부동산담보대출및임대지불지연건수 = 0                    THEN 0  -- 부담대 및 임대 지불 지연 건수 없음
              END                                                                                                                                               AS 부동산담보대출및임대지불지연발생여부
            , CASE WHEN 혼인유형 = 'M'                                          THEN 1  -- 기혼
                   WHEN 혼인유형 = 'S'                                          THEN 0  -- 미혼
              END                                                                                                                                               AS 혼인유형
            , CASE WHEN 부동산담보대출금액 > 0                                  THEN 1  -- 부담대 및 임대 지불 지연 건수 존재
                   WHEN 부동산담보대출금액 = 0                                  THEN 0  -- 부담대 및 임대 지불 지연 건수 없음
              END                                                                                                                                               AS 부동산담보대출금액존재여부
            , 보유차량수
            , CASE WHEN 주거유형 = 'OWN'                                        THEN 0  -- 자가
                   WHEN 주거유형 = 'RENT'                                       THEN 1  -- 임대
                   WHEN 주거유형 IS NULL                                        THEN 99
              END                                                                                                                                               AS 주거유형
            , 세그먼트번호
            , CASE WHEN 현직장근속년수 <   5  AND  현직장근속년수 >=  0         THEN 1
                   WHEN 현직장근속년수 <  10  AND  현직장근속년수 >=  5         THEN 2
                   WHEN 현직장근속년수 <  15  AND  현직장근속년수 >= 10         THEN 3
                   WHEN 현직장근속년수 <  20  AND  현직장근속년수 >= 15         THEN 4
                   WHEN 현직장근속년수 <  30  AND  현직장근속년수 >= 20         THEN 5
                   WHEN 현직장근속년수 <  40  AND  현직장근속년수 >= 30         THEN 6
                   WHEN 현직장근속년수 >= 40                                    THEN 7
              END                                                                                                                                               AS 총직장근속년수
            , CASE WHEN 회원유지기간 <   2                                      THEN 1
                   WHEN 회원유지기간 <   4    AND 회원유지기간 >= 2             THEN 2
                   WHEN 회원유지기간 <   7    AND 회원유지기간 >= 4             THEN 3
                   WHEN 회원유지기간 <  10    AND 회원유지기간 >= 7             THEN 4
                   WHEN 회원유지기간 >= 10                                      THEN 5
              END                                                                                                                                               AS 회원유지기간
            , CASE WHEN 거주기간 <   3                                          THEN 1
                   WHEN 거주기간 <   6        AND  거주기간 >= 3                THEN 2
                   WHEN 거주기간 <  10        AND  거주기간 >= 6                THEN 3
                   WHEN 거주기간 <  15        AND  거주기간 >= 10               THEN 4
                   WHEN 거주기간 <  20        AND  거주기간 >= 15               THEN 5
                   WHEN 거주기간 >= 20                                          THEN 6
              END                                                                                                                                               AS 거주기간
        FROM CUSTOMER
    ) T2
    ON T1.회원번호 = T2.회원번호

    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- 고객 시청 영화 정보 T3
    -- 고객의 기준년월로부터 최근 1개월 간의 시청영화 집계 정보 매핑
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    LEFT JOIN (
        SELECT
              a21.회원번호
            , a21.기준년월

            , COUNT(a22.영화번호)                                                                                                                               AS 최근1개월_영화시청건수

            , SUM(CASE WHEN a22.개봉년         = 1 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_1950년이전_시청건수
            , SUM(CASE WHEN a22.개봉년         = 2 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_1950년대_시청건수
            , SUM(CASE WHEN a22.개봉년         = 3 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_1960년대_시청건수
            , SUM(CASE WHEN a22.개봉년         = 4 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_1970년대_시청건수
            , SUM(CASE WHEN a22.개봉년         = 5 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_1980년대_시청건수
            , SUM(CASE WHEN a22.개봉년         = 6 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_1990년대_시청건수
            , SUM(CASE WHEN a22.개봉년         = 7 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_2000년대_시청건수
            , SUM(CASE WHEN a22.개봉년         = 8 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_개봉년_2010년이후_시청건수

            , SUM(CASE WHEN a22.제작비         = 1 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_0원_시청건수
            , SUM(CASE WHEN a22.제작비         = 2 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_5백만이하_시청건수
            , SUM(CASE WHEN a22.제작비         = 3 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_5백_1천만_시청건수
            , SUM(CASE WHEN a22.제작비         = 4 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_1_2천만_시청건수
            , SUM(CASE WHEN a22.제작비         = 5 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_2_3천만_시청건수
            , SUM(CASE WHEN a22.제작비         = 6 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_3_5천만_시청건수
            , SUM(CASE WHEN a22.제작비         = 7 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_5천만_1억_시청건수
            , SUM(CASE WHEN a22.제작비         = 8 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_제작비_1억이상_시청건수

            , SUM(CASE WHEN a22.박스오피스     = 1 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_0원_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 2 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_1천만이하_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 3 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_1_2천만_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 4 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_2_3천만_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 5 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_3_5천만_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 6 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_5천만_1억_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 7 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_1_2억_시청건수
            , SUM(CASE WHEN a22.박스오피스     = 8 THEN 1 ELSE 0 END)                                                                                           AS 최근1개월_박스오피스_2억이상_시청건수

        --------------------------------------------------------------------
        -- 당월 영화 시청 고객 추출
        --------------------------------------------------------------------
        FROM (
            SELECT
                  기준년월
                , 회원번호
                , 영화번호
            FROM MOVIE_FACT
            WHERE 기준년월 = {bas_ym}
        ) a21

        --------------------------------------------------------------------
        -- 영화마트 테이블 매핑
        --------------------------------------------------------------------
        LEFT JOIN (
            SELECT
                  영화번호
                , 영화명
                , CASE WHEN CAST(개봉년 AS INT) <  1950                                                             THEN 1
                       WHEN CAST(개봉년 AS INT) <  1960   AND     CAST(개봉년 AS INT) >= 1950                       THEN 2
                       WHEN CAST(개봉년 AS INT) <  1970   AND     CAST(개봉년 AS INT) >= 1960                       THEN 3
                       WHEN CAST(개봉년 AS INT) <  1980   AND     CAST(개봉년 AS INT) >= 1970                       THEN 4
                       WHEN CAST(개봉년 AS INT) <  1990   AND     CAST(개봉년 AS INT) >= 1980                       THEN 5
                       WHEN CAST(개봉년 AS INT) <  2000   AND     CAST(개봉년 AS INT) >= 1990                       THEN 6
                       WHEN CAST(개봉년 AS INT) <  2010   AND     CAST(개봉년 AS INT) >= 2000                       THEN 7
                       WHEN CAST(개봉년 AS INT) >= 2010                                                             THEN 8
                  END                                                                                                                                           AS 개봉년
                , CASE WHEN 제작비  ==          0                                                                   THEN 1
                        WHEN 제작비  <    5000000         AND     제작비 >         0                                THEN 2
                        WHEN 제작비  <   10000000         AND     제작비 >=  5000000                                THEN 3
                        WHEN 제작비  <   20000000         AND     제작비 >= 10000000                                THEN 4
                        WHEN 제작비  <   30000000         AND     제작비 >= 20000000                                THEN 5
                        WHEN 제작비  <   50000000         AND     제작비 >= 30000000                                THEN 6
                        WHEN 제작비  <  100000000         AND     제작비 >= 50000000                                THEN 7
                        WHEN 제작비 >=  100000000                                                                   THEN 8
                  END                                                                                                                                           AS 제작비
                , CASE WHEN 박스오피스  =          0                                                                THEN 1
                        WHEN 박스오피스 <   10000000      AND     박스오피스 >          0                           THEN 2
                        WHEN 박스오피스 <   20000000      AND     박스오피스 >=  10000000                           THEN 3
                        WHEN 박스오피스 <   30000000      AND     박스오피스 >=  20000000                           THEN 4
                        WHEN 박스오피스 <   50000000      AND     박스오피스 >=  30000000                           THEN 5
                        WHEN 박스오피스 <  100000000      AND     박스오피스 >=  50000000                           THEN 6
                        WHEN 박스오피스 <  200000000      AND     박스오피스 >= 100000000                           THEN 7
                        WHEN 박스오피스 >= 200000000                                                                THEN 8
                  END                                                                                                                                           AS 박스오피스
            FROM MOVIE
        ) a22
        ON a21.영화번호 = a22.영화번호

        GROUP BY
              a21.회원번호
            , a21.기준년월
    ) T3
    ON T1.회원번호 = T3.회원번호

    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- 최근 1개월 고객별 행동별 건수 T4
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    LEFT JOIN (
        SELECT
              회원번호
            , SUM(CASE WHEN 행동번호 =  1 THEN 1 ELSE 0 END )                           AS 최근1개월_전체_평가건수
            , SUM(CASE WHEN 행동번호 =  2 THEN 1 ELSE 0 END )                           AS 최근1개월_전체_시청시작건수
            , SUM(CASE WHEN 행동번호 =  4 THEN 1 ELSE 0 END )                           AS 최근1개월_전체_시청완료건수
            , SUM(CASE WHEN 행동번호 =  5 THEN 1 ELSE 0 END )                           AS 최근1개월_전체_탐색건수
            , SUM(CASE WHEN 행동번호 = 11 THEN 1 ELSE 0 END )                           AS 최근1개월_전체_구매건수
        FROM MOVIE_FACT
        WHERE 기준년월 = {bas_ym}
        GROUP BY 회원번호
    ) T4
    ON T1.회원번호 = T4.회원번호

    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- 최근 3개월 간 장르별 메긴 평점의 평균 T5
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    LEFT JOIN (
        SELECT
              회원번호

            , SUM(CASE WHEN 장르번호 =  1                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_역사
            , SUM(CASE WHEN 장르번호 =  2                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_애니메이션
            , SUM(CASE WHEN 장르번호 =  3                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_드라마
            , SUM(CASE WHEN 장르번호 =  6                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_코미디
            , SUM(CASE WHEN 장르번호 =  7                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_액션
            , SUM(CASE WHEN 장르번호 =  8                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_범죄
            , SUM(CASE WHEN 장르번호 =  9                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_스릴러
            , SUM(CASE WHEN 장르번호 = 11                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_모험
            , SUM(CASE WHEN 장르번호 = 12                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_판타지
            , SUM(CASE WHEN 장르번호 = 14                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_가족
            , SUM(CASE WHEN 장르번호 = 15                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_로맨스
            , SUM(CASE WHEN 장르번호 = 16                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_음악
            , SUM(CASE WHEN 장르번호 = 17                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_공포
            , SUM(CASE WHEN 장르번호 = 18                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_전쟁
            , SUM(CASE WHEN 장르번호 = 19                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_서부극
            , SUM(CASE WHEN 장르번호 = 20                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_미스테리
            , SUM(CASE WHEN 장르번호 = 24                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_단막극
            , SUM(CASE WHEN 장르번호 = 25                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_뮤지컬
            , SUM(CASE WHEN 장르번호 = 30                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_스포츠
            , SUM(CASE WHEN 장르번호 = 45                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_공상과학
            , SUM(CASE WHEN 장르번호 = 10                  THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_다큐멘터리
            , SUM(CASE WHEN 장르번호 IN (46, 53)           THEN 평균평점        ELSE 0 END)                 AS 최근3개월_평균평점_전기

        ----------------------------------------------------------------------------------
        -- 고객별 장르별 평균평점 산출
        ----------------------------------------------------------------------------------
        FROM (
            SELECT
                  b41.회원번호
                , b42.장르번호
                , AVG(b41.평점)                                              AS 평균평점
            FROM MOVIE_FACT b41

            ----------------------------------------------------------------
            -- 행동정보 테이블에서 고객별 장르별 평점 이력 추출
            ----------------------------------------------------------------
            LEFT JOIN (
                SELECT
                      영화번호
                    , 장르번호
                FROM MOVIE_GENRE

                --------------------------------------------
                -- 중복 장르 번호 제거
                --------------------------------------------
                EXCEPT
                SELECT
                       c41.영화번호
                     , c42.장르번호
                FROM MOVIE c41

                LEFT JOIN MOVIE_GENRE c42 -- 영화 장르 매핑
                ON c41.영화번호 = c42.영화번호

                LEFT JOIN GENRE c43 -- 장르명 매핑
                ON c42.장르번호 = c43.장르번호

                GROUP BY
                      c41.영화번호
                    , c43.장르명
                HAVING COUNT(1) > 1  -- 중복 존재 장르번호 추출
            ) b42
            ON b41.영화번호 = b42.영화번호

            WHERE
                b41.기준년월 BETWEEN {bas_ym_bfr_2m} AND {bas_ym} -- 최근 3개월 조건
            AND b41.평점 IS NOT NULL
            AND b42.장르번호 IS NOT NULL

            GROUP BY
                  b41.회원번호
                , b42.장르번호
        ) a41
        GROUP BY a41.회원번호
    ) T5
    ON T1.회원번호 = T5.회원번호

    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    -- 최근 1개월 장르별 행동건수
    ------------------------------------------------------------------------------------------------------------------------------------------------------------
    LEFT JOIN (
        SELECT
              a51.회원번호

            , SUM(CASE WHEN a52.장르번호 =  7        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_액션_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  7        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_액션_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  7        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_액션_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  7        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_액션_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  7        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_액션_구매횟수


            , SUM(CASE WHEN a52.장르번호 =  3        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_드라마_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  3        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_드라마_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  3        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_드라마_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  3        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_드라마_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  3        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_드라마_구매횟수

            , SUM(CASE WHEN a52.장르번호 =  6        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_코미디_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  6        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_코미디_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  6        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_코미디_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  6        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_코미디_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  6        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_코미디_구매횟수

            , SUM(CASE WHEN a52.장르번호 =  9        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_스릴러_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  9        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_스릴러_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  9        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_스릴러_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  9        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_스릴러_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  9        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_스릴러_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 15        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_로맨스_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 15        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_로맨스_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 15        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_로맨스_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 15        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_로맨스_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 15        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_로맨스_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 17        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_공포_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 17        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_공포_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 17        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_공포_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 17        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_공포_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 17        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_공포_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 45        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_공상과학_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 45        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_공상과학_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 45        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_공상과학_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 45        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_공상과학_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 45        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_공상과학_구매횟수

            , SUM(CASE WHEN a52.장르번호 =  8        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_범죄_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  8        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_범죄_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  8        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_범죄_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  8        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_범죄_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  8        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_범죄_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 12        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_판타지_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 12        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_판타지_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 12        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_판타지_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 12        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_판타지_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 12        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_판타지_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 14        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_가족_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 14        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_가족_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 14        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_가족_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 14        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_가족_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 14        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_가족_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 11        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_모험_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 11        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_모험_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 11        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_모험_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 11        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_모험_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 11        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_모험_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 18        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_전쟁_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 18        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_전쟁_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 18        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_전쟁_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 18        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_전쟁_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 18        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_전쟁_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 16        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_음악_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 16        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_음악_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 16        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_음악_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 16        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_음악_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 16        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_음악_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 25        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_뮤지컬_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 25        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_뮤지컬_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 25        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_뮤지컬_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 25        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_뮤지컬_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 25        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_뮤지컬_구매횟수

            , SUM(CASE WHEN a52.장르번호 IN (46, 53) AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_전기_탐색횟수
            , SUM(CASE WHEN a52.장르번호 IN (46, 53) AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_전기_평가횟수
            , SUM(CASE WHEN a52.장르번호 IN (46, 53) AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_전기_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 IN (46, 53) AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_전기_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 IN (46, 53) AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_전기_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 30        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_스포츠_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 30        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_스포츠_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 30        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_스포츠_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 30        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_스포츠_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 30        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_스포츠_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 19        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_서부극_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 19        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_서부극_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 19        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_서부극_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 19        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_서부극_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 19        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_서부극_구매횟수

            , SUM(CASE WHEN a52.장르번호 =  2        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_애니메이션_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  2        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_애니메이션_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  2        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_애니메이션_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  2        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_애니메이션_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  2        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_애니메이션_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 10        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_다큐멘터리_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 10        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_다큐멘터리_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 10        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_다큐멘터리_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 10        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_다큐멘터리_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 10        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_다큐멘터리_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 20        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_미스테리_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 20        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_미스테리_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 20        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_미스테리_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 20        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_미스테리_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 20        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_미스테리_구매횟수

            , SUM(CASE WHEN a52.장르번호 = 24        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_단막극_탐색횟수
            , SUM(CASE WHEN a52.장르번호 = 24        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_단막극_평가횟수
            , SUM(CASE WHEN a52.장르번호 = 24        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_단막극_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 = 24        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_단막극_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 = 24        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_단막극_구매횟수

            , SUM(CASE WHEN a52.장르번호 =  1        AND a51.행동번호 = 5         THEN 1 ELSE 0 END)          AS 최근1개월_역사_탐색횟수
            , SUM(CASE WHEN a52.장르번호 =  1        AND a51.행동번호 = 1         THEN 1 ELSE 0 END)          AS 최근1개월_역사_평가횟수
            , SUM(CASE WHEN a52.장르번호 =  1        AND a51.행동번호 = 2         THEN 1 ELSE 0 END)          AS 최근1개월_역사_시청완료횟수
            , SUM(CASE WHEN a52.장르번호 =  1        AND a51.행동번호 = 4         THEN 1 ELSE 0 END)          AS 최근1개월_역사_시청시작횟수
            , SUM(CASE WHEN a52.장르번호 =  1        AND a51.행동번호 = 11        THEN 1 ELSE 0 END)          AS 최근1개월_역사_구매횟수

        FROM MOVIE_FACT a51

        LEFT JOIN (
            SELECT
                  영화번호
                , 장르번호
            FROM MOVIE_GENRE

            --------------------------------------------
            -- 중복 장르 번호 제거
            --------------------------------------------
            EXCEPT
            SELECT
                   a01.영화번호
                 , a02.장르번호
            FROM MOVIE a01

            LEFT JOIN MOVIE_GENRE a02 -- 영화 장르 매핑
            ON a01.영화번호 = a02.영화번호

            LEFT JOIN GENRE a03 -- 장르명 매핑
            ON a02.장르번호 = a03.장르번호

            GROUP BY
                  a01.영화번호
                , a03.장르명
            HAVING COUNT(1) > 1  -- 중복 존재 장르번호 추출
        ) a52
        ON a51.영화번호 = a52.영화번호

        WHERE a51.기준년월 = {bas_ym}

        GROUP BY a51.회원번호
    ) T6
    ON T1.회원번호 = T6.회원번호

  """
  )


ed_tm = str(datetime.now()).split(' ')[1].split('.')[0]
el_tm = datetime.strptime(ed_tm, '%H:%M:%S') - datetime.strptime(st_tm, '%H:%M:%S')
el_tm = str(el_tm).split('.')[0].zfill(8)
print(f"[LOG] 피쳐마트 적재 종료 | 시작시간 = {st_tm}, 종료시간 = {ed_tm}, 소요시간 = {el_tm}, ")
