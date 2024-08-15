import logging
import os
from datetime import datetime, time

# 로그 디렉토리 설정 (필요시 경로를 변경하세요)
log_directory = "c:/Users/user/OneDrive - 파인트리파트너스(주)/movie/log"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# 현재 날짜에 따른 로그 파일 이름 생성
log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
log_filepath = os.path.join(log_directory, log_filename)

# 로거 설정
logger = logging.getLogger("DailyLogger")
logger.setLevel(logging.INFO)

# 파일 핸들러 생성
file_handler = logging.FileHandler(log_filepath)
file_handler.setLevel(logging.INFO)

# 로그 포맷 설정
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 기존 핸들러 제거 (이전 핸들러가 존재할 경우)
if logger.hasHandlers():
    logger.handlers.clear()

# 핸들러 추가
logger.addHandler(file_handler)

# 로그 기록 함수
def log_message(message, start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
    global log_filename, log_filepath, file_handler

    # 현재 시간 가져오기
    current_time = datetime.now().time()

    # 시작 시간과 종료 시간 확인
    # 로그 파일 날짜가 바뀌었는지 확인하여 파일 핸들러 재설정
    current_log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    if current_log_filename != log_filename:
        # 전역 변수 값 업데이트
        log_filename = current_log_filename
        log_filepath = os.path.join(log_directory, log_filename)
        logger.removeHandler(file_handler)
        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
        # 로그 기록
        logger.info(message)
        