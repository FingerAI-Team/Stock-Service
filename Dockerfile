FROM pytorch/pytorch:2.1.0-cuda12.1-cudnn8-devel
WORKDIR /ibk 
COPY . .
# 한글 입력을 위한 환경 변수 설정 
ENV LC_ALL=ko_KR.UTF-8 

RUN apt-get update && apt-get install -y locales
RUN locale-gen ko_KR.UTF-8   
RUN apt-get install python3-pip -y
RUN apt-get install vim -y 
RUN apt-get update && apt-get install git -y
RUN pip install -r requirements.txt 