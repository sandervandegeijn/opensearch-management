FROM python:3.12-alpine
COPY . /app/
RUN chmod 777 /app/main.py
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt