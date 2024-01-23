FROM python:3.8-slim

WORKDIR /tests

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["pytest", "test_app.py"]