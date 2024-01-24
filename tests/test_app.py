import time
import math
from datetime import datetime
from http import HTTPStatus

import pytest
import requests
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

BASE_URL = 'http://flask:5000'
DATABASE_URL = 'postgresql://postgres:postgres@db/postgres'

engine = create_engine(DATABASE_URL, echo=True)
Base = declarative_base()

class VisitedLink(Base):
    __tablename__ = 'visited_links'

    id = Column(Integer, primary_key=True)
    link = Column(String(255), nullable=False)
    visited_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.reflect(engine)
Base.metadata.create_all(engine, tables=[VisitedLink.__table__])


Session = sessionmaker(bind=engine)
session = Session()


def test_add_empty_list():
    data = {"links": []}
    response = requests.post(f"{BASE_URL}/visited_links", json=data)
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json().get("status") == "Empty list"


def test_add_links():
    data = {"links": ["https://dzen.ru/news/quotes/1?issue_tld=ru",
                      "https://vk.com/feed",
                      "https://www.youtube.com/feed/subscriptions"]}
    time_before = datetime.utcnow()

    response = requests.post(f"{BASE_URL}/visited_links", json=data)

    time_after = datetime.utcnow()

    result = session.query(VisitedLink.link).filter(
        VisitedLink.visited_at.between(time_before, time_after)
    ).all()

    links = [row[0] for row in result]

    session.query(VisitedLink).delete()
    session.commit()
    session.close()

    assert response.status_code == HTTPStatus.OK
    assert response.json().get("status") == "ok"
    assert links == data["links"]


def test_get_domains_without_params():
    response = requests.get(f'{BASE_URL}/visited_domains')
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json().get("status") == "Parameters 'from' and 'to' are required"

def test_get_domains_with_wrong_params():
    response = requests.get(f"{BASE_URL}/visited_domains?from=2024-01-24T09:00:34Z&to=2024-01-24T10:00:34Z")
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json().get("status") == "Values of parameters must be timestamps"

def test_get_domains_with_from_greater():
    response = requests.get(f'{BASE_URL}/visited_domains?from=100&to=1')
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert response.json().get("status") == "Parameter 'from' is greater than parameter 'to'"


def test_get_domains_ok():
    data = {"links": [
        "https://ya.ru/",
        "https://ya.ru/search/?text=мемы+с+котиками",
        "https://sber.ru",
        "https://stackoverflow.com/questions/65724760/how-it-is",
        "https://www.stackoverflow.com/"
       ]}
    time_before = str(math.floor(datetime.utcnow().timestamp()))

    requests.post(f"{BASE_URL}/visited_links", json=data)

    time_after = str(math.ceil(datetime.utcnow().timestamp()))

    response = requests.get(f"{BASE_URL}/visited_domains?from={time_before}&to={time_after}")

    assert response.status_code == HTTPStatus.OK
    assert response.json().get("status") == "ok"
    assert set(response.json().get("domains")) == {"ya.ru", "sber.ru", "stackoverflow.com"}
