import pytest
import requests

BASE_URL = 'http://flask:5000'

def test_get_users():
    response = requests.get(f'{BASE_URL}/visited_domains')
    assert response.status_code == 400