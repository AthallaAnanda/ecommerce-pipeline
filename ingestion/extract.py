# ingestion/extract.py
import requests
import random
from datetime import date

BASE_URL = "https://fakestoreapi.com"

def extract_products():
    # 1. Hit endpoint /products
    # 2. Untuk setiap product, tambahkan snapshot_date = hari ini
    # 3. Return list of dict
    pass

def extract_users():
    # 1. Hit endpoint /users
    # 2. Flatten nested fields (name.firstname, address.city, dll)
    # 3. Tambahkan snapshot_date
    # 4. Return list of dict
    pass

def extract_carts():
    # 1. Hit endpoint /carts
    # 2. Simulasi quantity dengan random multiplier (0.5 - 2.0)
    # 3. Tambahkan snapshot_date
    # 4. Return list of dict
    pass