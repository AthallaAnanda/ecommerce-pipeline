# ingestion/extract.py
import requests
import random
from datetime import date

# Konstanta URL Utama
BASE_URL = "https://fakestoreapi.com"

def extract_products():
    """
    Mengambil data seluruh produk dari API dan menambahkan stempel waktu (audit trail).
    
    Proses:
    1. Melakukan request GET ke endpoint /products.
    2. Menambahkan tanggal snapshot ke setiap produk untuk keperluan tracking history.
    3. Mengembalikan list produk dalam format dictionary.
    """
    try:
        # 1. Mengambil data dari API dengan batas waktu tunggu 10 detik
        response = requests.get(f"{BASE_URL}/products", timeout=10)
        
        # 2. Validasi: Jika status code bukan 200, akan melempar error ke blok except
        response.raise_for_status()
        
        # 3. Parsing data JSON menjadi list of dictionaries
        products = response.json()
        
        # 4. Mendapatkan tanggal hari ini dalam format standar ISO (YYYY-MM-DD)
        today_str = date.today().isoformat()
        
        # 5. Iterasi untuk menambahkan metadata snapshot ke setiap item
        for product in products:
            product["snapshot_date"] = today_str
            
        return products
        
    except requests.exceptions.RequestException as e:
        # Menangani berbagai error koneksi (timeout, DNS, 404, 500, dll)
        print(f"Error Extract Products: {e}")
        # Re-raise agar orkestrator (Airflow) tahu bahwa task gagal
        raise

def extract_users():
    """
    Mengambil data pengguna dari API dan merapikan strukturnya (Flattening).
    
    Proses:
    1. Mengambil JSON dari endpoint /users.
    2. Mengubah field bersarang (nested) menjadi kolom datar.
    3. Menghapus data sensitif seperti password.
    4. Menambahkan stempel tanggal pengambilan data.
    """
    try:
        # 1. Request data ke API dengan timeout untuk menghindari hang
        response = requests.get(f"{BASE_URL}/users", timeout=10)
        response.raise_for_status() # Memastikan status code 200
        users = response.json()
        
        today_str = date.today().isoformat()
        
        for user in users:
            # --- TAHAP FLATTENING ---
            # Menggunakan .get() agar aman jika field tidak ada (tidak bikin crash)
            name = user.get("name", {})
            address = user.get("address", {})
            geo = address.get("geolocation", {})

            # Memindahkan data dari dalam objek ke level atas (flat)
            user["name_firstname"] = name.get("firstname")
            user["name_lastname"] = name.get("lastname")
            user["address_city"] = address.get("city")
            user["lat"] = geo.get("lat")
            user["long"] = geo.get("long")
            
            # --- TAHAP ENRICHMENT ---
            # Tambahkan metadata untuk kebutuhan data warehousing (audit)
            user["snapshot_date"] = today_str

            # --- TAHAP CLEANING ---
            # Hapus objek lama dan data sensitif agar ringan dan aman
            for key in ["name", "address", "password"]:
                user.pop(key, None) # .pop lebih aman dibanding del
                
        return users
    
    except requests.exceptions.RequestException as e:
        print(f"Error Extract Users: {e}")
        raise # Re-raise exception, Airflow akan mark task FAILED
    
def extract_carts():
    """
    Mengambil data keranjang belanja dan melakukan simulasi perubahan kuantitas.
    
    Proses:
    1. Mengambil JSON dari endpoint /carts.
    2. Melakukan pengacakan kuantitas produk (simulasi data).
    3. Menghitung total barang di setiap keranjang.
    """
    try:
        # 1. Hit endpoint /carts
        response = requests.get(f"{BASE_URL}/carts", timeout=10)
        response.raise_for_status()
        carts = response.json()
        
        today_str = date.today().isoformat()

        # 2. Iterasi setiap keranjang yang masuk
        for cart in carts:
            cart["snapshot_date"] = today_str
            
            # --- SIMULASI DATA ---
            # Cek jika ada list produk agar tidak terjadi IndexError
            if cart.get("products"):
                for product in cart["products"]:
                    # Mengalikan kuantitas asli dengan angka acak (0.5 - 2.0)
                    multiplier = random.uniform(0.5, 2.0)
                    # Pastikan minimal 1 dan dibulatkan
                    product["quantity"] = max(1, round(multiplier * product["quantity"]))
            
            # --- AGREGASI ---
            # Menghitung total item dalam satu keranjang belanja
            cart["total_quantity"] = sum(p["quantity"] for p in cart.get("products", []))

        return carts

    except requests.exceptions.RequestException as e:
        print(f"Error Extract Carts: {e}")
        raise # Re-raise exception agar Airflow bisa menandai task sebagai FAILED