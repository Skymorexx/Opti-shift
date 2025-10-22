import psycopg2
from werkzeug.security import generate_password_hash

# 1. Adımdaki adresi buraya yapıştırın
DATABASE_URL = "postgresql://opti_shift_db_user:YUBHCAvbSh2Lg8gr9qEPcH9tKAqwizQC@dpg-d3sc0is9c44c73cmdfs0-a.frankfurt-postgres.render.com/opti_shift_db"

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Önce üniteyi oluştur
cursor.execute("INSERT INTO units (name) VALUES ('Kartal Dermatoloji') RETURNING id;")
unit_id = cursor.fetchone()[0]

# Sonra kullanıcıyı oluştur
password_hash = generate_password_hash("kartalderma123")
cursor.execute("INSERT INTO users (username, password, unit_id) VALUES (%s, %s, %s);",
               ('kartal_derma', password_hash, unit_id))

conn.commit()
cursor.close()
conn.close()
print("Kullanıcı başarıyla oluşturuldu!")