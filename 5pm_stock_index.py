import sqlite3

def run_sql_script(db_path="stock.db", sql_file="save_6m_data_index.sql"):
    # Kết nối SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Đọc file SQL
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_script = f.read()

    try:
        cursor.executescript(sql_script)
        conn.commit()
        print("✅ SQL script executed successfully")
    except Exception as e:
        print(f"❌ Error executing SQL script: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_sql_script()
