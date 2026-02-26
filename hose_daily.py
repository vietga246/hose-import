from vnstock import *
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import sqlite3
import pandas as pd   # Thiếu import pandas

def get_top_10_lowest_price_stocks(days=7):
    """
    Lấy danh sách top 10 cổ phiếu có giá thấp nhất trong một khoảng thời gian.
    """
    print("Bắt đầu lấy danh sách tất cả các mã cổ phiếu...")
    try:
        # Lấy danh sách toàn bộ công ty niêm yết
        listing = Listing()
        all_tickers = listing.all_symbols()
        print(f"Tìm thấy {len(all_tickers)} mã cổ phiếu. Bắt đầu quét dữ liệu...")
        
        symbols = all_tickers['symbol'].tolist()
        
    except Exception as e:
        print(f"Lỗi khi lấy danh sách cổ phiếu: {e}")
        return

    # Xác định khoảng thời gian từ đầu năm 2000
    end_date = datetime.now()
    start_date = end_date - timedelta(days)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    lowest_prices = []
    
    # Kết nối SQLite
    conn = sqlite3.connect("stock.db")
    cursor = conn.cursor()
    
    # Tạo bảng nếu chưa có
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        symbol TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (symbol, date)
    )
    """)
    conn.commit()
    
    # Sử dụng tqdm để tạo thanh tiến trình
    for ticker in tqdm(symbols, desc="Đang quét giá cổ phiếu"):
        try:
            quote = Quote(symbol=ticker, source="VCI")
            df = quote.history(start=start_date_str, end=end_date_str)
            
            if not df.empty:
                # print(f"Saving {ticker}, {len(df)} dòng dữ liệu")
                for _, row in df.iterrows():
                    try:
                        cursor.execute("""
                        INSERT OR REPLACE INTO stock_prices (symbol, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            ticker,
                            str(row.get("time")),  # dùng get tránh key error
                            float(row.get("open", 0)),
                            float(row.get("high", 0)),
                            float(row.get("low", 0)),
                            float(row.get("close", 0)),
                            int(row.get("volume", 0))
                        ))
                    except Exception as inner_e:
                        print(f"❌ Lỗi khi insert {ticker}: {inner_e}")
                conn.commit()
                
                min_price = df['low'].min()
                lowest_prices.append({'Mã CP': ticker, 'Giá thấp nhất (3 tháng)': min_price})
        except Exception as e:
            print(f"❌ Lỗi khi lấy dữ liệu {ticker}: {e}")
            continue
        time.sleep(2)

            
    conn.close()
    
    if not lowest_prices:
        print("Không có dữ liệu nào được tìm thấy.")
        return
    
    # Tạo DataFrame từ danh sách và sắp xếp
    result_df = pd.DataFrame(lowest_prices)
    result_df = result_df[result_df['Giá thấp nhất (3 tháng)'] > 0]
    top_10_lowest = result_df.sort_values(by='Giá thấp nhất (3 tháng)', ascending=True).head(10)
    
    return top_10_lowest

# --- Chạy chương trình ---
if __name__ == "__main__":
    top_10 = get_top_10_lowest_price_stocks()
    
    if top_10 is not None:
        print("\n--- TOP 10 MÃ CỔ PHIẾU CÓ GIÁ THẤP NHẤT TRONG 3 THÁNG QUA ---")
        top_10['Giá thấp nhất (3 tháng)'] = top_10['Giá thấp nhất (3 tháng)'].apply(lambda x: f"{x:,.0f} VNĐ")
        print(top_10.to_string(index=False))
