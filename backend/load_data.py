import pandas as pd
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base, StockData

# 1️⃣ Buat tabel kalau belum ada
Base.metadata.create_all(bind=engine)

# 2️⃣ Fungsi untuk load dan simpan data
def load_csv_to_db(filename: str, symbol: str):
    print(f"Loading data for {symbol} from {filename}...")

    try:
        df = pd.read_csv(filename, sep=None, engine='python', encoding='utf-8-sig')
    except Exception as e:
        print(f"Gagal baca file {filename}: {e}")
        return

    df.columns = [c.strip().capitalize() for c in df.columns]

    rename_map = {
        "Close": "Price",
        "Adj close": "Price",
        "Vol.": "Volume",
        "Vol": "Volume"
    }
    df = df.rename(columns=rename_map)
    def parse_volume(v):
        if pd.isna(v):
            return 0.0
        v = str(v).upper().replace(",", "").strip()
        if v.endswith("M"):
            return float(v[:-1]) * 1_000_000
        elif v.endswith("K"):
            return float(v[:-1]) * 1_000
        else:
            return float(v)

    df["Volume"] = df["Volume"].apply(parse_volume)
    required_cols = ["Date", "Open", "High", "Low", "Price", "Volume"]
    for col in required_cols:
        if col not in df.columns:
            print(f"Kolom {col} tidak ditemukan di {filename}. Kolom tersedia: {df.columns.tolist()}")
            return

    # Ubah format tanggal
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]) 

    # 3️⃣ Simpan ke database
    db = SessionLocal()
    count = 0
    for _, row in df.iterrows():
        stock = StockData(
            symbol=symbol,
            date=row["Date"],
            price=row["Price"],
            open=row["Open"],
            high=row["High"],
            low=row["Low"],
            volume=row["Volume"],
        )
        db.add(stock)
        count += 1

    db.commit()
    db.close()
    print(f"Selesai load {symbol}! {count} baris data dimasukkan.\n")

if __name__ == "__main__":
    load_csv_to_db("data/AAPL.csv", "AAPL")
    load_csv_to_db("data/GOOGL.csv", "GOOGL")
