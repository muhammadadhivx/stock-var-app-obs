from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base
from models import StockData
import numpy as np
from typing import Optional
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

# create tables at startup
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Stock VaR API")

# DB dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def root():
    return {"message": "Stock VaR API running"}

@app.get("/api/returns/{symbol}")
def get_returns(symbol: str, db: Session = Depends(get_db)):
    rows = db.query(StockData).filter(StockData.symbol == symbol).order_by(StockData.date).all()
    if not rows or len(rows) < 2:
        raise HTTPException(status_code=404, detail="Not enough data for symbol")
    prices = np.array([r.price for r in rows], dtype=float)
    returns = np.diff(prices) / prices[:-1]
    # include dates for chart x-axis (use second date onward)
    dates = [r.date.isoformat() for r in rows][1:]
    return {"symbol": symbol, "dates": dates, "returns": returns.tolist()}

def _parametric_var(returns, level):
    mu = returns.mean()
    sigma = returns.std(ddof=0)
    z = {95: 1.645, 99: 2.326}.get(level, 1.645)
    # VaR (loss) as negative return threshold
    param = mu - z * sigma
    return float(param)

@app.get("/api/var/{symbol}")
def get_var(symbol: str, level: Optional[int] = None, db: Session = Depends(get_db)):
    rows = db.query(StockData).filter(StockData.symbol == symbol).order_by(StockData.date).all()
    if not rows or len(rows) < 2:
        raise HTTPException(status_code=404, detail="Not enough data for symbol")
    prices = np.array([r.price for r in rows], dtype=float)
    returns = np.diff(prices) / prices[:-1]
    levels = [int(level)] if level else [95, 99]
    result = {}
    for lvl in levels:
        hist = float(np.percentile(returns, 100 - lvl))  # e.g. 5th percentile for 95%
        param = _parametric_var(returns, lvl)
        result[str(lvl)] = {"historical_var": hist, "parametric_var": param}
    return {"symbol": symbol, "vars": result}


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)