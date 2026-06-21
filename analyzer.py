"""
US Stock Analyzer  —  dual-horizon (short-term 1-3 weeks  +  long-term 5 years)

This is an ANALYSIS TOOL only. It does NOT place trades, hold positions, or use
any mock/paper money. It scores each stock on Fundamentals + Technicals + Quant
and gives a verdict (STRONG BUY / BUY / HOLD / SELL / STRONG SELL) plus a
*conviction* score for each horizon.

IMPORTANT, read this:
  - "Conviction" measures how strongly the factors agree — it is NOT the
    probability the call is correct. It is deliberately capped at 80%.
  - Technical/fundamental factors describe conditions; they do not reliably
    predict prices. Treat output as a screen, not advice. Not financial advice.

Run:  pip install flask flask-cors yfinance numpy pandas
      python analyzer.py
Open: http://localhost:5001/
"""

import time
import threading
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ──────────────────────────────────────────────────────────────────────────────
#  UNIVERSE  — ~130 liquid US names across every sector (incl. your holdings).
#  To analyze MORE: just add tickers to this list.
#  To load the full S&P 500 instead, set USE_SP500 = True (slower first run).
# ──────────────────────────────────────────────────────────────────────────────
USE_SP500 = False

UNIVERSE = [
    # Mega/large-cap tech & semis
    "AAPL","MSFT","NVDA","GOOGL","GOOG","AMZN","META","AVGO","TSLA","AMD","ORCL",
    "ADBE","CRM","CSCO","ACN","INTC","QCOM","TXN","AMAT","MU","ARM","PLTR","NOW",
    "INTU","IBM","ADI","LRCX","KLAC","SNPS","CDNS","PANW","CRWD","SNOW","NET",
    "DDOG","MRVL",
    # Communication / consumer
    "NFLX","DIS","CMCSA","T","VZ","TMUS","NKE","SBUX","MCD","HD","LOW","TGT",
    "COST","WMT","PG","KO","PEP","PM","MO","MDLZ","CL",
    # Financials
    "JPM","BAC","WFC","GS","MS","C","SCHW","BLK","AXP","V","MA","PYPL","SOFI","COIN",
    # Healthcare
    "UNH","JNJ","LLY","PFE","MRK","ABBV","TMO","ABT","DHR","BMY","AMGN","GILD",
    "CVS","MDT","ISRG",
    # Industrials / energy
    "CAT","DE","BA","GE","HON","UPS","RTX","LMT","XOM","CVX","COP","SLB","ENB",
    # Autos / EV / space
    "F","GM","RIVN","LCID","LUNR","RKLB",
    # High-growth / internet
    "UBER","ABNB","SHOP","SQ","ROKU","PINS","SNAP","RBLX","DKNG","U",
    # Broad ETFs (fundamentals limited — scored on trend/quant)
    "SPY","QQQ","VOO","VTI","SCHG","SCHD","VXUS","IWM","DIA",
]

BENCH = "SPY"

POLL_INTERVAL = 1800      # re-analyze prices every 30 min (avoids rate limits)
FUND_TTL_HOURS = 12       # fundamentals change slowly — cache for 12h
HISTORY_PERIOD = "5y"     # 5y daily covers both the 5y LT view and the ST slice

# ── Revised verdict scale (documented) ────────────────────────────────────────
# Net score is in [-1, +1]. Thresholds tightened so STRONG isn't over-called.
def verdict(score):
    if   score >=  0.45: return "STRONG BUY"
    elif score >=  0.18: return "BUY"
    elif score <= -0.45: return "STRONG SELL"
    elif score <= -0.18: return "SELL"
    else:                return "HOLD"

# ── Honest conviction (NOT probability), capped at 80, floored at 50 ───────────
def conviction(score, factors):
    mag = abs(score)
    aligned = sum(abs(f["contrib"]) for f in factors if (f["contrib"] >= 0) == (score >= 0))
    total   = sum(abs(f["contrib"]) for f in factors) or 1e-9
    agreement = aligned / total                      # 0.5 .. 1.0 typically
    raw = (50 + 30 * mag) * (0.7 + 0.3 * agreement)  # never exceeds 80
    return int(max(50, min(80, round(raw))))

def conviction_tier(c):
    if c >= 72: return "High"
    if c >= 60: return "Moderate"
    return "Low"

state = {
    "signals": {},
    "sectors": [],
    "last_updated": None,
    "status": "starting",
    "progress": {"done": 0, "total": len(UNIVERSE)},
    "universe_size": len(UNIVERSE),
}
lock = threading.Lock()
_fund_cache = {}   # symbol -> (info_dict, fetched_at)

# ══════════════════════════════════════════════════════════════════════════════
#  INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
def rsi(closes, period=14):
    if len(closes) < period + 2: return 50.0
    d = np.diff(closes.astype(float))
    g = np.where(d > 0, d, 0.0); l = np.where(d < 0, -d, 0.0)
    ag = g[:period].mean(); al = l[:period].mean() or 1e-9
    for i in range(period, len(d)):
        ag = (ag*(period-1)+g[i]) / period
        al = (al*(period-1)+l[i]) / period
    rs = ag / (al or 1e-9)
    return round(100 - 100/(1+rs), 2)

def macd(closes):
    s = pd.Series(closes.astype(float))
    e12 = s.ewm(span=12, adjust=False).mean(); e26 = s.ewm(span=26, adjust=False).mean()
    m = e12 - e26; sig = m.ewm(span=9, adjust=False).mean()
    up = m.iloc[-1] > sig.iloc[-1] and m.iloc[-3] <= sig.iloc[-3]
    dn = m.iloc[-1] < sig.iloc[-1] and m.iloc[-3] >= sig.iloc[-3]
    return round(m.iloc[-1],3), round(sig.iloc[-1],3), bool(up), bool(dn)

def bollinger_pctb(closes, period=20):
    if len(closes) < period: return 0.5
    s = pd.Series(closes.astype(float)).tail(period)
    sma = s.mean(); std = s.std()
    if std == 0: return 0.5
    return float((closes[-1] - (sma - 2*std)) / (4*std))

def ma(closes, p):
    return float(pd.Series(closes.astype(float)).tail(p).mean()) if len(closes) >= p else None

def momentum(closes, days):
    if len(closes) < days+1: return 0.0
    return round((closes[-1]-closes[-days-1]) / closes[-days-1] * 100, 2)

def rel_strength(closes, bench, days):
    if len(closes) < days+1 or len(bench) < days+1: return 0.0
    sr = (closes[-1]-closes[-days-1]) / closes[-days-1]
    br = (bench[-1]-bench[-days-1]) / bench[-days-1]
    return round((sr-br)*100, 2)

def atr_pct(highs, lows, closes, period=14):
    if len(closes) < period+1: return None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    return round(float(np.mean(trs[-period:]) / closes[-1] * 100), 2)

def ann_vol(closes, days=63):
    if len(closes) < days+1: return None
    rets = np.diff(closes[-days-1:]) / closes[-days-1:-1]
    return round(float(np.std(rets) * np.sqrt(252) * 100), 1)

def max_drawdown(closes, days=252):
    seg = closes[-days:] if len(closes) >= days else closes
    peak = np.maximum.accumulate(seg)
    return round(float(((seg - peak) / peak).min() * 100), 1)

def pos_52w(closes):
    seg = closes[-252:] if len(closes) >= 252 else closes
    lo, hi = float(seg.min()), float(seg.max())
    if hi == lo: return 50
    return int((closes[-1]-lo)/(hi-lo)*100)

def clamp(x, lo=-1.0, hi=1.0): return max(lo, min(hi, x))

# ══════════════════════════════════════════════════════════════════════════════
#  SHORT-TERM SIGNAL (1–3 weeks)  — technical + quant heavy
#  Each factor returns a sub-score in [-1, +1]; weights sum to 1.0.
# ══════════════════════════════════════════════════════════════════════════════
ST_WEIGHTS = {"trend":0.25, "momentum":0.30, "meanrev":0.10,
              "volume":0.10, "relstr":0.15, "quant":0.10}

def short_term_signal(closes, highs, lows, volumes, spy):
    price = float(closes[-1]); factors = []; reasons = []

    # Trend
    ma20, ma50, ma200 = ma(closes,20), ma(closes,50), ma(closes,200)
    t = 0.0
    if ma50 and ma200:
        if ma50 > ma200: t += 0.6; reasons.append("Uptrend: MA50 above MA200 (golden cross)")
        else:            t -= 0.6; reasons.append("Downtrend: MA50 below MA200 (death cross)")
    if ma20:
        if price > ma20: t += 0.4; reasons.append("Price above 20-day average")
        else:            t -= 0.4; reasons.append("Price below 20-day average")
    factors.append(_f("Trend", clamp(t), ST_WEIGHTS["trend"]))

    # Momentum (RSI + MACD)
    r = rsi(closes); mval, msig, up, dn = macd(closes)
    m = 0.0
    if   r < 30: m += 0.6; reasons.append(f"RSI {r} — oversold, bounce potential")
    elif r < 45: m += 0.2
    elif r > 70: m -= 0.6; reasons.append(f"RSI {r} — overbought")
    elif r > 55: m -= 0.2
    if   up:            m += 0.6; reasons.append("MACD bullish crossover")
    elif mval > msig:   m += 0.3
    elif dn:            m -= 0.6; reasons.append("MACD bearish crossover")
    else:              m -= 0.3
    factors.append(_f("Momentum", clamp(m), ST_WEIGHTS["momentum"]))

    # Mean reversion (Bollinger %b)
    pb = bollinger_pctb(closes); mr = 0.0
    if   pb < 0.05: mr += 0.8; reasons.append("Below lower Bollinger band — stretched down")
    elif pb < 0.2:  mr += 0.4
    elif pb > 0.95: mr -= 0.8; reasons.append("Above upper Bollinger band — stretched up")
    elif pb > 0.8:  mr -= 0.4
    factors.append(_f("Mean reversion", clamp(mr), ST_WEIGHTS["meanrev"]))

    # Volume confirmation
    v = 0.0
    if len(volumes) >= 20:
        vr = float(np.mean(volumes[-5:]) / (np.mean(volumes[-20:]) or 1e-9))
        direction = 1 if m >= 0 else -1
        if   vr > 1.8: v = direction*0.8; reasons.append(f"Volume surging ({vr:.1f}x avg) — confirms move")
        elif vr > 1.3: v = direction*0.4
        elif vr < 0.6: v = -direction*0.3; reasons.append("Thin volume — weak conviction")
    factors.append(_f("Volume", clamp(v), ST_WEIGHTS["volume"]))

    # Relative strength vs SPY (~15 trading days)
    rs15 = rel_strength(closes, spy, 15); q = clamp(rs15/10.0)
    if   rs15 > 5:  reasons.append(f"Outperforming S&P by {rs15:.1f}% (3wk)")
    elif rs15 < -5: reasons.append(f"Lagging S&P by {abs(rs15):.1f}% (3wk)")
    factors.append(_f("Relative strength", q, ST_WEIGHTS["relstr"]))

    # Quant risk (volatility regime) — high vol dampens directional conviction
    av = ann_vol(closes); quant = 0.0
    if av is not None:
        if av > 60:  quant = -0.4; reasons.append(f"Very high volatility ({av}%/yr) — elevated risk")
        elif av > 40: quant = -0.2
        elif av < 20: quant = 0.2
    factors.append(_f("Quant / risk", clamp(quant), ST_WEIGHTS["quant"]))

    score = round(sum(f["contrib"] for f in factors), 3)
    metrics = {
        "RSI (14)": r, "MACD": mval, "MACD signal": msig,
        "Bollinger %b": round(pb*100), "Momentum 5d": momentum(closes,5),
        "Momentum 10d": momentum(closes,10), "vs S&P (15d) %": rs15,
        "ATR %": atr_pct(highs,lows,closes), "Volatility %/yr": av,
        "MA20": _r(ma20), "MA50": _r(ma50), "MA200": _r(ma200),
    }
    return _assemble(score, factors, reasons, metrics, price)

# ══════════════════════════════════════════════════════════════════════════════
#  LONG-TERM SIGNAL (5 years)  — fundamentals heavy + valuation + quant trend
# ══════════════════════════════════════════════════════════════════════════════
LT_WEIGHTS = {"growth":0.20, "profit":0.15, "health":0.15,
              "valuation":0.20, "sentiment":0.10, "trend":0.20}

def long_term_signal(closes, spy, info, is_etf):
    price = float(closes[-1]); factors = []; reasons = []

    if is_etf:
        # ETFs: no company fundamentals — score purely on long-run trend/quant
        for k in ("growth","profit","health","valuation","sentiment"):
            factors.append(_f(k.title(), 0.0, LT_WEIGHTS[k]))
        reasons.append("ETF / fund — company fundamentals not applicable; trend-based only")
    else:
        rev   = info.get("revenueGrowth") or 0
        earn  = info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth") or 0
        pm    = info.get("profitMargins") or 0
        roe   = info.get("returnOnEquity") or 0
        roa   = info.get("returnOnAssets") or 0
        de    = info.get("debtToEquity") or 0
        cr    = info.get("currentRatio") or 0
        fcf   = info.get("freeCashflow") or 0
        mcap  = info.get("marketCap") or 1
        pe    = info.get("forwardPE") or info.get("trailingPE") or 0
        peg   = info.get("pegRatio") or 0
        ps    = info.get("priceToSalesTrailing12Months") or 0
        tgt   = info.get("targetMeanPrice") or 0
        rec   = (info.get("recommendationKey") or "").lower()
        sector= info.get("sector","")
        growth_sector = any(x in sector for x in ["Technology","Communication","Consumer Cyclical"])

        # Growth
        g = 0.0
        if   rev > 0.30: g += 0.6; reasons.append(f"Revenue growth {rev*100:.0f}% — exceptional")
        elif rev > 0.15: g += 0.4; reasons.append(f"Revenue growth {rev*100:.0f}% — strong")
        elif rev > 0.05: g += 0.1
        elif rev < 0:    g -= 0.5; reasons.append("Revenue declining")
        if   earn > 0.20: g += 0.4; reasons.append(f"Earnings growth {earn*100:.0f}%")
        elif earn > 0.05: g += 0.2
        elif earn < -0.10: g -= 0.4; reasons.append("Earnings shrinking")
        factors.append(_f("Growth", clamp(g), LT_WEIGHTS["growth"]))

        # Profitability
        p = 0.0
        if   pm > 0.25: p += 0.5; reasons.append(f"High net margin {pm*100:.0f}%")
        elif pm > 0.10: p += 0.2
        elif pm < 0:    p -= 0.5; reasons.append("Unprofitable (negative margin)")
        if   roe > 0.25: p += 0.4; reasons.append(f"Excellent ROE {roe*100:.0f}%")
        elif roe > 0.12: p += 0.2
        elif roe < 0:    p -= 0.3
        if roa > 0.10: p += 0.1
        factors.append(_f("Profitability", clamp(p), LT_WEIGHTS["profit"]))

        # Financial health
        h = 0.0
        fcfy = fcf/mcap if mcap else 0
        if   fcfy > 0.04: h += 0.5; reasons.append(f"Strong free-cash-flow yield {fcfy*100:.1f}%")
        elif fcfy > 0.01: h += 0.2
        elif fcfy < 0:    h -= 0.4; reasons.append("Negative free cash flow")
        if   0 < de < 50:  h += 0.3; reasons.append("Low debt load")
        elif de > 200:     h -= 0.4; reasons.append("High debt load")
        if   cr > 2:  h += 0.2
        elif 0 < cr < 1: h -= 0.3; reasons.append("Weak liquidity (current ratio < 1)")
        factors.append(_f("Financial health", clamp(h), LT_WEIGHTS["health"]))

        # Valuation
        val = 0.0
        cheap, fair, rich = (30,50,80) if growth_sector else (18,30,55)
        if pe > 0:
            if   pe < cheap: val += 0.6; reasons.append(f"Attractive P/E {pe:.0f}")
            elif pe < fair:  val += 0.2
            elif pe > rich:  val -= 0.6; reasons.append(f"Expensive P/E {pe:.0f}")
            elif pe > fair:  val -= 0.2
        if peg > 0:
            if   peg < 1:  val += 0.4; reasons.append(f"PEG {peg:.2f} < 1 — cheap vs growth")
            elif peg > 3:  val -= 0.4; reasons.append(f"PEG {peg:.2f} — growth fully priced in")
        factors.append(_f("Valuation", clamp(val), LT_WEIGHTS["valuation"]))

        # Sentiment (analyst targets + rating)
        s = 0.0
        if tgt and price:
            up = (tgt-price)/price
            if   up > 0.30: s += 0.6; reasons.append(f"Analyst target implies +{up*100:.0f}%")
            elif up > 0.10: s += 0.3
            elif up < -0.10: s -= 0.5; reasons.append(f"Analyst target implies {up*100:.0f}%")
        s += {"strong_buy":0.4,"buy":0.25,"hold":0.0,"underperform":-0.3,"sell":-0.4}.get(rec,0.0)
        factors.append(_f("Sentiment", clamp(s), LT_WEIGHTS["sentiment"]))

    # Long-run trend / quant (applies to everything incl. ETFs)
    ma50, ma200 = ma(closes,50), ma(closes,200)
    tr = 0.0
    if ma50 and ma200:
        if ma50 > ma200: tr += 0.4; reasons.append("Long-term uptrend intact (MA50>MA200)")
        else:            tr -= 0.4; reasons.append("Long-term downtrend (MA50<MA200)")
    mom1y = momentum(closes, min(252, len(closes)-1))
    rs1y  = rel_strength(closes, spy, min(252, len(closes)-1, len(spy)-1))
    if   mom1y > 20: tr += 0.3; reasons.append(f"Up {mom1y:.0f}% over ~1y")
    elif mom1y < -20: tr -= 0.3; reasons.append(f"Down {abs(mom1y):.0f}% over ~1y")
    if   rs1y > 10: tr += 0.3; reasons.append(f"Beating S&P by {rs1y:.0f}% (1y)")
    elif rs1y < -10: tr -= 0.3
    factors.append(_f("Long-term trend", clamp(tr), LT_WEIGHTS["trend"]))

    score = round(sum(f["contrib"] for f in factors), 3)
    metrics = {}
    if not is_etf:
        metrics = {
            "Revenue growth %": _pct(info.get("revenueGrowth")),
            "Earnings growth %": _pct(info.get("earningsGrowth") or info.get("earningsQuarterlyGrowth")),
            "Net margin %": _pct(info.get("profitMargins")),
            "ROE %": _pct(info.get("returnOnEquity")),
            "Debt/Equity": _r(info.get("debtToEquity")),
            "Current ratio": _r(info.get("currentRatio")),
            "Forward P/E": _r(info.get("forwardPE") or info.get("trailingPE")),
            "PEG": _r(info.get("pegRatio")),
            "P/S": _r(info.get("priceToSalesTrailing12Months")),
            "Analyst target": ("$"+str(round(info.get("targetMeanPrice"),2))) if info.get("targetMeanPrice") else "—",
            "Rating": info.get("recommendationKey","—"),
        }
    metrics.update({
        "1y return %": momentum(closes, min(252, len(closes)-1)),
        "vs S&P (1y) %": rel_strength(closes, spy, min(252, len(closes)-1, len(spy)-1)),
        "Max drawdown %": max_drawdown(closes),
        "52w range pos %": pos_52w(closes),
        "MA50": _r(ma50), "MA200": _r(ma200),
    })
    return _assemble(score, factors, reasons, metrics, price)

# ── helpers to package factor + signal dicts ──────────────────────────────────
def _f(name, sub, weight):
    return {"name": name, "score": round(sub,2), "weight": weight,
            "contrib": round(sub*weight,4)}

def _assemble(score, factors, reasons, metrics, price):
    c = conviction(score, factors)
    factors_sorted = sorted(factors, key=lambda f: abs(f["contrib"]), reverse=True)
    return {
        "verdict": verdict(score), "score": score,
        "conviction": c, "tier": conviction_tier(c),
        "price": round(price,2),
        "reasons": reasons[:8],
        "factors": [{"name":f["name"], "score":f["score"], "weight":int(f["weight"]*100)}
                    for f in factors_sorted],
        "metrics": metrics,
    }

def _r(x):
    try: return round(float(x),2)
    except (TypeError, ValueError): return None

def _pct(x):
    try: return round(float(x)*100,1)
    except (TypeError, ValueError): return None

# ══════════════════════════════════════════════════════════════════════════════
#  DATA FETCH + ANALYSIS CYCLE
# ══════════════════════════════════════════════════════════════════════════════
def load_universe():
    if not USE_SP500:
        return sorted(set(UNIVERSE))
    try:  # optional: pull full S&P 500 (runs on your machine)
        tbl = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        syms = [s.replace(".","-") for s in tbl["Symbol"].tolist()]
        return sorted(set(syms + ["SPY","QQQ","VOO"]))
    except Exception as e:
        print("  S&P500 load failed, using built-in list:", e)
        return sorted(set(UNIVERSE))

def get_fundamentals(sym):
    hit = _fund_cache.get(sym)
    if hit and datetime.now() - hit[1] < timedelta(hours=FUND_TTL_HOURS):
        return hit[0]
    try:
        info = yf.Ticker(sym).info or {}
    except Exception:
        info = {}
    _fund_cache[sym] = (info, datetime.now())
    return info

def extract(df, sym, multi):
    try:
        sub = df[sym] if multi else df
        sub = sub.dropna(how="all")
        if sub.empty or "Close" not in sub: return None
        return sub
    except Exception:
        return None

ETF_SET = {"SPY","QQQ","VOO","VTI","SCHG","SCHD","VXUS","IWM","DIA"}

def run_cycle():
    syms = load_universe()
    with lock:
        state["status"] = "fetching prices"
        state["progress"] = {"done": 0, "total": len(syms)}
        state["universe_size"] = len(syms)
    print(f"\n[{datetime.now():%H:%M:%S}] Downloading {len(syms)} tickers (5y daily)…")

    dl = sorted(set(syms + [BENCH]))
    try:
        df = yf.download(dl, period=HISTORY_PERIOD, interval="1d",
                         group_by="ticker", auto_adjust=True, threads=True, progress=False)
    except Exception as e:
        print("  bulk download failed:", e)
        with lock: state["status"] = "error"
        return
    multi = isinstance(df.columns, pd.MultiIndex)

    spy_sub = extract(df, BENCH, multi)
    spy = spy_sub["Close"].dropna().values if spy_sub is not None else np.ones(300)

    new = {}; sectors = set(); done = 0
    for sym in syms:
        try:
            sub = extract(df, sym, multi)
            if sub is None or len(sub["Close"].dropna()) < 60:
                done += 1; continue
            closes = sub["Close"].dropna().values
            highs  = sub["High"].dropna().values
            lows   = sub["Low"].dropna().values
            vols   = sub["Volume"].dropna().values
            is_etf = sym in ETF_SET
            info   = {} if is_etf else get_fundamentals(sym)
            name   = info.get("shortName") or info.get("longName") or sym
            sector = "ETF / Fund" if is_etf else (info.get("sector") or "—")
            sectors.add(sector)

            day_chg = round((closes[-1]-closes[-2])/closes[-2]*100, 2) if len(closes) > 1 else 0.0
            st = short_term_signal(closes, highs, lows, vols, spy)
            lt = long_term_signal(closes, spy, info, is_etf)

            new[sym] = {"name": name, "sector": sector, "price": round(float(closes[-1]),2),
                        "day_change": day_chg, "st": st, "lt": lt}
        except Exception as e:
            print(f"  {sym}: {e}")
        finally:
            done += 1
            if done % 10 == 0 or done == len(syms):
                with lock:
                    state["signals"] = dict(new)
                    state["progress"] = {"done": done, "total": len(syms)}
                    state["status"] = "analyzing"

    with lock:
        state["signals"] = new
        state["sectors"] = sorted(s for s in sectors if s)
        state["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state["status"] = "running"
        state["progress"] = {"done": len(syms), "total": len(syms)}
    print(f"  ✓ analyzed {len(new)} stocks.")

def loop():
    while True:
        try: run_cycle()
        except Exception as e: print("cycle error:", e)
        time.sleep(POLL_INTERVAL)

# ── routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "dashboard.html")

@app.route("/api/status")
def api_status():
    with lock:
        return jsonify(state)

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=run_cycle, daemon=True).start()
    return jsonify({"message": "refresh started"})

if __name__ == "__main__":
    print("="*60)
    print("  US Stock Analyzer  ·  Short-Term (1–3wk) + Long-Term (5y)")
    print(f"  Universe: {len(load_universe())} stocks  ·  analysis only, no trades")
    print("  Dashboard → http://localhost:5001/")
    print("="*60)
    threading.Thread(target=loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5001, debug=False)