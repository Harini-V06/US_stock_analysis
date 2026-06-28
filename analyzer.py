"""
US Stock Analyzer — hosted edition (Financial Modeling Prep data)

Dual-horizon analysis only (no trading, no mock data):
• Short-term (1–3 weeks) : technical + quant
• Long-term (5 years)    : fundamentals + valuation + trend

Designed to run on a free host (e.g. Render) behind gunicorn, with data from
Financial Modeling Prep (FMP). Reads the API key from the FMP_API_KEY
environment variable — the key is NEVER hard-coded.

Honest notes (shown in the UI too):
• "Conviction" = how strongly the factors agree (capped 80%), NOT the
  probability the call is right.
• Free FMP plan = 250 requests/day, so the universe is ~50 stocks and data
  refreshes about once a day. Both are easy to raise on a paid tier.
• Indicators are computed in pure Python — no numpy/pandas — so the host
  build is tiny and fast.

PATCH 01 applied:
• Verdicts are now assigned by universe-wide percentile rank, not fixed
  raw-score thresholds. This makes BUY/SELL meaningful regardless of whether
  the whole market is trending up or down.
• Percentile bands shift ±8 pct-points based on the macro regime score.
• pct_rank (0–100) is exposed on each horizon signal for the UI to display.

Local run:
    pip install flask flask-cors requests
    set FMP_API_KEY=your_key   (PowerShell: $env:FMP_API_KEY="your_key")
    python analyzer.py
"""

import os
import time
import math
import threading
from datetime import datetime, timedelta

import requests
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FMP_KEY      = os.environ.get("FMP_API_KEY", "").strip()
FMP_BASE     = "https://financialmodelingprep.com/api/v3"
FINNHUB_KEY  = os.environ.get("FINNHUB_API_KEY", "").strip()


POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 86400))  # re-analyze once/day
RATIOS_TTL_H  = 24          # fundamentals cache (hours)
GROWTH_TTL_H  = 24 * 7     # growth cache (weekly)
TARGET_TTL_H  = 24 * 7     # analyst target cache (weekly)
PRICE_BARS    = 400         # daily bars to pull (enough for MA200 + 1y momentum)
CALL_SLEEP    = 0.25        # gentle pause between API calls

U = [
    ("AAPL","Apple","Technology"),("MSFT","Microsoft","Technology"),
    ("NVDA","NVIDIA","Technology"),("GOOGL","Alphabet","Communication Services"),
    ("AMZN","Amazon","Consumer Cyclical"),("META","Meta Platforms","Communication Services"),
    ("AVGO","Broadcom","Technology"),("AMD","Advanced Micro Devices","Technology"),
    ("ORCL","Oracle","Technology"),("CRM","Salesforce","Technology"),
    ("ADBE","Adobe","Technology"),("ARM","Arm Holdings","Technology"),
    ("PLTR","Palantir","Technology"),("QCOM","Qualcomm","Technology"),
    ("MU","Micron","Technology"),("INTC","Intel","Technology"),
    ("NOW","ServiceNow","Technology"),("SNOW","Snowflake","Technology"),
    ("NET","Cloudflare","Technology"),("CRWD","CrowdStrike","Technology"),
    ("PANW","Palo Alto Networks","Technology"),
    ("NFLX","Netflix","Communication Services"),("DIS","Disney","Communication Services"),
    ("NKE","Nike","Consumer Cyclical"),("SBUX","Starbucks","Consumer Cyclical"),
    ("MCD","McDonald's","Consumer Cyclical"),("COST","Costco","Consumer Defensive"),
    ("WMT","Walmart","Consumer Defensive"),("KO","Coca-Cola","Consumer Defensive"),
    ("PEP","PepsiCo","Consumer Defensive"),
    ("JPM","JPMorgan Chase","Financial Services"),("BAC","Bank of America","Financial Services"),
    ("V","Visa","Financial Services"),("MA","Mastercard","Financial Services"),
    ("GS","Goldman Sachs","Financial Services"),("SOFI","SoFi Technologies","Financial Services"),
    ("COIN","Coinbase","Financial Services"),("PYPL","PayPal","Financial Services"),
    ("UNH","UnitedHealth","Healthcare"),("JNJ","Johnson & Johnson","Healthcare"),
    ("LLY","Eli Lilly","Healthcare"),("PFE","Pfizer","Healthcare"),
    ("ABBV","AbbVie","Healthcare"),("MRK","Merck","Healthcare"),
    ("CAT","Caterpillar","Industrials"),("BA","Boeing","Industrials"),
    ("GE","GE Aerospace","Industrials"),("XOM","Exxon Mobil","Energy"),
    ("CVX","Chevron","Energy"),
    ("TSLA","Tesla","Consumer Cyclical"),("F","Ford","Consumer Cyclical"),
    ("LUNR","Intuitive Machines","Industrials"),("RKLB","Rocket Lab","Industrials"),
    # ETFs (scored on trend only)
    ("SPY","S&P 500 ETF","ETF / Fund"),("QQQ","Nasdaq-100 ETF","ETF / Fund"),
    ("SCHG","Schwab US Large-Cap Growth ETF","ETF / Fund"),("VOO","Vanguard S&P 500 ETF","ETF / Fund"),
]

UNIVERSE = [{"sym": s, "name": n, "sector": sec, "etf": sec == "ETF / Fund"} for s, n, sec in U]
ETF_SET  = {u["sym"] for u in UNIVERSE if u["etf"]}
BENCH    = "SPY"

def verdict(score):
    """
    Raw-score verdict — used during per-stock assembly, before the full
    universe is known. After run_cycle() completes, normalize_verdicts()
    overwrites these with percentile-based verdicts.
    """
    if score >= 0.45:   return "STRONG BUY"
    elif score >= 0.18: return "BUY"
    elif score <= -0.45: return "STRONG SELL"
    elif score <= -0.18: return "SELL"
    return "HOLD"


def verdict_from_pct(pct, macro_score=0.0):
    """
    Percentile-based verdict (PATCH 01).

    pct         : 0–100, this stock's rank in the full universe this cycle.
    macro_score : from market_regime()['score'], range [-1, +1].

    Default bands (neutral macro):
        STRONG BUY  >= 88th pct   (~top 12%)
        BUY         >= 68th pct   (~top 32%)
        HOLD        32nd–68th pct (~middle 36%)
        SELL        <= 32nd pct   (~bottom 32%)
        STRONG SELL <= 12th pct   (~bottom 12%)

    Regime shift (max ±8 pct-points):
        Risk-on  (macro > 0.3) → top band HARDER to reach (strong stocks
                                  are common; only outliers deserve STRONG BUY)
        Risk-off (macro < -0.3)→ top band EASIER to reach (holding up in a
                                  down market deserves credit)
    """
    shift = max(-8.0, min(8.0, float(macro_score) * 10.0))

    sb_lo = 88 - shift   # STRONG BUY lower bound
    b_lo  = 68 - shift   # BUY lower bound
    s_hi  = 32 + shift   # SELL upper bound  (at or below → SELL)
    ss_hi = 12 + shift   # STRONG SELL upper bound

    if pct >= sb_lo:    return "STRONG BUY"
    elif pct >= b_lo:   return "BUY"
    elif pct <= ss_hi:  return "STRONG SELL"
    elif pct <= s_hi:   return "SELL"
    return "HOLD"


def normalize_verdicts(signals, macro_score=0.0):
    """
    Re-assign verdicts across the full universe using percentile rank (PATCH 01).

    Called once at the end of run_cycle(), after all stocks are scored.
    Modifies `signals` in place — overwrites 'verdict' and adds 'pct_rank'
    on each horizon dict.

    Parameters
    ----------
    signals     : dict  sym -> signal dict (must have 'st' and 'lt' with 'score')
    macro_score : float from market_regime()['score'], range [-1, +1]
    """
    if not signals:
        return

    syms = list(signals.keys())

    for horizon in ("st", "lt"):
        scored = [
            (sym, signals[sym][horizon]["score"])
            for sym in syms
            if horizon in signals[sym]
        ]
        if not scored:
            continue

        # Sort ascending: rank 0 = weakest score
        sorted_pairs = sorted(scored, key=lambda x: x[1])
        n = len(sorted_pairs)

        for rank, (sym, _) in enumerate(sorted_pairs):
            pct = round(rank / (n - 1) * 100) if n > 1 else 50
            signals[sym][horizon]["verdict"]  = verdict_from_pct(pct, macro_score)
            signals[sym][horizon]["pct_rank"] = pct

    # Update combined verdict using weighted-average percentile (LT 55%, ST 45%)
    for sym in syms:
        if "combined" not in signals[sym]:
            continue
        st_pct   = signals[sym].get("st", {}).get("pct_rank", 50)
        lt_pct   = signals[sym].get("lt", {}).get("pct_rank", 50)
        comb_pct = round(lt_pct * 0.55 + st_pct * 0.45)
        signals[sym]["combined"]["verdict"]  = verdict_from_pct(comb_pct, macro_score)
        signals[sym]["combined"]["pct_rank"] = comb_pct


def conviction(score, factors):
    mag       = abs(score)
    aligned   = sum(abs(f["contrib"]) for f in factors if (f["contrib"] >= 0) == (score >= 0))
    total     = sum(abs(f["contrib"]) for f in factors) or 1e-9
    agreement = aligned / total
    raw       = (50 + 30 * mag) * (0.7 + 0.3 * agreement)
    return int(max(50, min(80, round(raw))))


def conviction_tier(c):
    return "High" if c >= 72 else ("Moderate" if c >= 60 else "Low")


state = {
    "signals": {}, "sectors": [], "last_updated": None,
    "status": "starting", "progress": {"done": 0, "total": len(UNIVERSE)},
    "universe_size": len(UNIVERSE), "macro": None,
}
lock   = threading.Lock()
_cache = {}   # key -> (value, fetched_at)



def sma(v, p):
    return sum(v[-p:]) / p if len(v) >= p else None

def clamp(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))

def ema_series(v, span):
    k = 2 / (span + 1); out = []; e = v[0]
    for i, x in enumerate(v):
        e = x if i == 0 else x * k + e * (1 - k)
        out.append(e)
    return out

def rsi(v, period=14):
    if len(v) < period + 2: return 50.0
    d  = [v[i+1] - v[i] for i in range(len(v) - 1)]
    ag = sum(x for x in d[:period] if x > 0) / period
    al = (sum(-x for x in d[:period] if x < 0) / period) or 1e-9
    for x in d[period:]:
        g = x if x > 0 else 0.0; l = -x if x < 0 else 0.0
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
    rs = ag / (al or 1e-9)
    return round(100 - 100 / (1 + rs), 2)

def macd(v):
    if len(v) < 35: return 0.0, 0.0, False, False
    e12  = ema_series(v, 12); e26 = ema_series(v, 26)
    line = [a - b for a, b in zip(e12, e26)]
    sig  = ema_series(line, 9)
    up   = line[-1] > sig[-1] and line[-3] <= sig[-3]
    dn   = line[-1] < sig[-1] and line[-3] >= sig[-3]
    return round(line[-1], 3), round(sig[-1], 3), up, dn

def stdev(v):
    n = len(v); m = sum(v) / n
    return (sum((x - m) ** 2 for x in v) / n) ** 0.5

def bollinger_pctb(v, period=20):
    if len(v) < period: return 0.5
    seg = v[-period:]; m = sum(seg) / period; sd = stdev(seg)
    return 0.5 if sd == 0 else (v[-1] - (m - 2 * sd)) / (4 * sd)

def momentum(v, days):
    if len(v) < days + 1: return 0.0
    return round((v[-1] - v[-days-1]) / v[-days-1] * 100, 2)

def rel_strength(v, b, days):
    if len(v) < days + 1 or len(b) < days + 1: return 0.0
    return round(((v[-1]-v[-days-1])/v[-days-1] - (b[-1]-b[-days-1])/b[-days-1]) * 100, 2)

def atr_pct(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
           for i in range(1, len(closes))]
    return round(sum(trs[-period:]) / period / closes[-1] * 100, 2)

def ann_vol(closes, days=63):
    if len(closes) < days + 1: return None
    seg  = closes[-days-1:]
    rets = [(seg[i+1]-seg[i])/seg[i] for i in range(len(seg)-1)]
    return round(stdev(rets) * math.sqrt(252) * 100, 1)

def max_drawdown(closes, days=252):
    seg = closes[-days:] if len(closes) >= days else closes
    peak = seg[0]; mdd = 0.0
    for x in seg:
        peak = max(peak, x); mdd = min(mdd, (x - peak) / peak)
    return round(mdd * 100, 1)

def pos_52w(closes):
    seg = closes[-252:] if len(closes) >= 252 else closes
    lo, hi = min(seg), max(seg)
    return 50 if hi == lo else int((closes[-1]-lo)/(hi-lo)*100)

def _r(x):
    try:   return round(float(x), 2)
    except (TypeError, ValueError): return None

def _pct(x):
    try:   return round(float(x) * 100, 1)
    except (TypeError, ValueError): return None


ST_W = {"trend":0.25, "momentum":0.30, "meanrev":0.10, "volume":0.10, "relstr":0.15, "quant":0.10}
LT_W = {"growth":0.20, "profit":0.15, "health":0.15, "valuation":0.20, "sentiment":0.10, "trend":0.20}

def _f(name, sub, w):
    return {"name": name, "score": round(sub, 2), "weight": w, "contrib": round(sub * w, 4)}

def _assemble(score, factors, reasons, metrics, price):
    c  = conviction(score, factors)
    fs = sorted(factors, key=lambda f: abs(f["contrib"]), reverse=True)
    return {
        "verdict":    verdict(score),
        "score":      score,
        "conviction": c,
        "tier":       conviction_tier(c),
        "price":      round(price, 2),
        "reasons":    reasons[:8],
        "factors":    [{"name": f["name"], "score": f["score"], "weight": int(f["weight"]*100)} for f in fs],
        "metrics":    metrics,
        # pct_rank will be filled in by normalize_verdicts() after the full cycle
        "pct_rank":   None,
    }


def short_term_signal(closes, highs, lows, vols, spy):
    price = closes[-1]; factors = []; reasons = []


    ma20, ma50, ma200 = sma(closes, 20), sma(closes, 50), sma(closes, 200)
    t = 0.0
    if ma50 and ma200:
        if ma50 > ma200: t += 0.6; reasons.append("Uptrend: MA50 above MA200 (golden cross)")
        else:            t -= 0.6; reasons.append("Downtrend: MA50 below MA200 (death cross)")
    if ma20:
        if price > ma20: t += 0.4; reasons.append("Price above 20-day average")
        else:            t -= 0.4; reasons.append("Price below 20-day average")
    factors.append(_f("Trend", clamp(t), ST_W["trend"]))


    r = rsi(closes); mval, msig, up, dn = macd(closes); m = 0.0
    if r < 30:   m += 0.6; reasons.append(f"RSI {r} — oversold, bounce potential")
    elif r < 45: m += 0.2
    elif r > 70: m -= 0.6; reasons.append(f"RSI {r} — overbought")
    elif r > 55: m -= 0.2
    if up:           m += 0.6; reasons.append("MACD bullish crossover")
    elif mval > msig: m += 0.3
    elif dn:          m -= 0.6; reasons.append("MACD bearish crossover")
    else:             m -= 0.3
    factors.append(_f("Momentum", clamp(m), ST_W["momentum"]))

    pb = bollinger_pctb(closes); mr = 0.0
    if pb < 0.05:   mr += 0.8; reasons.append("Below lower Bollinger band — stretched down")
    elif pb < 0.2:  mr += 0.4
    elif pb > 0.95: mr -= 0.8; reasons.append("Above upper Bollinger band — stretched up")
    elif pb > 0.8:  mr -= 0.4
    factors.append(_f("Mean reversion", clamp(mr), ST_W["meanrev"]))

    
    v = 0.0
    if len(vols) >= 20:
        vr = (sum(vols[-5:]) / 5) / ((sum(vols[-20:]) / 20) or 1e-9)
        d  = 1 if m >= 0 else -1
        if vr > 1.8:  v = d * 0.8; reasons.append(f"Volume surging ({vr:.1f}x avg) — confirms move")
        elif vr > 1.3: v = d * 0.4
        elif vr < 0.6: v = -d * 0.3; reasons.append("Thin volume — weak conviction")
    factors.append(_f("Volume", clamp(v), ST_W["volume"]))

    rs15 = rel_strength(closes, spy, 15)
    if rs15 > 5:   reasons.append(f"Outperforming S&P by {rs15:.1f}% (3wk)")
    elif rs15 < -5: reasons.append(f"Lagging S&P by {abs(rs15):.1f}% (3wk)")
    factors.append(_f("Relative strength", clamp(rs15 / 10.0), ST_W["relstr"]))

   
    av = ann_vol(closes); q = 0.0
    if av is not None:
        if av > 60:  q = -0.4; reasons.append(f"Very high volatility ({av}%/yr) — elevated risk")
        elif av > 40: q = -0.2
        elif av < 20: q = 0.2
    factors.append(_f("Quant / risk", clamp(q), ST_W["quant"]))

    score   = round(sum(f["contrib"] for f in factors), 3)
    metrics = {
        "RSI (14)":         r,
        "MACD":             mval,
        "MACD signal":      msig,
        "Bollinger %b":     round(pb * 100),
        "Momentum 5d":      momentum(closes, 5),
        "Momentum 10d":     momentum(closes, 10),
        "vs S&P (15d) %":   rs15,
        "ATR %":            atr_pct(highs, lows, closes),
        "Volatility %/yr":  av,
        "MA20":             _r(ma20),
        "MA50":             _r(ma50),
        "MA200":            _r(ma200),
    }
    return _assemble(score, factors, reasons, metrics, price)


def long_term_signal(closes, spy, fund, is_etf):
    price = closes[-1]; factors = []; reasons = []

    if is_etf:
        for k in ("growth", "profit", "health", "valuation", "sentiment"):
            factors.append(_f(k.title(), 0.0, LT_W[k]))
        reasons.append("ETF / fund — company fundamentals not applicable; trend-based only")
    else:
        rev  = fund.get("rev",    0); earn = fund.get("earn",   0)
        pm   = fund.get("pm",     0); roe  = fund.get("roe",    0)
        roa  = fund.get("roa",    0); de   = fund.get("de",     0)
        cr   = fund.get("cr",     0); fcfy = fund.get("fcfy",   0)
        pe   = fund.get("pe",     0); peg  = fund.get("peg",    0)
        ps   = fund.get("ps",     0); tgt  = fund.get("target", 0)
        growth_sector = fund.get("growth_sector", False)

        # Growth
        g = 0.0
        if rev > 0.30:   g += 0.6; reasons.append(f"Revenue growth {rev*100:.0f}% — exceptional")
        elif rev > 0.15: g += 0.4; reasons.append(f"Revenue growth {rev*100:.0f}% — strong")
        elif rev > 0.05: g += 0.1
        elif rev < 0:    g -= 0.5; reasons.append("Revenue declining")
        if earn > 0.20:   g += 0.4; reasons.append(f"Earnings growth {earn*100:.0f}%")
        elif earn > 0.05: g += 0.2
        elif earn < -0.10: g -= 0.4; reasons.append("Earnings shrinking")
        factors.append(_f("Growth", clamp(g), LT_W["growth"]))

        # Profitability
        p = 0.0
        if pm > 0.25:   p += 0.5; reasons.append(f"High net margin {pm*100:.0f}%")
        elif pm > 0.10: p += 0.2
        elif pm < 0:    p -= 0.5; reasons.append("Unprofitable (negative margin)")
        if roe > 0.25:   p += 0.4; reasons.append(f"Excellent ROE {roe*100:.0f}%")
        elif roe > 0.12: p += 0.2
        elif roe < 0:    p -= 0.3
        if roa > 0.10:   p += 0.1
        factors.append(_f("Profitability", clamp(p), LT_W["profit"]))

        # Financial health
        h = 0.0
        if fcfy > 0.04:   h += 0.5; reasons.append(f"Strong free-cash-flow yield {fcfy*100:.1f}%")
        elif fcfy > 0.01: h += 0.2
        elif fcfy < 0:    h -= 0.4; reasons.append("Negative free cash flow")
        if 0 < de < 0.5:  h += 0.3; reasons.append("Low debt load")
        elif de > 2.0:    h -= 0.4; reasons.append("High debt load")
        if cr > 2:          h += 0.2
        elif 0 < cr < 1:   h -= 0.3; reasons.append("Weak liquidity (current ratio < 1)")
        factors.append(_f("Financial health", clamp(h), LT_W["health"]))

        # Valuation
        val = 0.0
        cheap, fair, rich = (30, 50, 80) if growth_sector else (18, 30, 55)
        if pe > 0:
            if pe < cheap:   val += 0.6; reasons.append(f"Attractive P/E {pe:.0f}")
            elif pe < fair:  val += 0.2
            elif pe > rich:  val -= 0.6; reasons.append(f"Expensive P/E {pe:.0f}")
            elif pe > fair:  val -= 0.2
        if peg > 0:
            if peg < 1:  val += 0.4; reasons.append(f"PEG {peg:.2f} < 1 — cheap vs growth")
            elif peg > 3: val -= 0.4; reasons.append(f"PEG {peg:.2f} — growth fully priced in")
        factors.append(_f("Valuation", clamp(val), LT_W["valuation"]))

        # Sentiment (analyst price target)
        s = 0.0
        if tgt and price:
            up = (tgt - price) / price
            if up > 0.30:    s += 0.6; reasons.append(f"Analyst target implies +{up*100:.0f}%")
            elif up > 0.10:  s += 0.3
            elif up < -0.10: s -= 0.5; reasons.append(f"Analyst target implies {up*100:.0f}%")
        factors.append(_f("Sentiment", clamp(s), LT_W["sentiment"]))

    # Long-run trend (all stocks including ETFs)
    ma50, ma200 = sma(closes, 50), sma(closes, 200); tr = 0.0
    if ma50 and ma200:
        if ma50 > ma200: tr += 0.4; reasons.append("Long-term uptrend intact (MA50>MA200)")
        else:            tr -= 0.4; reasons.append("Long-term downtrend (MA50<MA200)")
    mom1y = momentum(closes, min(252, len(closes)-1))
    rs1y  = rel_strength(closes, spy, min(252, len(closes)-1, len(spy)-1))
    if mom1y > 20:  tr += 0.3; reasons.append(f"Up {mom1y:.0f}% over ~1y")
    elif mom1y < -20: tr -= 0.3; reasons.append(f"Down {abs(mom1y):.0f}% over ~1y")
    if rs1y > 10:   tr += 0.3; reasons.append(f"Beating S&P by {rs1y:.0f}% (1y)")
    elif rs1y < -10: tr -= 0.3
    factors.append(_f("Long-term trend", clamp(tr), LT_W["trend"]))

    score   = round(sum(f["contrib"] for f in factors), 3)
    metrics = {}
    if not is_etf:
        metrics = {
            "Revenue growth %":  _pct(fund.get("rev")),
            "Earnings growth %": _pct(fund.get("earn")),
            "Net margin %":      _pct(fund.get("pm")),
            "ROE %":             _pct(fund.get("roe")),
            "Debt/Equity":       _r(fund.get("de")),
            "Current ratio":     _r(fund.get("cr")),
            "Forward P/E":       _r(fund.get("pe")),
            "PEG":               _r(fund.get("peg")),
            "P/S":               _r(fund.get("ps")),
            "Analyst target":    ("$" + str(_r(fund.get("target")))) if fund.get("target") else "—",
        }
        if fund.get("edgar_rev"):
            b = fund["edgar_rev"] / 1e9
            metrics[f"Revenue (SEC FY{fund.get('edgar_fy','')})"] = f"${b:,.1f}B"

    metrics.update({
        "1y return %":       momentum(closes, min(252, len(closes)-1)),
        "vs S&P (1y) %":     rel_strength(closes, spy, min(252, len(closes)-1, len(spy)-1)),
        "Max drawdown %":    max_drawdown(closes),
        "52w range pos %":   pos_52w(closes),
        "MA50":              _r(ma50),
        "MA200":             _r(ma200),
    })
    return _assemble(score, factors, reasons, metrics, price)


def fmp(path, params=None):
    params = dict(params or {}); params["apikey"] = FMP_KEY
    try:
        r = requests.get(f"{FMP_BASE}/{path}", params=params, timeout=20)
        time.sleep(CALL_SLEEP)
        if r.status_code == 429:
            print("  FMP rate limit hit (429) — backing off"); return None
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        print("  FMP error:", e); return None


def cached(key, ttl_h, fetch):
    hit = _cache.get(key)
    if hit and datetime.now() - hit[1] < timedelta(hours=ttl_h):
        return hit[0]
    val = fetch()
    if val is not None:
        _cache[key] = (val, datetime.now())
    elif hit:
        return hit[0]   # serve stale on failure
    return val


def get_prices(sym):
    data = fmp(f"historical-price-full/{sym}", {"timeseries": PRICE_BARS})
    if not data or "historical" not in data: return None
    hist   = list(reversed(data["historical"]))   # oldest → newest
    closes = [h["close"]                for h in hist if h.get("close") is not None]
    highs  = [h.get("high",  h["close"]) for h in hist if h.get("close") is not None]
    lows   = [h.get("low",   h["close"]) for h in hist if h.get("close") is not None]
    vols   = [h.get("volume", 0) or 0    for h in hist if h.get("close") is not None]
    return (closes, highs, lows, vols) if len(closes) >= 60 else None

# ── SEC EDGAR (official filings — primary, authenticated source, no key) ──────
EDGAR_UA      = os.environ.get("EDGAR_UA", "stock-analyzer personal-research contact@example.com")
EDGAR_HEADERS = {"User-Agent": EDGAR_UA, "Accept-Encoding": "gzip, deflate"}

REV_CONCEPTS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
]
NI_CONCEPTS = ["NetIncomeLoss", "ProfitLoss"]


def edgar_get(url):
    try:
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=25)
        time.sleep(0.2)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def load_cik_map():
    def fetch():
        d = edgar_get("https://www.sec.gov/files/company_tickers.json")
        if not d: return None
        return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in d.values()}
    return cached("cikmap", 24, fetch)


def _annual_series(data, concepts):
    """Return {year -> value} for full-year (≈365-day) facts, by period-end year."""
    facts = data.get("facts", {}).get("us-gaap", {})
    for c in concepts:
        usd = (facts.get(c, {}).get("units", {}) or {}).get("USD")
        if not usd: continue
        by_year = {}
        for u in usd:
            s, e, val = u.get("start"), u.get("end"), u.get("val")
            if not (s and e and val is not None): continue
            try:
                sy, sm, sd = map(int, s.split("-")); ey, em, ed = map(int, e.split("-"))
                span = (datetime(ey, em, ed) - datetime(sy, sm, sd)).days
            except Exception:
                continue
            if 350 <= span <= 380:
                by_year[ey] = val   # later filings overwrite (restatements)
        if by_year:
            return by_year
    return {}


def get_edgar(sym):
    def fetch():
        m = load_cik_map()
        if not m: return None
        cik = m.get(sym.upper())
        if not cik: return None
        data = edgar_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")
        if not data: return None
        rev = _annual_series(data, REV_CONCEPTS)
        ni  = _annual_series(data, NI_CONCEPTS)
        if not rev: return None
        years  = sorted(rev.keys())
        latest = years[-1]; prev = years[-2] if len(years) >= 2 else None
        out    = {"rev_latest": rev[latest], "fy": latest}
        if prev and rev.get(prev):
            out["rev_growth"] = (rev[latest] - rev[prev]) / rev[prev]
        if latest in ni and rev[latest]:
            out["net_margin"] = ni[latest] / rev[latest]
        return out
    return cached(f"edgar:{sym}", GROWTH_TTL_H, fetch)


def get_fundamentals(sym, sector):
    ratios = cached(f"ratios:{sym}",  RATIOS_TTL_H,
                    lambda: fmp(f"ratios-ttm/{sym}"))
    growth = cached(f"growth:{sym}",  GROWTH_TTL_H,
                    lambda: fmp(f"financial-growth/{sym}", {"period": "annual", "limit": 1}))
    target = cached(f"target:{sym}",  TARGET_TTL_H,
                    lambda: fmp(f"price-target-consensus", {"symbol": sym}))

    r = (ratios[0] if isinstance(ratios, list) and ratios else {}) or {}
    g = (growth[0] if isinstance(growth, list) and growth else {}) or {}
    t = (target[0] if isinstance(target, list) and target else {}) or {}

    def pick(d, *keys):
        for k in keys:
            if d.get(k) not in (None, ""):
                try:   return float(d[k])
                except (TypeError, ValueError): pass
        return 0.0

    fund = {
        "rev":    pick(g, "growthRevenue",    "revenueGrowth"),
        "earn":   pick(g, "growthNetIncome",  "epsgrowth",        "growthEPS"),
        "pm":     pick(r, "netProfitMarginTTM",   "netProfitMargin"),
        "roe":    pick(r, "returnOnEquityTTM",    "returnOnEquity"),
        "roa":    pick(r, "returnOnAssetsTTM",    "returnOnAssets"),
        "de":     pick(r, "debtEquityRatioTTM",   "debtToEquityTTM",   "debtEquityRatio"),
        "cr":     pick(r, "currentRatioTTM",      "currentRatio"),
        "fcfy":   pick(r, "freeCashFlowYieldTTM", "freeCashFlowYield"),
        "pe":     pick(r, "peRatioTTM",           "priceEarningsRatioTTM", "peRatio"),
        "peg":    pick(r, "pegRatioTTM",          "priceEarningsToGrowthRatioTTM", "pegRatio"),
        "ps":     pick(r, "priceToSalesRatioTTM", "priceSalesRatioTTM"),
        "target": pick(t, "targetConsensus",      "targetMean",        "priceTargetAverage"),
        "growth_sector": sector in ("Technology", "Communication Services", "Consumer Cyclical"),
    }

    # ── Cross-check / ground against official SEC EDGAR filings ──────────────
    edgar      = get_edgar(sym)
    confidence = "FMP data"; edgar_rev = None; edgar_fy = None; flag = None

    if edgar:
        edgar_fy  = edgar.get("fy"); edgar_rev = edgar.get("rev_latest")
        confidence = "SEC-verified"
        fmp_rev_growth = fund["rev"]
        if edgar.get("rev_growth") is not None:
            if fmp_rev_growth and abs(fmp_rev_growth - edgar["rev_growth"]) > 0.15:
                flag = "FMP & SEC differ on revenue growth — verify"
            fund["rev"] = edgar["rev_growth"]
        if edgar.get("net_margin") is not None:
            fund["pm"] = edgar["net_margin"]

    fund["confidence"] = confidence
    fund["flag"]       = flag
    fund["edgar_rev"]  = edgar_rev
    fund["edgar_fy"]   = edgar_fy
    return fund

# ── Finnhub: aggregated Wall Street analyst consensus (free tier) ─────────────
def finnhub_get(path, params=None):
    params = dict(params or {}); params["token"] = FINNHUB_KEY
    try:
        r = requests.get(f"https://finnhub.io/api/v1/{path}", params=params, timeout=20)
        time.sleep(0.1)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def get_consensus(sym):
    if not FINNHUB_KEY:
        return None
    data = cached(f"cons:{sym}", RATIOS_TTL_H,
                  lambda: finnhub_get("stock/recommendation", {"symbol": sym}))
    if not isinstance(data, list) or not data:
        return None
    d  = max(data, key=lambda x: x.get("period", ""))
    sb = d.get("strongBuy", 0); b = d.get("buy", 0); h = d.get("hold", 0)
    s  = d.get("sell",      0); ss = d.get("strongSell", 0)
    total = sb + b + h + s + ss
    if total == 0:
        return None
    score = (sb*1 + b*0.5 + h*0 - s*0.5 - ss*1) / total
    return {
        "buy": sb+b, "hold": h, "sell": s+ss, "total": total,
        "verdict": verdict(score), "score": round(score, 3),
    }

# ── Market regime (computed macro signal from the broad market) ───────────────
def market_regime(spy):
    ma200 = sma(spy, 200); price = spy[-1]
    mom   = momentum(spy, min(126, len(spy)-1)); vol = ann_vol(spy)
    score = 0.0; notes = []
    if ma200 and price > ma200: score += 0.5; notes.append("S&P above its 200-day average")
    elif ma200:                 score -= 0.5; notes.append("S&P below its 200-day average")
    if mom > 5:    score += 0.3; notes.append(f"S&P +{mom:.0f}% over ~6mo")
    elif mom < -5: score -= 0.3; notes.append(f"S&P {mom:.0f}% over ~6mo")
    if vol and vol > 25: score -= 0.2; notes.append(f"elevated volatility ({vol:.0f}%/yr)")
    score  = clamp(score)
    regime = "Risk-on" if score > 0.3 else ("Risk-off" if score < -0.3 else "Neutral")
    return {"regime": regime, "score": round(score, 2), "note": "; ".join(notes) or "mixed"}

# ── Combined view: model (ST+LT) + analyst consensus + macro ─────────────────
def combined_view(st, lt, cons, macro_score):
    factors  = [{"contrib": lt["score"]*0.35}, {"contrib": st["score"]*0.25}]
    opinions = [lt["score"], st["score"]]
    if cons:
        factors.append({"contrib": cons["score"]*0.30}); opinions.append(cons["score"])
    factors.append({"contrib": macro_score*0.10})
    score = round(sum(f["contrib"] for f in factors), 3)
    dirs  = [1 if o > 0.05 else (-1 if o < -0.05 else 0) for o in opinions]
    nz    = [d for d in dirs if d != 0]
    if not nz:                             label = "No clear edge"
    elif all(d == nz[0] for d in nz):     label = "Aligned"
    else:                                  label = "Mixed signals"
    return {
        "verdict":     verdict(score),
        "score":       score,
        "confidence":  conviction(score, factors),
        "label":       label,
        "has_analyst": bool(cons),
        "pct_rank":    None,   # filled by normalize_verdicts()
    }

# ══════════════════════════════════════════════════════════════════════════════
# ANALYSIS CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def run_cycle():
    if not FMP_KEY:
        with lock: state["status"] = "no_api_key"
        print("  ⚠ FMP_API_KEY is not set — add it as an environment variable.")
        return

    with lock:
        state["status"]   = "fetching"
        state["progress"] = {"done": 0, "total": len(UNIVERSE)}

    print(f"\n[{datetime.now():%H:%M:%S}] Analyzing {len(UNIVERSE)} stocks via FMP…")

    spy_p = get_prices(BENCH)
    spy   = spy_p[0] if spy_p else [1.0] * 300
    macro = market_regime(spy)

    with lock:
        state["macro"] = macro

    new     = {}; sectors = set(); done = 0

    for u in UNIVERSE:
        sym = u["sym"]
        try:
            pr = get_prices(sym)
            if pr:
                closes, highs, lows, vols = pr
                fund     = {} if u["etf"] else get_fundamentals(sym, u["sector"])
                st       = short_term_signal(closes, highs, lows, vols, spy)
                lt       = long_term_signal(closes, spy, fund, u["etf"])
                cons     = None if u["etf"] else get_consensus(sym)
                combined = combined_view(st, lt, cons, macro["score"])
                day_chg  = round((closes[-1]-closes[-2])/closes[-2]*100, 2) if len(closes) > 1 else 0.0

                new[sym] = {
                    "name":            u["name"],
                    "sector":          u["sector"],
                    "price":           round(closes[-1], 2),
                    "day_change":      day_chg,
                    "data_confidence": ("n/a (ETF)" if u["etf"] else fund.get("confidence", "FMP data")),
                    "flag":            (None if u["etf"] else fund.get("flag")),
                    "consensus":       cons,
                    "combined":        combined,
                    "st":              st,
                    "lt":              lt,
                }
                sectors.add(u["sector"])
        except Exception as e:
            print(f"  {sym}: {e}")
        finally:
            done += 1
            if done % 5 == 0 or done == len(UNIVERSE):
                with lock:
                    state["signals"]  = dict(new)
                    state["progress"] = {"done": done, "total": len(UNIVERSE)}
                    state["status"]   = "analyzing"

    # ── PATCH 01: re-assign verdicts using universe percentile ranks ──────────
    normalize_verdicts(new, macro.get("score", 0.0) if macro else 0.0)

    with lock:
        state["signals"]      = new
        state["sectors"]      = sorted(sectors)
        state["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state["status"]       = "running" if new else "error"
        state["progress"]     = {"done": len(UNIVERSE), "total": len(UNIVERSE)}

    print(f"  ✓ analyzed {len(new)} stocks.")


def loop():
    while True:
        try:   run_cycle()
        except Exception as e: print("cycle error:", e)
        time.sleep(POLL_INTERVAL)


_worker_started = False

def start_worker():
    global _worker_started
    with lock:
        if _worker_started: return
        _worker_started = True
    threading.Thread(target=loop, daemon=True).start()

start_worker()

# ── Routes ────────────────────────────────────────────────────────────────────
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
    port = int(os.environ.get("PORT", 5001))
    print("=" * 60)
    print("  US Stock Analyzer (hosted edition · FMP data)")
    print(f"  Universe : {len(UNIVERSE)} stocks · analysis only, no trades")
    print(f"  API key  : {'set ✓' if FMP_KEY else 'NOT SET — add FMP_API_KEY'}")
    print(f"  Dashboard: http://localhost:{port}/")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=False)
