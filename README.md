# US Stock Analyzer

A dual-horizon stock analysis tool that scores 57 US stocks across short-term technical signals (1–3 weeks) and long-term fundamental signals (5 years). Served as a live auto-refreshing dashboard via Flask.

**Analysis only — not financial advice. No trades are executed.**

<img width="956" height="453" alt="0" src="https://github.com/user-attachments/assets/82886323-b8d3-4b0b-81ff-ec284b213c79" />

---

## What it does

Each stock gets two independent scores:

**Short-term (1–3 weeks)** — technicals and quant signals
- Trend: MA20 / MA50 / MA200 alignment
- Momentum: RSI (Wilder smoothing) + MACD crossover
- Mean reversion: Bollinger %b
- Volume: surge/thin vs 20-day average, confirmed by actual price direction
- Relative strength vs S&P 500 (15-day)
- Volatility risk: annualised vol penalty

**Long-term (5 years)** — fundamentals and valuation
- Growth: revenue + earnings growth (SEC EDGAR cross-checked)
- Profitability: net margin + ROE + ROA
- Financial health: FCF yield + debt/equity + current ratio
- Valuation: P/E scored vs live sector median (not hardcoded bands) + PEG
- Sentiment: analyst price target upside
- Long-term trend: MA50/MA200 + 1-year return vs S&P

A **combined view** blends both horizons (LT 35%, ST 25%) with analyst consensus (30%) and macro regime (10%).

---

## Signals and scoring

Verdicts (STRONG BUY / BUY / HOLD / SELL / STRONG SELL) are assigned by **universe-wide percentile rank**, not fixed score thresholds. BUY means top ~30% of the current universe this cycle. The bands shift ±8 percentile points based on the macro regime — harder to get STRONG BUY when the whole market is trending up, easier when it's risk-off.

**Conviction** (50–80%) measures how strongly the factors agree, not the probability the call is right. It is deliberately capped at 80%.

**Data coverage warning** — stocks with fewer than 60% of core fundamental fields available (new IPOs, spin-offs) have their long-term score discounted and show a ⚠ warning.

---

## Data sources

| Source | Used for | Key required |
|--------|----------|-------------|
| Financial Modeling Prep (stable API) | Price history, ratios, growth, analyst targets | Yes — `FMP_API_KEY` |
| SEC EDGAR (XBRL) | Cross-check revenue figures against official filings | No |
| Finnhub | Wall St. analyst buy/hold/sell consensus | Optional — `FINNHUB_API_KEY` |

> **Free FMP plan = 250 requests/day.** A full 57-stock cycle uses ~180 calls. Results are cached for 24 hours so the quota is only consumed once per day on restart.

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/Harini-V06/US_stock_analysis
cd US_stock_analysis
pip install flask flask-cors requests python-dotenv
```

### 2. Create a `.env` file

```
FMP_API_KEY=your_fmp_key_here
FINNHUB_API_KEY=your_finnhub_key_here
POLL_INTERVAL=86400
```

Get your FMP key at [financialmodelingprep.com](https://financialmodelingprep.com) (free tier is sufficient).  
Get your Finnhub key at [finnhub.io](https://finnhub.io) (free tier, optional — hides consensus column if absent).

### 3. Run

```bash
# bash / Mac / Linux
python analyzer.py

# Windows PowerShell
python analyzer.py
```

Open **http://localhost:5001** in your browser. The first cycle takes ~5 minutes on the free FMP plan. After that, results are cached and load instantly.

---

## Configuration

All config is in `analyzer.py` or `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `FMP_API_KEY` | — | Required. FMP API key |
| `FINNHUB_API_KEY` | — | Optional. Adds analyst consensus column |
| `EDGAR_UA` | `stock-analyzer personal-research contact@example.com` | User-agent for SEC EDGAR (required by SEC ToS) |
| `POLL_INTERVAL` | `86400` | Seconds between full re-analysis cycles (1 day) |
| `FETCH_WORKERS` | `2` | Parallel fetch threads. Keep at 2 on free FMP plan |
| `PRICE_BARS` | `250` | Daily bars fetched per stock (~1 year) |
| `PORT` | `5001` | Flask port |

> Do not raise `FETCH_WORKERS` above 3 or lower `CALL_SLEEP` below 0.4 on the free FMP plan — you will hit 429 rate limits and burn your daily quota.

---

## Dashboard features

- **Macro regime banner** — Risk-on / Neutral / Risk-off based on S&P 500 position, momentum, and volatility
- **Filter tabs** — All / Short-term buys / Short-term sells / Long-term buys / Long-term sells
- **Sector filter** — dropdown to isolate a sector
- **Sort** — by conviction, day move, or A–Z
- **Search** — by ticker or company name
- **Expandable cards** — click any card for full factor breakdown, metrics, and reasons
- **Percentile rank** — shown next to each verdict (e.g. "78th") so you can see exactly where a stock ranks
- **Auto-refresh** — polls `/api/status` every 20 seconds
- **Data coverage warning** — ⚠ badge on stocks with sparse fundamental data

---

## Tech stack

| Component | Technology |
|-----------|-----------|
| Backend | Python 3, Flask, flask-cors |
| Data | Financial Modeling Prep (stable API), SEC EDGAR XBRL |
| Indicators | Pure Python — no numpy or pandas |
| Frontend | Vanilla JS, HTML/CSS (single file) |
| Concurrency | `concurrent.futures.ThreadPoolExecutor` |

---

## Patches applied

| # | Description |
|---|-------------|
| 01 | Percentile-based verdicts — relative to universe, not fixed thresholds |
| 02 | Volume direction fix — uses actual price movement, not momentum sub-score |
| 03 | Sector-relative P/E — scores vs live sector median |
| 04 | Missing data tracking — discounts LT scores when fundamental coverage is sparse |
| 05 | Robust MACD crossover — held-sign detection with stability check |
| 06 | Parallel fetch — ThreadPoolExecutor with per-symbol timeout |
| 07 | FMP stable API migration — all endpoints updated from deprecated api/v3 |

---

## Disclaimer

This tool is for **research and educational purposes only**. It does not constitute financial advice. Scores and verdicts are relative rankings within a fixed universe — they are not predictions of future price movements. Always do your own research before making any investment decisions.

