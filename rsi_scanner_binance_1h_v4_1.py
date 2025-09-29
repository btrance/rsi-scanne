#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Spot 1H RSI Scanner (v4.2, resilient+binance host fallback)
- 출력: 기본 USDT만, 시총 내림차순(시총 실패시 24h 거래대금 내림차순 자동 폴백)
- CoinGecko 호출 실패 시 자동 폴백 유지
- **Binance API 다중 호스트 폴백**: api, api1, api2, api3, api-gcp
- KST/UTC 표기, 천단위 콤마, 법정/스테이블 베이스 제외(옵션으로 허용 가능)
"""
import os, sys, csv, time, asyncio, random
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone, timedelta
import aiohttp

# ── Binance & CoinGecko ────────────────────────────────────────────────────────
BINANCE_API_HOSTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api-gcp.binance.com",
]
COINGECKO_API = "https://api.coingecko.com/api/v3"

DEFAULT_HEADERS = {
    "User-Agent": "RSI-Scanner/4.2 (+binance-spot-1h-rsi)"
}

# ── 기본 파라미터 ─────────────────────────────────────────────────────────────
BLACKLIST_SUBSTRINGS = ["UP", "DOWN", "BULL", "BEAR"]
DEFAULT_QUOTES = ["USDT"]
DEFAULT_OB = 70.0
DEFAULT_OS = 30.0
DEFAULT_PERIOD = 14
DEFAULT_LIMIT = 200
DEFAULT_CONCURRENCY = 10
DEFAULT_MIN_VOLUME_USDT = 0.0

FIAT_BASES = {
    "EUR","USD","GBP","TRY","BRL","ZAR","RUB","AUD","CAD","CHF","JPY","KRW",
    "UAH","VND","IDR","NGN","TWD","MXN","PLN","SAR","AED","SEK","NOK","DKK","HKD","CNY","THB"
}
STABLE_BASES = {"USDT","USDC","BUSD","FDUSD","TUSD","USDD","DAI","USD1","USDP","UST","USTC"}

STATIC_CG_MAP = {
    "BTC":"bitcoin","ETH":"ethereum","BNB":"binancecoin","XRP":"ripple","ADA":"cardano","SOL":"solana",
    "DOGE":"dogecoin","TRX":"tron","DOT":"polkadot","MATIC":"matic-network","AVAX":"avalanche-2",
    "LINK":"chainlink","LTC":"litecoin","BCH":"bitcoin-cash","NEAR":"near","ATOM":"cosmos",
    "UNI":"uniswap","ETC":"ethereum-classic","XLM":"stellar","AAVE":"aave","CRV":"curve-dao-token",
    "LDO":"lido-dao","SUI":"sui","APT":"aptos","ARB":"arbitrum","OP":"optimism",
}

# ── 데이터 구조 ────────────────────────────────────────────────────────────────
@dataclass
class SymbolInfo:
    symbol: str
    base: str
    quote: str

@dataclass
class ScanResult:
    symbol: str
    base: str
    quote: str
    rsi: float
    close: float
    change_24h_pct: float
    quote_volume_24h: float
    time_kst: str
    time_utc: str

# ── CLI 파서 ───────────────────────────────────────────────────────────────────
def parse_args(argv: List[str]) -> Dict:
    args = {
        "quotes": DEFAULT_QUOTES,
        "overbought": DEFAULT_OB,
        "oversold": DEFAULT_OS,
        "period": DEFAULT_PERIOD,
        "limit": DEFAULT_LIMIT,
        "concurrency": DEFAULT_CONCURRENCY,
        "min_volume": DEFAULT_MIN_VOLUME_USDT,
        "timeout": 20,
        "save_csv": True,
        "allow_fiat": False,
        "only_usdt": True,            # 출력 필터: 기본은 USDT만
        "sort_key": "marketcap_desc", # marketcap_desc | rsi | volume
        "show_n": 0,                  # 0 = 제한 없음
        "skip_mcap": False,
    }
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--quotes" and i+1 < len(argv):
            args["quotes"] = [q.strip().upper() for q in argv[i+1].split(",") if q.strip()]; i += 2; continue
        if a == "--overbought" and i+1 < len(argv):
            args["overbought"] = float(argv[i+1]); i += 2; continue
        if a == "--oversold" and i+1 < len(argv):
            args["oversold"] = float(argv[i+1]); i += 2; continue
        if a == "--period" and i+1 < len(argv):
            args["period"] = int(argv[i+1]); i += 2; continue
        if a == "--limit" and i+1 < len(argv):
            args["limit"] = int(argv[i+1]); i += 2; continue
        if a == "--concurrency" and i+1 < len(argv):
            args["concurrency"] = int(argv[i+1]); i += 2; continue
        if a == "--min-volume" and i+1 < len(argv):
            args["min_volume"] = float(argv[i+1]); i += 2; continue
        if a == "--no-csv":
            args["save_csv"] = False; i += 1; continue
        if a == "--timeout" and i+1 < len(argv):
            args["timeout"] = int(argv[i+1]); i += 2; continue
        if a == "--allow-fiat":
            args["allow_fiat"] = True; i += 1; continue
        if a == "--no-only-usdt":
            args["only_usdt"] = False; i += 1; continue
        if a == "--sort" and i+1 < len(argv):
            args["sort_key"] = argv[i+1].strip().lower(); i += 2; continue
        if a == "--show-n" and i+1 < len(argv):
            args["show_n"] = int(argv[i+1]); i += 2; continue
        if a == "--skip-mcap":
            args["skip_mcap"] = True; i += 1; continue
        i += 1
    return args

# ── 유틸 ──────────────────────────────────────────────────────────────────────
def is_blacklisted(symbol: str) -> bool:
    return any(k in symbol for k in BLACKLIST_SUBSTRINGS)

def should_exclude_base(base: str, allow_fiat: bool) -> bool:
    if allow_fiat: return False
    if base in FIAT_BASES: return True
    if base in STABLE_BASES: return True
    return False

def wilder_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1: return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

async def fetch_json(session: aiohttp.ClientSession, url: str, params: Dict = None, timeout: int = 20):
    for attempt in range(8):
        try:
            async with session.get(url, params=params, timeout=timeout) as resp:
                if resp.status in (418, 429, 403, 451, 503):
                    backoff = min(10.0, 1.5 * (attempt + 1)) + random.uniform(0, 0.7)
                    await asyncio.sleep(backoff); continue
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            backoff = min(6.0, 1.2 * (attempt + 1)) + random.uniform(0, 0.5)
            await asyncio.sleep(backoff)
    raise RuntimeError(f"Failed to fetch {url} after retries")

def make_session(connector):
    return aiohttp.ClientSession(connector=connector, headers=DEFAULT_HEADERS)

def _u(host: str, path: str) -> str:
    return f"{host}{path}"

async def fetch_any(session: aiohttp.ClientSession, path: str, params: Dict = None, timeout: int = 20):
    """여러 Binance 호스트를 돌며 첫 성공 응답 반환"""
    last_err = None
    for host in BINANCE_API_HOSTS:
        try:
            return await fetch_json(session, _u(host, path), params=params, timeout=timeout)
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.7)
    if last_err:
        raise last_err
    raise RuntimeError("No Binance hosts tried")

# ── Binance 호출들(폴백 사용) ─────────────────────────────────────────────────
async def get_spot_symbols(session: aiohttp.ClientSession, quotes: List[str], allow_fiat: bool, timeout: int) -> List[SymbolInfo]:
    exinfo = await fetch_any(session, "/api/v3/exchangeInfo", timeout=timeout)
    out: List[SymbolInfo] = []
    for s in exinfo.get("symbols", []):
        if s.get("status", "TRADING") != "TRADING": continue
        base = s.get("baseAsset"); quote = s.get("quoteAsset"); symbol = s.get("symbol")
        if not base or not quote or not symbol: continue
        if quote not in quotes: continue
        if is_blacklisted(symbol): continue
        if should_exclude_base(base, allow_fiat): continue
        permissions = s.get("permissions"); is_spot = s.get("isSpotTradingAllowed", None)
        allowed = isinstance(is_spot, bool) and is_spot
        if not allowed and isinstance(permissions, list): allowed = ("SPOT" in permissions)
        if not allowed and permissions is None and is_spot is None: allowed = True
        if allowed: out.append(SymbolInfo(symbol=symbol, base=base, quote=quote))
    return out

async def get_24h_ticker_map(session: aiohttp.ClientSession, timeout: int) -> Dict[str, Dict]:
    data = await fetch_any(session, "/api/v3/ticker/24hr", timeout=timeout)
    return {d["symbol"]: d for d in data}

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, limit: int, timeout: int) -> List[List]:
    params = {"symbol": symbol, "interval": "1h", "limit": limit}
    return await fetch_any(session, "/api/v3/klines", params=params, timeout=timeout)

# ── CoinGecko(시총) ───────────────────────────────────────────────────────────
async def map_cg_ids(session: aiohttp.ClientSession, bases: List[str], timeout: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for base in bases:
        if base in STATIC_CG_MAP:
            out[base] = STATIC_CG_MAP[base]; continue
        try:
            data = await fetch_json(session, f"{COINGECKO_API}/search", params={"query": base}, timeout=timeout)
            candidates = data.get("coins", [])
            exact = [c for c in candidates if c.get("symbol","").lower() == base.lower()]
            def rank_val(x): r = x.get("market_cap_rank"); return 10**9 if (r is None) else int(r)
            pick = (sorted(exact, key=rank_val)[0] if exact else (sorted(candidates, key=rank_val)[0] if candidates else None))
            if pick and pick.get("id"): out[base] = pick["id"]
        except Exception: pass
    return out

async def fetch_market_caps(session: aiohttp.ClientSession, cg_ids: List[str], timeout: int) -> Dict[str, Tuple[float,int]]:
    out: Dict[str, Tuple[float,int]] = {}
    if not cg_ids: return out
    ids_param = ",".join(cg_ids[:250])
    try:
        data = await fetch_json(session, f"{COINGECKO_API}/coins/markets", params={"vs_currency":"usd","ids":ids_param}, timeout=timeout)
        for d in data:
            mc = d.get("market_cap") or 0.0
            r = d.get("market_cap_rank") or 10**9
            out[d["id"]] = (float(mc), int(r))
    except Exception:
        return {}
    return out

# ── 심볼 스캔 ─────────────────────────────────────────────────────────────────
async def scan_symbol(session: aiohttp.ClientSession, si: SymbolInfo, limit: int, period: int, ob: float, os_: float, tickers: Dict[str, Dict], min_volume: float, timeout: int, sem: asyncio.Semaphore) -> Optional[ScanResult]:
    async with sem:
        t = tickers.get(si.symbol)
        if t is None: return None
        try:
            quote_vol = float(t.get("quoteVolume", "0"))
            price_change_pct = float(t.get("priceChangePercent", "0"))
        except Exception:
            quote_vol = 0.0; price_change_pct = 0.0
        if quote_vol < min_volume: return None
        kl = await fetch_klines(session, si.symbol, limit, timeout)
        closes = [float(k[4]) for k in kl]
        rsi = wilder_rsi(closes, period=period)
        if rsi is None: return None
        last_close = closes[-1]
        now_utc = datetime.now(timezone.utc)
        kst = now_utc.astimezone(timezone(timedelta(hours=9)))
        if rsi >= ob or rsi <= os_:
            return ScanResult(
                symbol=si.symbol, base=si.base, quote=si.quote,
                rsi=round(rsi, 2), close=last_close,
                change_24h_pct=round(price_change_pct, 2),
                quote_volume_24h=round(quote_vol, 2),
                time_kst=kst.strftime("%Y-%m-%d %H:%M:%S"),
                time_utc=now_utc.strftime("%Y-%m-%d %H:%M:%S"),
            )
        return None

async def run_scan(quotes: List[str], overbought: float, oversold: float, period: int, limit: int, concurrency: int, min_volume: float, allow_fiat: bool, skip_mcap: bool, timeout: int) -> Tuple[List[ScanResult], List[SymbolInfo]]:
    connector = aiohttp.TCPConnector(limit=concurrency*2, ssl=False)
    async with make_session(connector) as session:
        symbols = await get_spot_symbols(session, quotes, allow_fiat, timeout=timeout)
        tickers = await get_24h_ticker_map(session, timeout=timeout)
        sem = asyncio.Semaphore(concurrency)
        results = await asyncio.gather(*[scan_symbol(session, si, limit, period, overbought, oversold, tickers, min_volume, timeout, sem) for si in symbols], return_exceptions=True)
        out: List[ScanResult] = []
        for r in results:
            if isinstance(r, Exception): continue
            if r is not None: out.append(r)
        # 시총 보강
        if not skip_mcap and out:
            bases = sorted({r.base for r in out})
            cg_map = await map_cg_ids(session, bases, timeout=timeout)
            mc_raw = await fetch_market_caps(session, list(cg_map.values()), timeout=timeout)
            base_mc: Dict[str, Tuple[float,int]] = {b: mc_raw.get(cid, (0.0, 10**9)) for b, cid in cg_map.items()}
            for r in out:
                r._market_cap = base_mc.get(r.base, (0.0, 10**9))[0]  # type: ignore
                r._mc_rank   = base_mc.get(r.base, (0.0, 10**9))[1]  # type: ignore
        else:
            for r in out:
                r._market_cap = 0.0  # type: ignore
                r._mc_rank   = 10**9 # type: ignore
        return out, symbols

# ── 출력/정렬/알림 ────────────────────────────────────────────────────────────
def apply_output_filters(results: List[ScanResult], only_usdt: bool) -> List[ScanResult]:
    return [r for r in results if (r.quote == "USDT")] if only_usdt else results

def sort_results(results: List[ScanResult], sort_key: str) -> List[ScanResult]:
    if sort_key == "rsi": return sorted(results, key=lambda r: r.rsi, reverse=True)
    if sort_key == "volume": return sorted(results, key=lambda r: r.quote_volume_24h, reverse=True)
    if any(getattr(r, "_market_cap", 0.0) > 0 for r in results):
        return sorted(results, key=lambda r: getattr(r, "_market_cap", 0.0), reverse=True)
    return sorted(results, key=lambda r: r.quote_volume_24h, reverse=True)

def print_table(results: List[ScanResult], ob: float, os_: float, used_sort: str):
    ob_list = [r for r in results if r.rsi >= ob]
    os_list = [r for r in results if r.rsi <= os_]
    def _section(title: str, rows: List[ScanResult]):
        print(f"\n{title} (정렬: {used_sort})")
        print("-" * 130)
        print(f"{'SYMBOL':<16}{'RSI':>8}{'CLOSE':>16}{'24H%':>10}{'QVOL(24h)':>18}{'MKT CAP ($)':>18}  {'KST':>19}  {'UTC':>19}")
        for r in rows:
            mc = getattr(r, "_market_cap", 0.0)
            print(f"{r.symbol:<16}{r.rsi:>8.2f}{r.close:>16,.6f}{r.change_24h_pct:>10.2f}{r.quote_volume_24h:>18,.0f}{mc:>18,.0f}  {r.time_kst:>19}  {r.time_utc:>19}")
    print("" if ob_list or os_list else "\n신호 없음")
    if ob_list: _section("과매수 (USDT만 표시)", ob_list)
    else: print("\n과매수 없음")
    if os_list: _section("과매도 (USDT만 표시)", os_list)
    else: print("\n과매도 없음")

def save_csv(results: List[ScanResult]) -> str:
    if not results: return ""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = f"rsi_scanner_1h_{ts}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","base","quote","rsi","close","change_24h_pct","quote_volume_24h","market_cap","time_kst","time_utc"])
        for r in results:
            mc = getattr(r, "_market_cap", 0.0)
            w.writerow([r.symbol,r.base,r.quote,r.rsi,r.close,r.change_24h_pct,r.quote_volume_24h,mc,r.time_kst,r.time_utc])
    return path

async def maybe_send_telegram(results: List[ScanResult], ob: float, os_: float, used_sort: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN"); chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id: return
    kst = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")
    ob_hits = sort_results([r for r in results if r.rsi >= ob], used_sort)[:10]
    os_hits = sort_results([r for r in results if r.rsi <= os_], used_sort)[:10]
    if not ob_hits and not os_hits: return
    lines = [f"[RSI 1H 스캐너] {kst} KST (USDT & {used_sort})"]
    if ob_hits: lines.append("과매수: " + ", ".join(f"{r.symbol}({r.rsi:.1f})" for r in ob_hits))
    if os_hits: lines.append("과매도: " + ", ".join(f"{r.symbol}({r.rsi:.1f})" for r in os_hits))
    text = "\n".join(lines)
    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
        try:
            await session.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": text})
        except Exception:
            pass

# ── 엔트리포인트 ─────────────────────────────────────────────────────────────
async def async_main():
    args = parse_args(sys.argv[1:])
    start = time.time()
    results, symbols = await run_scan(
        quotes=args["quotes"], overbought=args["overbought"], oversold=args["oversold"],
        period=args["period"], limit=args["limit"], concurrency=args["concurrency"],
        min_volume=args["min_volume"], allow_fiat=args["allow_fiat"],
        skip_mcap=args["skip_mcap"], timeout=args["timeout"],
    )
    filtered = apply_output_filters(results, args["only_usdt"])
    sorted_list = sort_results(filtered, args["sort_key"])
    used_sort = args["sort_key"]
    if used_sort == "marketcap_desc" and not any(getattr(r, "_market_cap", 0.0) > 0 for r in sorted_list):
        used_sort = "volume"
    if args["show_n"] and args["show_n"] > 0:
        sorted_list = sorted_list[:args["show_n"]]
    print(f"[완료] 스캔 심볼 수: {len(symbols)} | 히트 수: {len(results)} | 표시 수: {len(sorted_list)} | 경과: {time.time()-start:.1f}s")
    print(f"필터: QUOTES={args['quotes']} PERIOD={args['period']} OB={args['overbought']} OS={args['oversold']} MIN_VOL={args['min_volume']} ONLY_USDT={args['only_usdt']} SORT={used_sort} SKIP_MCAP={args['skip_mcap']}")
    print_table(sorted_list, args["overbought"], args["oversold"], used_sort)
    if args["save_csv"]:
        path = save_csv(sorted_list)
        if path: print(f"\nCSV 저장: {path}")
    await maybe_send_telegram(sorted_list, args["overbought"], args["oversold"], used_sort)

def main(): asyncio.run(async_main())
if __name__ == "__main__": main()
