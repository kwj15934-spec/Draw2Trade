"""
US 주식 데이터 서비스 (yfinance 기반)

캐시 구조:
  cache/us/tickers.json        — S&P 500 + NDX100 + ETF 목록 (일 1회 갱신)
  cache/us/ohlcv/{symbol}.json — 일봉 OHLCV (당일 last_date 기준 캐시)

티커 수집:
  1. Wikipedia에서 S&P 500 목록 로드 (503개)
  2. NASDAQ 100 supplement (하드코딩)
  3. 인기 ETF 하드코딩
  실패 시 하드코딩 fallback 100개 사용.
"""
import json
import logging
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── 경로 ──────────────────────────────────────────────────────────────────────
_BASE_DIR = Path(__file__).resolve().parent.parent.parent   # draw2trade_web/
_US_CACHE_DIR = _BASE_DIR / "cache" / "us" / "ohlcv"
_US_TICKERS_FILE = _BASE_DIR / "cache" / "us" / "tickers.json"

# ── 메모리 캐시 (lazy) ────────────────────────────────────────────────────────
_mem_us_ohlcv: dict[str, dict] = {}
_mem_us_names: dict[str, str] = {}
_ticker_list_cache: list[dict] = []

# ── NASDAQ 주요 종목 보충 (S&P 500에 없는 종목 포함) ──────────────────────────
_NDX_SUPPLEMENT: list[tuple[str, str]] = [
    # NASDAQ-100 구성 종목
    ("ADBE", "Adobe"), ("ADP", "Automatic Data Processing"), ("ADSK", "Autodesk"),
    ("AEP", "American Electric Power"), ("ALGN", "Align Technology"),
    ("AMAT", "Applied Materials"), ("AMD", "Advanced Micro Devices"),
    ("AMGN", "Amgen"), ("ANSS", "Ansys"), ("ASML", "ASML Holding"),
    ("BKNG", "Booking Holdings"), ("BIIB", "Biogen"),
    ("CDNS", "Cadence Design Systems"), ("CEG", "Constellation Energy"),
    ("CMCSA", "Comcast"), ("COST", "Costco Wholesale"), ("CPRT", "Copart"),
    ("CRWD", "CrowdStrike"), ("CSCO", "Cisco Systems"), ("CTAS", "Cintas"),
    ("DDOG", "Datadog"), ("DLTR", "Dollar Tree"), ("DXCM", "DexCom"),
    ("EA", "Electronic Arts"), ("EXC", "Exelon"), ("FAST", "Fastenal"),
    ("FTNT", "Fortinet"), ("GILD", "Gilead Sciences"), ("HUBS", "HubSpot"),
    ("IDXX", "IDEXX Laboratories"), ("ILMN", "Illumina"), ("INTC", "Intel"),
    ("INTU", "Intuit"), ("ISRG", "Intuitive Surgical"), ("KDP", "Keurig Dr Pepper"),
    ("KHC", "Kraft Heinz"), ("LRCX", "Lam Research"), ("MCHP", "Microchip Technology"),
    ("MDLZ", "Mondelez International"), ("MNST", "Monster Beverage"),
    ("MRNA", "Moderna"), ("MRVL", "Marvell Technology"), ("MSFT", "Microsoft"),
    ("MU", "Micron Technology"), ("NFLX", "Netflix"), ("NVDA", "NVIDIA"),
    ("NXPI", "NXP Semiconductors"), ("ODFL", "Old Dominion Freight"),
    ("ON", "ON Semiconductor"), ("ORLY", "O'Reilly Automotive"), ("PCAR", "PACCAR"),
    ("PAYX", "Paychex"), ("PYPL", "PayPal"), ("QCOM", "Qualcomm"),
    ("REGN", "Regeneron Pharmaceuticals"), ("ROP", "Roper Technologies"),
    ("ROST", "Ross Stores"), ("SBUX", "Starbucks"), ("SNPS", "Synopsys"),
    ("TEAM", "Atlassian"), ("TMUS", "T-Mobile US"), ("TTWO", "Take-Two Interactive"),
    ("TXN", "Texas Instruments"), ("VRSK", "Verisk Analytics"),
    ("VRTX", "Vertex Pharmaceuticals"), ("WBA", "Walgreens Boots Alliance"),
    ("WBD", "Warner Bros. Discovery"), ("WDAY", "Workday"), ("XEL", "Xcel Energy"),
    ("ZM", "Zoom Video"), ("ZS", "Zscaler"),
    # 주요 NASDAQ 테크
    ("ABNB", "Airbnb"), ("ACLS", "Axcelis Technologies"), ("ACMR", "ACM Research"),
    ("AEHR", "Aehr Test Systems"), ("AFRM", "Affirm Holdings"), ("AGX", "Argan"),
    ("AI", "C3.ai"), ("AIOT", "Sievert Larsen & Associates"), ("AKAM", "Akamai"),
    ("ALTR", "Altair Engineering"), ("ALVO", "Alvotech"), ("APP", "Applovin"),
    ("APPN", "Appian"), ("APPS", "Digital Turbine"), ("ARWR", "Arrowhead Pharma"),
    ("ASGN", "ASGN"), ("ASND", "Ascendis Pharma"),
    ("ASTS", "AST SpaceMobile"), ("ATVI", "Activision Blizzard"),
    ("AXON", "Axon Enterprise"), ("AZPN", "Aspen Technology"),
    ("BILL", "Bill.com"), ("BLKB", "Blackbaud"), ("BMRN", "BioMarin"),
    ("BNGO", "Bionano Genomics"), ("BRDG", "Bridge Investment"),
    ("BROS", "Dutch Bros"), ("BTBT", "Bit Digital"),
    ("CAMT", "Camtek"), ("CARG", "CarGurus"), ("CASY", "Casey's General Stores"),
    ("CEVA", "CEVA"), ("CFLT", "Confluent"), ("CHKP", "Check Point Software"),
    ("CHWY", "Chewy"), ("CLFD", "Clearfield"), ("CLNE", "Clean Energy Fuels"),
    ("CLOV", "Clover Health"), ("CNXC", "Concentrix"), ("COIN", "Coinbase"),
    ("COHU", "Cohu"), ("COLM", "Columbia Sportswear"), ("COUR", "Coursera"),
    ("CRNX", "Crinetics Pharma"), ("CROX", "Crocs"), ("CSGP", "CoStar Group"),
    ("CSWI", "CSW Industrials"), ("CTSH", "Cognizant"), ("CVNA", "Carvana"),
    ("DASH", "DoorDash"), ("DBX", "Dropbox"), ("DKNG", "DraftKings"),
    ("DOCN", "DigitalOcean"), ("DOCS", "Doximity"), ("DOCU", "DocuSign"),
    ("DOMO", "Domo"), ("DRVN", "Driven Brands"), ("DUOL", "Duolingo"),
    ("EBAY", "eBay"), ("EGHT", "8x8"), ("ENOVIS", "Enovis"),
    ("ENVX", "Enovix"), ("ENPH", "Enphase Energy"), ("ENTG", "Entegris"),
    ("EPAM", "EPAM Systems"), ("EQIX", "Equinix"), ("ESTC", "Elastic"),
    ("ETSY", "Etsy"), ("EXAS", "Exact Sciences"),
    ("FANG", "Diamondback Energy"), ("FARO", "FARO Technologies"),
    ("FFIV", "F5 Networks"), ("FIVE", "Five Below"), ("FIVN", "Five9"),
    ("FIVERR", "Fiverr"), ("FLNC", "Fluence Energy"), ("FROG", "JFrog"),
    ("FSLR", "First Solar"), ("FTDR", "Frontdoor"), ("FUTU", "Futu Holdings"),
    ("GBTG", "Global Business Travel"), ("GDDY", "GoDaddy"),
    ("GERN", "Geron"), ("GLBE", "Global-E Online"), ("GLPG", "Galapagos"),
    ("GMAB", "Genmab"), ("GRAB", "Grab Holdings"), ("GTLB", "GitLab"),
    ("GH", "Guardant Health"), ("HALO", "Halozyme"),
    ("HCP", "HashiCorp"), ("HELE", "Helen of Troy"), ("HIBB", "Hibbett"),
    ("HIMS", "Hims & Hers Health"), ("HOLX", "Hologic"), ("HOOD", "Robinhood"),
    ("HUBS", "HubSpot"), ("IAC", "IAC"), ("ICLR", "ICON"),
    ("IMVT", "Immunovant"), ("INCY", "Incyte"), ("INFN", "Infinera"),
    ("INMD", "InMode"), ("INSP", "Inspire Medical"), ("IONQ", "IonQ"),
    ("IONS", "Ionis Pharma"), ("IOSP", "Innospec"),
    ("IPGP", "IPG Photonics"), ("IRTC", "iRhythm"), ("ITRI", "Itron"),
    ("JAZZ", "Jazz Pharma"), ("JOBY", "Joby Aviation"),
    ("KIDS", "OrthoPediatrics"), ("KIND", "Nextdoor"),
    ("KRTX", "Karuna Therapeutics"), ("KRUS", "Kura Sushi"),
    ("LNTH", "Lantheus"), ("LPLA", "LPL Financial"), ("LSCC", "Lattice Semi"),
    ("LUNR", "Intuitive Machines"), ("LYFT", "Lyft"),
    ("MASI", "Masimo"), ("MANH", "Manhattan Associates"),
    ("MARA", "Marathon Digital"), ("MBLY", "Mobileye"),
    ("MCOM", "Microcom"), ("MDB", "MongoDB"), ("META", "Meta Platforms"),
    ("MKTX", "MarketAxess"), ("MMSI", "Merit Medical"), ("MNDY", "Monday.com"),
    ("MPWR", "Monolithic Power"), ("MSTR", "MicroStrategy"),
    ("NDAQ", "Nasdaq"), ("NBIX", "Neurocrine Bio"), ("NCNO", "nCino"),
    ("NET", "Cloudflare"), ("NTAP", "NetApp"), ("NTLA", "Intellia"),
    ("NTRA", "Natera"), ("NTNX", "Nutanix"), ("NVCR", "NovaCure"),
    ("NVEI", "Nuvei"), ("NWSA", "News Corp A"), ("NWS", "News Corp B"),
    ("OBDC", "Blue Owl Capital"), ("OKTA", "Okta"), ("OMCL", "Omnicell"),
    ("OPK", "OPKO Health"), ("PACB", "Pacific Biosciences"),
    ("PAGS", "PagSeguro"), ("PANW", "Palo Alto Networks"),
    ("PATH", "UiPath"), ("PAYO", "Payoneer"), ("PCVX", "Vaxcyte"),
    ("PDCO", "Patterson Companies"), ("PFSI", "PennyMac Financial"),
    ("PINS", "Pinterest"), ("PLTR", "Palantir"), ("PLUG", "Plug Power"),
    ("PNFP", "Pinnacle Financial"), ("PODD", "Insulet"), ("POOL", "Pool Corp"),
    ("PTCT", "PTC Therapeutics"), ("PTON", "Peloton"),
    ("RBLX", "Roblox"), ("RCUS", "Arcus Bio"), ("RDDT", "Reddit"),
    ("RIOT", "Riot Platforms"), ("RIVN", "Rivian Automotive"),
    ("ROKU", "Roku"), ("RPAY", "Repay Holdings"), ("RXRX", "Recursion Pharma"),
    ("S", "SentinelOne"), ("SAIL", "SailPoint"), ("SEDG", "SolarEdge"),
    ("SHOP", "Shopify"), ("SIRI", "Sirius XM"), ("SMAR", "Smartsheet"),
    ("SMG", "Scotts Miracle-Gro"), ("SMTC", "Semtech"), ("SNAP", "Snap"),
    ("SNOW", "Snowflake"), ("SOFI", "SoFi Technologies"), ("SONO", "Sonos"),
    ("SPCE", "Virgin Galactic"), ("SPOT", "Spotify"),
    ("SRPT", "Sarepta Therapeutics"), ("SWAV", "ShockWave Medical"),
    ("SWTX", "SpringWorks"), ("SYNA", "Synaptics"),
    ("TASK", "TaskUs"), ("TDOC", "Teladoc"), ("TENB", "Tenable"),
    ("TGTX", "TG Therapeutics"), ("TIGR", "UP Fintech"),
    ("TKNO", "Alpha Teknova"), ("TMDX", "TransMedics"),
    ("TPVG", "TriplePoint Venture"), ("TRMB", "Trimble"),
    ("TSLA", "Tesla"), ("TTD", "The Trade Desk"), ("TWLO", "Twilio"),
    ("TXG", "10x Genomics"), ("TZOO", "Travelzoo"),
    ("U", "Unity Software"), ("UBER", "Uber Technologies"),
    ("UPST", "Upstart Holdings"), ("UTHR", "United Therapeutics"),
    ("VCEL", "Vericel"), ("VNET", "VNET Group"),
    ("VRNS", "Varonis Systems"), ("VTRS", "Viatris"),
    ("WBEV", "Winc"), ("WIX", "Wix.com"), ("WOLF", "Wolfspeed"),
    ("XRAY", "Dentsply Sirona"), ("XPEV", "XPeng"),
    ("YEXT", "Yext"), ("YMM", "Full Truck Alliance"),
    ("Z", "Zillow Group C"), ("ZG", "Zillow Group A"),
    ("ZI", "ZoomInfo"), ("ZNGA", "Zynga"),
]

# ── 인기 ETF ──────────────────────────────────────────────────────────────────
_ETFS: list[tuple[str, str]] = [
    ("SPY",  "SPDR S&P 500 ETF"),
    ("QQQ",  "Invesco NASDAQ 100 ETF"),
    ("IWM",  "iShares Russell 2000 ETF"),
    ("DIA",  "SPDR Dow Jones ETF"),
    ("VTI",  "Vanguard Total Market ETF"),
    ("VOO",  "Vanguard S&P 500 ETF"),
    ("IVV",  "iShares S&P 500 ETF"),
    ("GLD",  "SPDR Gold Shares"),
    ("SLV",  "iShares Silver Trust"),
    ("TLT",  "iShares 20Y Treasury ETF"),
    ("IEF",  "iShares 7-10Y Treasury ETF"),
    ("HYG",  "iShares High Yield Bond ETF"),
    ("LQD",  "iShares Investment Grade Bond ETF"),
    ("XLF",  "Financial Select Sector ETF"),
    ("XLE",  "Energy Select Sector ETF"),
    ("XLK",  "Technology Select Sector ETF"),
    ("XLV",  "Health Care Select Sector ETF"),
    ("XLI",  "Industrial Select Sector ETF"),
    ("XLY",  "Consumer Discretionary ETF"),
    ("XLP",  "Consumer Staples ETF"),
    ("XLB",  "Materials Select Sector ETF"),
    ("XLU",  "Utilities Select Sector ETF"),
    ("XLRE", "Real Estate ETF"),
    ("XLC",  "Communication Services ETF"),
    ("ARKK", "ARK Innovation ETF"),
    ("ARKG", "ARK Genomic Revolution ETF"),
    ("ARKW", "ARK Next Generation Internet ETF"),
    ("SMH",  "VanEck Semiconductor ETF"),
    ("SOXX", "iShares Semiconductor ETF"),
    ("IBB",  "iShares Biotech ETF"),
    ("VNQ",  "Vanguard Real Estate ETF"),
    ("EEM",  "iShares MSCI Emerging Markets ETF"),
    ("EFA",  "iShares MSCI EAFE ETF"),
    ("VWO",  "Vanguard FTSE Emerging Markets ETF"),
    ("UNG",  "United States Natural Gas ETF"),
    ("USO",  "United States Oil ETF"),
]

# ── Fallback 하드코딩 (Wikipedia 로드 실패 시) ────────────────────────────────
_FALLBACK_TICKERS: list[tuple[str, str]] = [
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"),
    ("GOOGL", "Alphabet A"), ("GOOG", "Alphabet C"), ("AMZN", "Amazon"),
    ("META", "Meta Platforms"), ("TSLA", "Tesla"), ("BRK-B", "Berkshire Hathaway B"),
    ("JPM", "JPMorgan Chase"), ("V", "Visa"), ("UNH", "UnitedHealth"),
    ("XOM", "ExxonMobil"), ("JNJ", "Johnson & Johnson"), ("WMT", "Walmart"),
    ("MA", "Mastercard"), ("PG", "Procter & Gamble"), ("AVGO", "Broadcom"),
    ("HD", "Home Depot"), ("CVX", "Chevron"), ("LLY", "Eli Lilly"),
    ("MRK", "Merck"), ("ABBV", "AbbVie"), ("COST", "Costco"),
    ("KO", "Coca-Cola"), ("PEP", "PepsiCo"), ("BAC", "Bank of America"),
    ("TMO", "Thermo Fisher Scientific"), ("MCD", "McDonald's"),
    ("CSCO", "Cisco Systems"), ("CRM", "Salesforce"), ("ABT", "Abbott"),
    ("ACN", "Accenture"), ("ADBE", "Adobe"), ("AMD", "Advanced Micro Devices"),
    ("AMGN", "Amgen"), ("AXP", "American Express"), ("BA", "Boeing"),
    ("BMY", "Bristol-Myers Squibb"), ("CAT", "Caterpillar"),
    ("CMCSA", "Comcast"), ("COP", "ConocoPhillips"), ("DHR", "Danaher"),
    ("DIS", "Disney"), ("DOW", "Dow"), ("DUK", "Duke Energy"),
    ("EMR", "Emerson Electric"), ("F", "Ford"), ("FDX", "FedEx"),
    ("GE", "GE Aerospace"), ("GS", "Goldman Sachs"), ("HON", "Honeywell"),
    ("IBM", "IBM"), ("INTC", "Intel"), ("INTU", "Intuit"),
    ("ISRG", "Intuitive Surgical"), ("ITW", "Illinois Tool Works"),
    ("KHC", "Kraft Heinz"), ("LIN", "Linde"), ("LOW", "Lowe's"),
    ("MDLZ", "Mondelez"), ("MMM", "3M"), ("MO", "Altria"),
    ("MS", "Morgan Stanley"), ("NFLX", "Netflix"), ("NEE", "NextEra Energy"),
    ("NKE", "Nike"), ("NOW", "ServiceNow"), ("ORCL", "Oracle"),
    ("PFE", "Pfizer"), ("PM", "Philip Morris"), ("PYPL", "PayPal"),
    ("QCOM", "Qualcomm"), ("RTX", "RTX Corp"), ("SBUX", "Starbucks"),
    ("SCHW", "Charles Schwab"), ("SLB", "SLB"), ("SO", "Southern Company"),
    ("SPG", "Simon Property Group"), ("T", "AT&T"), ("TGT", "Target"),
    ("TJX", "TJX Companies"), ("TMUS", "T-Mobile US"), ("TXN", "Texas Instruments"),
    ("UNP", "Union Pacific"), ("UPS", "UPS"), ("USB", "US Bancorp"),
    ("VZ", "Verizon"), ("WFC", "Wells Fargo"), ("WM", "Waste Management"),
]


# ─────────────────────────────────────────────────────────────────────────────
# 초기화
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_dirs() -> None:
    _US_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _US_TICKERS_FILE.parent.mkdir(parents=True, exist_ok=True)


def _fetch_nasdaq_ftp() -> list[tuple[str, str, str]]:
    """
    NASDAQ trader FTP에서 NASDAQ + NYSE/AMEX 전체 상장 종목 로드.
    인증 없이 접근 가능한 공식 공개 파일 사용.

    nasdaqlisted.txt  — NASDAQ 상장 (~4000개)
    otherlisted.txt   — NYSE / AMEX 등 (~8000개)
    """
    import urllib.request as _req

    urls = [
        "https://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
        "https://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt",
    ]
    seen: set[str] = set()
    result: list[tuple[str, str, str]] = []

    for url in urls:
        try:
            req = _req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with _req.urlopen(req, timeout=15) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            lines = text.strip().split("\n")
            # 첫 줄은 헤더, 마지막 줄은 "File Creation Time=..." 메타라인
            for line in lines[1:]:
                parts = line.strip().split("|")
                if len(parts) < 2:
                    continue
                sym  = parts[0].strip().replace(".", "-")
                name = parts[1].strip()
                if not sym or not name:
                    continue
                # 테스트 이슈 제외 (nasdaqlisted: col3, otherlisted: col6)
                test_col = 3 if "nasdaqlisted" in url else 6
                if len(parts) > test_col and parts[test_col].strip().upper() == "Y":
                    continue
                # 메타라인 / 특수 심볼 제외
                if sym.startswith("File") or "/" in sym or "^" in sym or len(sym) > 6:
                    continue
                if sym not in seen:
                    seen.add(sym)
                    result.append((sym, name, ""))
        except Exception as e:
            logger.warning("NASDAQ FTP 로드 실패 (%s): %s", url, e)

    logger.info("NASDAQ FTP에서 %d개 종목 로드 완료", len(result))
    return result


def _fetch_nasdaq_screener() -> list[tuple[str, str, str]]:
    """
    NASDAQ screener API에서 전체 미국 상장 주식 목록을 로드.
    Returns list of (symbol, name, sector). 실패 시 빈 리스트 반환.
    """
    try:
        import urllib.request as _req
        url = (
            "https://api.nasdaq.com/api/screener/stocks"
            "?tableonly=true&limit=10000&download=true"
        )
        req = _req.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.nasdaq.com",
            "Referer": "https://www.nasdaq.com/",
        })
        with _req.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        rows = (data.get("data") or {}).get("table", {}).get("rows") or []
        result = []
        for row in rows:
            sym = str(row.get("symbol", "") or "").strip().replace(".", "-").replace("^", "")
            name = str(row.get("name", "") or "").strip()
            sector = str(row.get("sector", "") or "").strip()
            if not sym or not name or sym == "Symbol" or "/" in sym or len(sym) > 8:
                continue
            result.append((sym, name, sector))
        logger.info("NASDAQ screener에서 %d개 종목 로드 완료", len(result))
        return result
    except Exception as e:
        logger.warning("NASDAQ screener 로드 실패: %s (다음 소스 시도)", e)
        return []


def _fetch_bundled_nasdaq() -> list[tuple[str, str, str]]:
    """
    리포지토리에 번들된 data/nasdaq_tickers.csv 로드.
    외부 네트워크 불필요 — 서버 방화벽 영향 없음.
    """
    csv_path = _BASE_DIR / "data" / "nasdaq_tickers.csv"
    if not csv_path.exists():
        return []
    try:
        result = []
        seen: set[str] = set()
        with open(csv_path, encoding="utf-8") as f:
            next(f)  # 헤더 스킵
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",", 2)
                if len(parts) < 2:
                    continue
                sym = parts[0].strip().upper()
                name = parts[1].strip()
                sector = parts[2].strip() if len(parts) > 2 else ""
                if sym and name and sym not in seen:
                    seen.add(sym)
                    result.append((sym, name, sector))
        logger.info("번들 nasdaq_tickers.csv에서 %d개 종목 로드", len(result))
        return result
    except Exception as e:
        logger.warning("번들 nasdaq_tickers.csv 로드 실패: %s", e)
        return []


def _fetch_sp500_from_wikipedia() -> list[tuple[str, str, str]]:
    """Wikipedia에서 S&P 500 종목 목록 로드. (symbol, name, gics_sector)"""
    try:
        import io as _io
        import urllib.request as _req

        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        req = _req.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; Draw2Trade/1.0)"
        })
        with _req.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        tables = pd.read_html(_io.StringIO(html), flavor="lxml")
        df = tables[0]
        # 컬럼: Symbol, Security, GICS Sector, GICS Sub-Industry, ...
        sym_col    = df.columns[0]
        name_col   = df.columns[1]
        sector_col = df.columns[2]  # GICS Sector
        result = []
        for _, row in df.iterrows():
            sym    = str(row[sym_col]).strip().replace(".", "-")  # BRK.B → BRK-B
            name   = str(row[name_col]).strip()
            sector = str(row[sector_col]).strip()
            if sym and name:
                result.append((sym, name, sector))
        logger.info("Wikipedia에서 S&P 500 %d개 종목 로드 완료 (섹터 포함)", len(result))
        return result
    except Exception as e:
        logger.warning("Wikipedia S&P 500 로드 실패: %s (fallback 사용)", e)
        return []


def _build_ticker_list() -> list[dict]:
    """
    티커 목록 빌드 (우선순위):
      1. NASDAQ screener API (전체 미국 상장 ~6000개)
      2. Wikipedia S&P 500 (~503개)
      3. 하드코딩 fallback (~84개)
    + NDX supplement + ETF 항상 추가.
    결과를 cache/us/tickers.json에 저장 (일 1회 갱신).
    """
    _ensure_dirs()

    today_str = date.today().isoformat()
    if _US_TICKERS_FILE.exists():
        try:
            cached = json.loads(_US_TICKERS_FILE.read_text(encoding="utf-8"))
            # 날짜 일치 + 종목 수 1000개 이상일 때만 캐시 사용 (S&P 500만 있는 캐시 무시)
            if cached.get("date") == today_str and len(cached.get("tickers", [])) >= 1000:
                logger.info("US 티커 목록 캐시 사용 (%d개)", len(cached["tickers"]))
                return cached["tickers"]
        except Exception:
            pass

    # 1순위: NASDAQ trader FTP (NASDAQ + NYSE/AMEX 전체, ~8000개)
    base_stocks = _fetch_nasdaq_ftp()

    # 2순위: 번들 CSV (서버 방화벽 무관, 항상 작동)
    if len(base_stocks) < 500:
        base_stocks = _fetch_bundled_nasdaq()

    # 3순위: NASDAQ screener API
    if len(base_stocks) < 500:
        base_stocks = _fetch_nasdaq_screener()

    # 4순위: Wikipedia S&P 500
    if len(base_stocks) < 500:
        base_stocks = _fetch_sp500_from_wikipedia()

    # 5순위: 하드코딩 fallback
    if not base_stocks:
        base_stocks = [(s, n, "") for s, n in _FALLBACK_TICKERS]

    # 중복 없이 합치기 (base 우선, NDX supplement/ETF는 없는 경우만 추가)
    seen: set[str] = set()
    combined: list[tuple[str, str, str]] = []
    for sym, name, sector in base_stocks:
        if sym not in seen:
            seen.add(sym)
            combined.append((sym, name, sector))
    # NDX supplement은 항상 추가 (FTP 실패 시에도 주요 NASDAQ 종목 보장)
    for sym, name in _NDX_SUPPLEMENT:
        if sym not in seen:
            seen.add(sym)
            combined.append((sym, name, "Technology"))
    for sym, name in _ETFS:
        if sym not in seen:
            seen.add(sym)
            combined.append((sym, name, "ETF"))

    # 티커 알파벳 순 정렬
    combined.sort(key=lambda x: x[0])
    tickers = [{"ticker": sym, "name": name, "sector": sector} for sym, name, sector in combined]

    try:
        _US_TICKERS_FILE.write_text(
            json.dumps({"date": today_str, "tickers": tickers}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning("US 티커 목록 저장 실패: %s", e)

    logger.info("US 티커 목록 빌드 완료 (%d개)", len(tickers))
    return tickers


def build_us_name_cache() -> None:
    """
    서버 시작 시 호출. 티커 목록 로드 후 _mem_us_names 채움.
    OHLCV는 로드하지 않음 (lazy).
    """
    global _ticker_list_cache
    _ticker_list_cache = _build_ticker_list()
    for item in _ticker_list_cache:
        _mem_us_names[item["ticker"]] = item["name"]
    logger.info("US 이름 캐시 빌드 완료: %d개", len(_mem_us_names))


# ─────────────────────────────────────────────────────────────────────────────
# 티커 목록
# ─────────────────────────────────────────────────────────────────────────────

def get_us_tickers() -> list[dict]:
    """US 티커 목록 반환. 캐시 없으면 빌드."""
    global _ticker_list_cache
    if not _ticker_list_cache:
        build_us_name_cache()
    return _ticker_list_cache


def search_us_tickers(q: str, limit: int = 30) -> list[dict]:
    """US 종목 검색 (티커 또는 회사명 포함)."""
    q = (q or "").strip().lower()
    if not q:
        return []
    results = []
    for item in get_us_tickers():
        ticker = item.get("ticker", "")
        name   = item.get("name", "")
        if q in ticker.lower() or q in name.lower():
            results.append(item)
            if len(results) >= limit:
                break
    return results


def get_us_sectors() -> list[dict]:
    """US 섹터 목록 + 종목 수 반환."""
    cnt: Counter = Counter()
    for item in get_us_tickers():
        s = item.get("sector", "") or ""
        cnt[s] += 1
    # 빈 섹터는 제외하고 반환
    result = [
        {"id": s, "name": s, "count": c}
        for s, c in sorted(cnt.items())
        if s
    ]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_from_yfinance(symbol: str, period: str = "10y", interval: str = "1d") -> Optional[dict]:
    """yfinance로 OHLCV 로드. 실패 시 None."""
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        df = t.history(period=period, interval=interval, auto_adjust=True)
        if df is None or df.empty:
            return None

        # multi-level columns 처리
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

        df = df.reset_index()
        # Date 컬럼 정규화
        date_col = "Date" if "Date" in df.columns else df.columns[0]
        df["_date_str"] = pd.to_datetime(df[date_col]).dt.strftime(
            "%Y-%m-%d" if interval in ("1d", "1wk") else "%Y-%m-01"
        )

        freq_map = {"1d": "d", "1wk": "w", "1mo": "m"}
        freq = freq_map.get(interval, "d")

        result = {
            "dates":     df["_date_str"].tolist(),
            "open":      df["Open"].fillna(0).round(4).tolist(),
            "high":      df["High"].fillna(0).round(4).tolist(),
            "low":       df["Low"].fillna(0).round(4).tolist(),
            "close":     df["Close"].fillna(0).round(4).tolist(),
            "volume":    df["Volume"].fillna(0).astype(int).tolist(),
            "freq":      freq,
            "last_date": df["_date_str"].iloc[-1] if len(df) > 0 else "",
        }
        return result
    except Exception as e:
        logger.warning("yfinance 로드 실패 (%s, %s): %s", symbol, interval, e)
        return None


def get_us_ohlcv(symbol: str, years: int = 10) -> Optional[dict]:
    """
    3-tier cache: 메모리 → 디스크 → yfinance (일봉).
    당일 last_date면 캐시 유효.
    """
    symbol = symbol.upper()
    today_str = date.today().isoformat()

    # 1) 메모리
    if symbol in _mem_us_ohlcv:
        cached = _mem_us_ohlcv[symbol]
        if cached.get("last_date") == today_str:
            return cached

    # 2) 디스크
    _ensure_dirs()
    cache_path = _US_CACHE_DIR / f"{symbol}.json"
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            if data.get("last_date") == today_str:
                _mem_us_ohlcv[symbol] = data
                if symbol not in _mem_us_names and data.get("name"):
                    _mem_us_names[symbol] = data["name"]
                return data
        except Exception:
            pass

    # 3) yfinance
    period = f"{years}y"
    data = _fetch_from_yfinance(symbol, period=period, interval="1d")
    if data is None:
        return None

    # 회사명 보강
    data["name"] = _mem_us_names.get(symbol, symbol)

    # 저장
    try:
        cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("US OHLCV 저장 실패 (%s): %s", symbol, e)

    _mem_us_ohlcv[symbol] = data
    return data


def get_us_ohlcv_by_timeframe(symbol: str, timeframe: str = "daily") -> Optional[dict]:
    """
    timeframe: 'daily' | 'weekly' | 'monthly'
    daily → get_us_ohlcv() (캐시 활용)
    weekly/monthly → yfinance 직접 (no disk cache, 간단하게)
    """
    symbol = symbol.upper()
    if timeframe == "daily":
        return get_us_ohlcv(symbol)
    interval_map = {"weekly": "1wk", "monthly": "1mo"}
    interval = interval_map.get(timeframe, "1wk")
    return _fetch_from_yfinance(symbol, period="10y", interval=interval)


def get_us_company_name(symbol: str) -> str:
    return _mem_us_names.get(symbol.upper(), symbol.upper())


# ─────────────────────────────────────────────────────────────────────────────
# 검색용 캐시 접근자
# ─────────────────────────────────────────────────────────────────────────────

def all_us_ohlcv() -> dict[str, dict]:
    return _mem_us_ohlcv


def all_us_names() -> dict[str, str]:
    return _mem_us_names


# 자동 프리페치 대상 종목 (S&P 500 상위 + NDX + ETF)
# 티커 목록이 수천 개로 늘어도 자동 다운로드는 이 범위만 수행
_PRIORITY_SYMBOLS: set[str] = (
    {sym for sym, _ in _NDX_SUPPLEMENT}
    | {sym for sym, _ in _ETFS}
    | {sym for sym, _ in _FALLBACK_TICKERS}
)


def prefetch_us_ohlcv_background() -> None:
    """
    서버 시작 시 백그라운드 스레드에서 US OHLCV 프리페치.
    - 디스크 캐시가 유효하면 메모리 로드 (모든 종목)
    - 캐시 없거나 만료 → _PRIORITY_SYMBOLS 에 포함된 종목만 yfinance 다운로드
      (나머지는 사용자가 조회할 때 온디맨드로 다운로드)
    """
    import threading
    import time

    def _worker() -> None:
        tickers = get_us_tickers()
        today_str = date.today().isoformat()
        logger.info(
            "US OHLCV 백그라운드 프리페치 시작: 전체 %d개 (우선순위 자동다운: %d개)",
            len(tickers), len(_PRIORITY_SYMBOLS),
        )
        success = 0
        for item in tickers:
            symbol = item["ticker"]
            # 유효한 디스크 캐시 → 메모리에 로드 (모든 종목)
            cache_path = _US_CACHE_DIR / f"{symbol}.json"
            if cache_path.exists():
                try:
                    data = json.loads(cache_path.read_text(encoding="utf-8"))
                    if data.get("last_date") == today_str:
                        if symbol not in _mem_us_ohlcv:
                            _mem_us_ohlcv[symbol] = data
                            if symbol not in _mem_us_names and data.get("name"):
                                _mem_us_names[symbol] = data["name"]
                        success += 1
                        continue
                except Exception:
                    pass
            # 캐시 없거나 만료 → 우선순위 종목만 yfinance 다운로드
            if symbol not in _PRIORITY_SYMBOLS:
                continue
            data = get_us_ohlcv(symbol)
            if data:
                success += 1
            time.sleep(0.15)  # yfinance rate limit 방지
        logger.info("US OHLCV 백그라운드 프리페치 완료: %d 종목 메모리 로드", success)

    t = threading.Thread(target=_worker, daemon=True, name="us-ohlcv-prefetch")
    t.start()


def ensure_us_ohlcv_from_disk() -> int:
    """
    디스크에 캐시된 US OHLCV를 모두 메모리에 로드 (검색 전 호출).
    네트워크 호출 없이 이전에 조회한 종목들을 검색 대상에 포함시킴.
    """
    _ensure_dirs()
    count = 0
    for cache_path in _US_CACHE_DIR.glob("*.json"):
        symbol = cache_path.stem
        if symbol in _mem_us_ohlcv:
            count += 1
            continue
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            _mem_us_ohlcv[symbol] = data
            if symbol not in _mem_us_names and data.get("name"):
                _mem_us_names[symbol] = data["name"]
            count += 1
        except Exception:
            pass
    return count
