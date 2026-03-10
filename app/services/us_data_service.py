"""
US 주식 데이터 서비스

데이터 소스 우선순위:
  1. KIS (한국투자증권 Open API) — KIS_APP_KEY / KIS_APP_SECRET 설정 시
  2. yfinance — KIS 미설정 또는 실패 시 fallback

캐시 구조:
  cache/us/tickers.json        — 전체 US 종목 목록 (일 1회 갱신, excd 포함)
  cache/us/ohlcv/{symbol}.json — 일봉 OHLCV (당일 last_date 기준 캐시)

티커 수집:
  1. NASDAQ screener API (전체 미국 상장 ~6000개)
  2. NASDAQ trader FTP (nasdaqlisted.txt + otherlisted.txt, 거래소 코드 포함)
  3. 번들 CSV fallback
  4. Wikipedia S&P 500 fallback
  실패 시 하드코딩 fallback 사용.
"""
import json
import logging
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from app.services import kis_client

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

    nasdaqlisted.txt  — NASDAQ 상장 (~4000개) → excd=NAS
    otherlisted.txt   — NYSE / AMEX 등 (~8000개) → Exchange 컬럼으로 구분
      N=NYSE(NYS), A=AMEX(AMS), P=NYSE ARCA(NYS), Z=BATS(NAS), V=IEX(NYS)
    """
    import urllib.request as _req

    # otherlisted Exchange 컬럼 → KIS excd
    _exch_map = {"N": "NYS", "A": "AMS", "P": "NYS", "Z": "NAS", "V": "NYS"}

    urls = [
        "https://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt",
        "https://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt",
    ]
    seen: set[str] = set()
    result: list[tuple[str, str, str]] = []

    for url in urls:
        is_nasdaq = "nasdaqlisted" in url
        try:
            req = _req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with _req.urlopen(req, timeout=15) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            lines = text.strip().split("\n")
            # 첫 줄 헤더 스킵
            header = lines[0].split("|") if lines else []
            exch_col = None
            if not is_nasdaq:
                try:
                    exch_col = header.index("Exchange")
                except ValueError:
                    exch_col = 2  # 기본 위치
            for line in lines[1:]:
                parts = line.strip().split("|")
                if len(parts) < 2:
                    continue
                sym  = parts[0].strip().replace(".", "-")
                name = parts[1].strip()
                if not sym or not name:
                    continue
                # 테스트 이슈 제외 (nasdaqlisted: col3, otherlisted: col6)
                test_col = 3 if is_nasdaq else 6
                if len(parts) > test_col and parts[test_col].strip().upper() == "Y":
                    continue
                # 메타라인 / 특수 심볼 제외
                if sym.startswith("File") or "/" in sym or "^" in sym or len(sym) > 6:
                    continue
                # 거래소 코드 매핑
                if is_nasdaq:
                    excd = "NAS"
                else:
                    raw_exch = (parts[exch_col].strip() if exch_col and len(parts) > exch_col else "N")
                    excd = _exch_map.get(raw_exch, "NYS")
                if sym not in seen:
                    seen.add(sym)
                    result.append((sym, name, "", excd))   # (sym, name, sector, excd)
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
        # screener exchange → KIS excd 매핑
        _exch_map = {
            "NASDAQ": "NAS", "Nasdaq": "NAS",
            "NYSE": "NYS", "New York Stock Exchange": "NYS",
            "AMEX": "AMS", "NYSE American": "AMS", "NYSE MKT": "AMS",
            "NYSE ARCA": "NYS", "Bats": "NAS", "BATS": "NAS",
        }
        rows = (data.get("data") or {}).get("rows") or []
        result = []
        for row in rows:
            sym = str(row.get("symbol", "") or "").strip().replace(".", "-").replace("^", "")
            name = str(row.get("name", "") or "").strip()
            sector = str(row.get("sector", "") or "").strip()
            exchange = str(row.get("exchange", "") or "").strip()
            excd = _exch_map.get(exchange, "NAS")  # 기본 NAS (screener는 주로 NASDAQ)
            if not sym or not name or sym == "Symbol" or "/" in sym or len(sym) > 8:
                continue
            result.append((sym, name, sector, excd))
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
                    result.append((sym, name, sector, ""))   # excd 미상
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
                result.append((sym, name, sector, ""))   # excd 미상 → 나중에 FTP에서 보강
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
            cached_tickers = cached.get("tickers", [])
            # 날짜 일치 + 종목 수 3000개 이상 + excd가 제대로 채워진 경우만 캐시 사용
            has_excd = sum(1 for t in cached_tickers if t.get("excd") in ("NAS","NYS","AMS"))
            if (cached.get("date") == today_str
                    and len(cached_tickers) >= 3000
                    and has_excd >= 2000):
                logger.info("US 티커 목록 캐시 사용 (%d개, excd %d개)", len(cached_tickers), has_excd)
                return cached_tickers
        except Exception:
            pass

    # 1순위: NASDAQ screener API (~6000개, sector + excd 포함)
    base_stocks = _fetch_nasdaq_screener()

    # 2순위: NASDAQ trader FTP (FTP 접근 가능 환경, ~8000개, excd 포함)
    if len(base_stocks) < 500:
        base_stocks = _fetch_nasdaq_ftp()

    # 3순위: 번들 CSV (서버 방화벽 무관, 항상 작동)
    if len(base_stocks) < 500:
        base_stocks = _fetch_bundled_nasdaq()

    # 4순위: Wikipedia S&P 500
    if len(base_stocks) < 500:
        base_stocks = _fetch_sp500_from_wikipedia()

    # 5순위: 하드코딩 fallback
    if not base_stocks:
        base_stocks = [(s, n, "", "NAS") for s, n in _FALLBACK_TICKERS]

    # 중복 없이 합치기 (base 우선, NDX supplement/ETF는 없는 경우만 추가)
    seen: set[str] = set()
    combined: list[tuple[str, str, str, str]] = []   # (sym, name, sector, excd)
    for item in base_stocks:
        sym = item[0]; name = item[1]
        sector = item[2] if len(item) > 2 else ""
        excd   = item[3] if len(item) > 3 else ""
        if sym not in seen:
            seen.add(sym)
            combined.append((sym, name, sector, excd))

    # NDX supplement은 항상 추가 (FTP 실패 시에도 주요 NASDAQ 종목 보장)
    for sym, name in _NDX_SUPPLEMENT:
        if sym not in seen:
            seen.add(sym)
            combined.append((sym, name, "Technology", "NAS"))
    for sym, name in _ETFS:
        if sym not in seen:
            seen.add(sym)
            combined.append((sym, name, "ETF", "NYS"))

    # FTP excd 보강: 항상 FTP에서 심볼별 거래소 코드를 가져와 덮어씀.
    # screener download=true 는 exchange 필드가 없어 전부 NAS로 기본 세팅되므로
    # FTP(nasdaqlisted + otherlisted)의 정확한 excd로 반드시 override 해야 함.
    ftp_excd_map: dict[str, str] = {}
    try:
        ftp_data = _fetch_nasdaq_ftp()
        ftp_excd_map = {sym: excd for sym, _, _, excd in ftp_data if excd}
        logger.info("FTP excd 맵 %d개 로드", len(ftp_excd_map))
    except Exception as e:
        logger.warning("FTP excd 맵 로드 실패: %s", e)

    if ftp_excd_map:
        # FTP excd 우선 적용 (screener 기본값 NAS를 올바른 거래소 코드로 교체)
        combined = [
            (sym, name, sector, ftp_excd_map.get(sym) or excd or "NAS")
            for sym, name, sector, excd in combined
        ]

    # S&P 500 마킹: Wikipedia에서 심볼 목록 확보 후 is_sp500 플래그 설정
    sp500_syms: set[str] = set()
    try:
        sp500_raw = _fetch_sp500_from_wikipedia()
        sp500_syms = {item[0] for item in sp500_raw}
        logger.info("S&P 500 마킹용 심볼 %d개 확보", len(sp500_syms))
    except Exception as e:
        logger.warning("S&P 500 마킹 실패 (무시): %s", e)

    # 티커 알파벳 순 정렬
    combined.sort(key=lambda x: x[0])
    tickers = [
        {
            "ticker": sym,
            "name": name,
            "sector": sector,
            "excd": excd or "NAS",   # KIS 거래소 코드 (NAS/NYS/AMS)
            "is_sp500": sym in sp500_syms,
        }
        for sym, name, sector, excd in combined
    ]

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


def _get_excd(symbol: str) -> str:
    """티커 캐시에서 KIS 거래소 코드 조회. 없으면 'NAS' 반환."""
    for item in _ticker_list_cache:
        if item.get("ticker") == symbol:
            return item.get("excd") or "NAS"
    return "NAS"


def get_excd(symbol: str) -> str:
    """Public: 티커의 KIS 거래소 코드 반환 (NAS / NYS / AMS)."""
    return _get_excd(symbol)


def _fetch_from_kis(symbol: str, years: int = 10, gubn: str = "0") -> Optional[dict]:
    """
    KIS API로 US OHLCV 조회.
    gubn: '0'=일봉, '1'=주봉, '2'=월봉
    """
    excd = _get_excd(symbol)
    records = kis_client.fetch_us_ohlcv_paginated(symbol, excd, years, gubn)
    if not records:
        return None

    # 오름차순 정렬 (오래된→최신)
    records.sort(key=lambda r: r.get("bass_dt", ""))

    freq_map = {"0": "d", "1": "w", "2": "m"}
    freq = freq_map.get(gubn, "d")

    dates, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for r in records:
        raw_date = r.get("bass_dt", "")
        if not raw_date or len(raw_date) != 8:
            continue
        try:
            if gubn == "2":
                d = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m")
            else:
                d = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
        try:
            o  = round(float(r.get("open") or 0), 4)
            h  = round(float(r.get("high") or 0), 4)
            lo = round(float(r.get("low") or 0), 4)
            c  = round(float(r.get("clos") or 0), 4)
            v  = int(r.get("tvol") or 0)
        except (ValueError, TypeError):
            continue
        if c == 0:
            continue
        dates.append(d)
        opens.append(o)
        highs.append(h)
        lows.append(lo)
        closes.append(c)
        volumes.append(v)

    if not dates:
        return None

    return {
        "dates":     dates,
        "open":      opens,
        "high":      highs,
        "low":       lows,
        "close":     closes,
        "volume":    volumes,
        "freq":      freq,
        "last_date": dates[-1],
    }


def get_us_ohlcv(symbol: str, years: int = 10) -> Optional[dict]:
    """
    3-tier cache: 메모리 → 디스크 → KIS/yfinance (일봉).
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

    # 3) KIS API (설정된 경우)
    data = None
    if kis_client.is_configured():
        data = _fetch_from_kis(symbol, years=years, gubn="0")
        if data is None:
            logger.debug("KIS US OHLCV 실패, yfinance fallback (%s)", symbol)

    # 4) yfinance fallback
    if data is None:
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
    weekly/monthly → KIS 또는 yfinance 직접 (no disk cache)
    """
    symbol = symbol.upper()
    if timeframe == "daily":
        return get_us_ohlcv(symbol)

    # KIS
    if kis_client.is_configured():
        gubn_map = {"weekly": "1", "monthly": "2"}
        gubn = gubn_map.get(timeframe, "1")
        data = _fetch_from_kis(symbol, years=10, gubn=gubn)
        if data:
            return data
        logger.debug("KIS US OHLCV timeframe 실패, yfinance fallback (%s, %s)", symbol, timeframe)

    # yfinance fallback
    interval_map = {"weekly": "1wk", "monthly": "1mo"}
    interval = interval_map.get(timeframe, "1wk")
    return _fetch_from_yfinance(symbol, period="10y", interval=interval)


def get_us_company_name(symbol: str) -> str:
    return _mem_us_names.get(symbol.upper(), symbol.upper())


# ─────────────────────────────────────────────────────────────────────────────
# US 분봉 / 시간봉
# ─────────────────────────────────────────────────────────────────────────────

def get_us_intraday(symbol: str, interval_min: int = 5) -> list[dict] | None:
    """
    US 분봉/시간봉 캔들 반환.
    interval_min: 1 | 5 | 15 | 30 | 60 | 240

    KIS HHDFS76200200 — NMIN: 1, 2, 5, 10, 15, 30 (60/240은 30m 집계).
    time 값은 "display ET as UTC" 방식 Unix timestamp.
    """
    from datetime import timezone
    from app.services.kis_client import fetch_us_minute_paginated, is_configured

    if not is_configured():
        return None

    excd = get_excd(symbol)
    if not excd:
        return None

    # NMIN 매핑: 60/240은 30분봉 데이터를 집계
    native_nmin = interval_min if interval_min <= 30 else 30
    pages_map   = {1: 2, 5: 2, 15: 2, 30: 3, 60: 5, 240: 8}
    pages       = pages_map.get(interval_min, 3)

    raw = fetch_us_minute_paginated(symbol, excd, nmin=native_nmin, pages=pages)
    if not raw:
        return None

    candles: list[dict] = []
    seen: set[str] = set()
    for r in reversed(raw):
        d = r.get("kymd", "")
        t = r.get("khms", "")
        if not d or not t or len(d) != 8 or len(t) != 6:
            continue
        key = d + t
        if key in seen:
            continue
        seen.add(key)
        try:
            dt = datetime(int(d[:4]), int(d[4:6]), int(d[6:]),
                          int(t[:2]), int(t[2:4]), int(t[4:]),
                          tzinfo=timezone.utc)
            candles.append({
                "time":   int(dt.timestamp()),
                "open":   float(r.get("open")  or 0),
                "high":   float(r.get("high")  or 0),
                "low":    float(r.get("low")   or 0),
                "close":  float(r.get("close") or r.get("last") or 0),
                "volume": int(r.get("tvol")    or 0),
            })
        except (ValueError, TypeError):
            continue

    if not candles:
        return None

    if interval_min in (60, 240):
        from app.services.data_service import _aggregate_intraday
        return _aggregate_intraday(candles, interval_min * 60)
    return candles


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
    - 디스크 캐시가 유효하면 메모리 로드 (모든 종목, 장 중/외 공통)
    - 캐시 없거나 만료 → 장 마감 후에만 우선순위 종목 KIS/yfinance 다운로드
      (장 중에는 API 과부하 방지를 위해 대기 후 다운로드)
    """
    import threading
    import time

    def _worker() -> None:
        tickers = get_us_tickers()
        today_str = date.today().isoformat()
        in_market = kis_client.is_market_hours()

        logger.info(
            "US OHLCV 백그라운드 프리페치 시작: 전체 %d개 (우선순위 자동다운: %d개)%s",
            len(tickers), len(_PRIORITY_SYMBOLS),
            " [장 중 — 디스크 캐시만 로드]" if in_market else "",
        )

        disk_loaded = 0
        need_download: list[str] = []

        # 1단계: 디스크 캐시 로드 (장 중/외 공통)
        for item in tickers:
            symbol = item["ticker"]
            cache_path = _US_CACHE_DIR / f"{symbol}.json"
            if cache_path.exists():
                try:
                    data = json.loads(cache_path.read_text(encoding="utf-8"))
                    if data.get("last_date") == today_str:
                        if symbol not in _mem_us_ohlcv:
                            _mem_us_ohlcv[symbol] = data
                            if symbol not in _mem_us_names and data.get("name"):
                                _mem_us_names[symbol] = data["name"]
                        disk_loaded += 1
                        continue
                except Exception:
                    pass
            # 캐시 없거나 만료 → 다운로드 대상 (우선순위 종목만)
            if symbol in _PRIORITY_SYMBOLS:
                need_download.append(symbol)

        logger.info("US OHLCV 디스크 캐시 로드: %d 종목", disk_loaded)

        if not need_download:
            logger.info("US OHLCV 백그라운드 프리페치 완료 (다운로드 대상 없음)")
            return

        # 2단계: 장 중이면 마감까지 대기
        if in_market:
            logger.info(
                "장 중 — US OHLCV 다운로드 %d 종목 대기 중 (장 마감 후 자동 시작)",
                len(need_download),
            )
            # 최대 8시간 대기하며 장 마감 확인 (10분 간격)
            for _ in range(48):
                time.sleep(600)
                if not kis_client.is_market_hours():
                    break
            else:
                logger.info("US OHLCV 대기 시간 초과 — 다운로드 건너뜀 (다음 서버 시작 시 재시도)")
                return
            logger.info("장 마감 확인 — US OHLCV 다운로드 시작 (%d 종목)", len(need_download))

        # 3단계: 다운로드
        downloaded = 0
        for symbol in need_download:
            data = get_us_ohlcv(symbol)
            if data:
                downloaded += 1
            time.sleep(0.15)  # rate limit 방지

        logger.info("US OHLCV 백그라운드 프리페치 완료: 디스크 %d + 신규 %d 종목", disk_loaded, downloaded)

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
