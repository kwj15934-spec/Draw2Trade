# Draw2Trade

KOSPI 전 종목 월봉 차트에서 **직접 그린 패턴과 유사한 종목**을 검색하는 웹 애플리케이션.

---

## 핵심 기능

| 기능 | 설명 |
|------|------|
| 실시간 캔들 차트 | TradingView Lightweight Charts 기반 KOSPI 월봉 |
| 패턴 드로잉 | 자유곡선 / 추세선 오버레이 도구 |
| 유사 종목 검색 | Pearson 상관계수 기반 Top 20 |
| 서버 캐싱 | 시작 시 KOSPI 전 종목 월봉 메모리 선로드 |

---

## 프로젝트 구조

```
draw2trade_web/
├── app/
│   ├── main.py                   # FastAPI 엔트리포인트
│   ├── routers/
│   │   ├── chart.py              # GET /api/kospi/list, GET /api/chart/{ticker}
│   │   └── pattern.py            # POST /api/pattern/search
│   └── services/
│       ├── data_service.py       # pykrx 데이터 로더 + 디스크/메모리 캐시
│       └── similarity_service.py # Pearson 유사도 계산 (NumPy 벡터화)
├── static/
│   └── js/
│       ├── chart.js              # LW Charts 초기화 & 데이터 로딩
│       └── draw.js               # 드로잉 도구 & 검색 결과 렌더링
├── templates/
│   ├── base.html                 # 레이아웃 + CSS
│   └── index.html                # 메인 페이지
├── cache/                        # 디스크 캐시 (자동 생성)
│   └── ohlcv/                    # 종목별 월봉 JSON
└── requirements.txt
```

---

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| `GET`  | `/` | 메인 페이지 |
| `GET`  | `/api/kospi/list` | KOSPI 종목 리스트 (쿼리 `category`로 섹터 필터 가능) |
| `GET`  | `/api/kospi/search?q=` | 종목 검색 (티커/회사명) |
| `GET`  | `/api/kospi/categories` | 카테고리(섹터) 목록 + 종목 수 |
| `GET`  | `/api/chart/{ticker}` | 월봉 OHLCV (LW Charts 포맷) |
| `POST` | `/api/pattern/search` | 유사 종목 Top N 검색 |

### POST /api/pattern/search

**Request:**
```json
{
  "draw_points": [0.1, 0.3, 0.6, 0.8, 0.5],
  "lookback_months": 36,
  "top_n": 20
}
```

**Response:**
```json
{
  "results": [
    {"ticker": "005930", "company_name": "삼성전자", "similarity_score": 0.9123},
    {"ticker": "000660", "company_name": "SK하이닉스", "similarity_score": 0.8876}
  ]
}
```

---

## 설치 및 실행

### 1. 요구사항

- Python **3.11** 이상
- 인터넷 연결 (pykrx가 KRX에서 데이터 수집)

### 2. 의존성 설치

```bash
cd draw2trade_web
pip install -r requirements.txt
```

### 3. 서버 실행

```bash
cd draw2trade_web
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

브라우저에서 `http://localhost:8000` 접속.

> **참고:** 첫 실행 시 KOSPI 전 종목(약 800~900개) 월봉 데이터를 수집하므로
> 서버 시작까지 **수 분이 소요**될 수 있습니다.
> 이후 실행부터는 `cache/ohlcv/` 디스크 캐시를 사용해 빠르게 시작됩니다.

### 4. 개발 모드 (코드 변경 시 자동 재시작)

```bash
cd draw2trade_web
uvicorn app.main:app --reload --log-level info
```

---

## 사용 방법

1. 좌측 드롭다운에서 **종목 선택** → **차트 로드** 클릭
2. 드로잉 도구 선택:
   - **✎ 자유곡선** — 마우스 드래그로 원하는 패턴을 자유롭게 그리기
   - **↗ 추세선** — 두 점 클릭으로 추세선 그리기
3. 차트 위에 원하는 **패턴을 그린다**
4. **비교 기간** 선택 (기본 36개월)
5. **유사 종목 검색** 버튼 클릭
6. 우측 사이드바에서 **Top 20** 결과 확인
7. 결과 종목 클릭 → 해당 종목 차트로 이동하여 확인

**지우기**: 드로잉을 지우고 도구를 비활성화 (차트 줌/이동 복원)

---

## 유사도 알고리즘

```
사용자 패턴 (캔버스 픽셀 좌표)
  └→ x축 기준 150구간 bin 평균 리샘플링
  └→ y축 반전 (캔버스 상단=고가=1, 하단=저가=0)
  └→ Min-Max 정규화 (0~1)
         ↓
각 KOSPI 종목 최근 N개월 종가
  └→ 150포인트 선형 보간 리샘플링
  └→ Min-Max 정규화 (0~1)
         ↓
Pearson 상관계수 corr ∈ [-1, 1]
  └→ similarity_score = (corr + 1) / 2 ∈ [0, 1]
         ↓
상위 20개 종목 반환 (내림차순)
```

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| Backend | FastAPI, Python 3.11+ |
| Frontend | HTML5, CSS3, Vanilla JS |
| Chart | TradingView Lightweight Charts v4 |
| Data | pykrx (KOSPI 월봉) |
| Similarity | NumPy (벡터화 Pearson 상관계수) |
| Cache | 메모리 dict + 디스크 JSON |
