import os
import requests
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/news")

@router.get("/search")
async def get_market_news(query: str):
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="서버에 네이버 API 키가 설정되지 않았습니다.")
        
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    # 정확도를 위해 5개, 유사도순 정렬
    params = {"query": query, "display": 5, "sort": "sim"}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail="네이버 API 호출 실패")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
