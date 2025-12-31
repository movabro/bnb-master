import requests
import json
import pandas as pd
from urllib.parse import unquote
from dotenv import load_dotenv
import os

# .env 파일 로드
load_dotenv()

class AirbnbChecker:
    def __init__(self, service_key=None):
        self.base_url = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
        self.service_key = service_key or os.getenv('MY_SERVICE_KEY')
        self.session = requests.Session()

    def check_building(self, sigungu_cd, bjdong_cd, bun, ji='0000'):
        """
        건축물대장 표제부를 조회하여 에어비앤비 적합성을 판단합니다.
        
        :param sigungu_cd: 시군구 코드 (예: 동작구 11590)
        :param bjdong_cd: 법정동 코드 (예: 노량진동 10100)
        :param bun: 번 (본번)
        :param ji: 지 (부번, 없으면 '0000')
        """

        params = {
            'serviceKey': self.service_key,
            'sigunguCd': sigungu_cd,
            'bjdongCd': bjdong_cd,
            'bun': bun.zfill(4), # 4자리 패딩
            'ji': ji.zfill(4),   # 4자리 패딩
            'numOfRows': 10,
            'pageNo': 1,
            '_type': 'json'      # JSON 포맷 요청
        }

        try:
            resp = self.session.get(self.base_url, params=params, timeout=10)
            # 디버그에 유용: 실제 요청 URL 확인
            # print("REQ:", resp.url)

            if resp.status_code >= 400:
                # 공공데이터 API가 500을 주는 케이스 포함
                return f"❌ API HTTP 오류: {resp.status_code}\n{resp.text[:500]}"
            # Content-Type이 application/json;charset=UTF-8 형태여도 파싱 시도
            try:
                data = resp.json()
            except ValueError:
                return f"❌ JSON 파싱 실패(응답이 JSON이 아님):\n{resp.text[:500]}"

            header = (data.get("response") or {}).get("header") or {}
            if header.get("resultCode") != "00":
                return f"❌ API 오류: resultCode={header.get('resultCode')} / resultMsg={header.get('resultMsg')}"

            body = (data.get("response") or {}).get("body") or {}
            items = (body.get("items") or {}).get("item")

            if not items:
                return "❌ 해당 주소의 건축물대장을 찾을 수 없습니다."

            item = items[0] if isinstance(items, list) else items
            return self._analyze_building(item)
        

        except requests.RequestException as e:
            return f"⚠️ 요청 실패: {e!r}"

    def _analyze_building(self, item):
        """
        수신된 데이터를 바탕으로 적합성 분석
        """
        bld_name = item.get('bldNm', '이름없는 건물')
        main_purps = item.get('mainPurpsCdNm', '미지정') # 주용도
        viol_yn = item.get('violBldYn', '0') # 위반건축물 여부 (0: 정상, 1: 위반)

        print(f"--- 🏢 건물 분석 결과: {bld_name} ---")
        print(f"📍 용도: {main_purps}")
        
        # 1. 위반건축물 체크
        if viol_yn == '1':
            return "⛔ [부적합] 위반건축물입니다. (허가 절대 불가)"

        # 2. 용도 체크 (오피스텔, 근생 등 필터링)
        # 허가 가능 용도: 단독, 다가구, 다세대, 연립, 아파트
        allowed_types = ['단독주택', '다가구주택', '다세대주택', '연립주택', '아파트', '도시형생활주택']
        
        # 주의 용도: 근린생활시설(상가), 업무시설(오피스텔)
        if any(dtype in main_purps for dtype in allowed_types):
            return "✅ [적합 예상] 주거용 건물입니다. (단, 호스트 거주 요건 등 세부 확인 필요)"
        elif "업무시설" in main_purps:
            return "⚠️ [주의] 오피스텔(업무시설)은 원칙적으로 '외국인관광도시민박업' 불가합니다. (위홈 특례 제외)"
        elif "근린생활시설" in main_purps:
            return "⛔ [부적합] 근린생활시설(상가)은 민박업 등록이 불가능합니다."
        else:
            return f"❓ [판단 보류] 용도가 '{main_purps}'입니다. 구청 문의가 필요합니다."

# --- 실행 예시 ---
# .env 파일에 MY_SERVICE_KEY를 설정하세요
bot = AirbnbChecker()

# 예: 서울 동작구(11590) 노량진동(10100) 123-4번지 조회 시
# 법정동 코드는 '행정표준코드관리시스템'에서 확인 가능
# 본동(10400)	
result = bot.check_building('11590', '10400', '48', '31')
print(result)