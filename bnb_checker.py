import requests
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import date, datetime
from dotenv import load_dotenv
import os
from urllib.parse import unquote, quote

# .env 파일 로드
load_dotenv()



class BldRgstHubClient:
    """
    건축HUB 건축물대장(OpenAPI) 클라이언트
    - 표제부: getBrTitleInfo
    - 전유부: getBrExposInfo (동/호/층) -> 층별 세대수 집계 가능
    """

    BASE_URL = "https://apis.data.go.kr/1613000/BldRgstHubService"

    def __init__(self, service_key: str, timeout: int = 15):
        if not service_key:
            raise ValueError("service_key is required")
        # 혹시 인코딩된 키(%2F...)가 들어와도 방어
        self.service_key = unquote(service_key)
        self.timeout = timeout

    def _request_items(
        self,
        operation: str,
        params: Dict[str, Any],
        *,
        num_of_rows: int = 100,
        max_pages: int = 200,
    ) -> List[Dict[str, Any]]:
        url = f"{self.BASE_URL}/{operation}"
        all_items: List[Dict[str, Any]] = []

        page_no = 1
        while True:
            q = dict(params)
            q.update(
                {
                    "serviceKey": self.service_key,
                    "_type": "json",
                    "numOfRows": num_of_rows,
                    "pageNo": page_no,
                }
            )

            resp = requests.get(url, params=q, timeout=self.timeout)
            resp.raise_for_status()

            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype:
                raise RuntimeError(f"Unexpected Content-Type: {ctype}. Head: {resp.text[:200]}")

            data = resp.json()

            header = (data.get("response") or {}).get("header") or {}
            if header.get("resultCode") != "00":
                raise RuntimeError(f"API error {header.get('resultCode')}: {header.get('resultMsg')}")

            body = (data.get("response") or {}).get("body") or {}
            items = (body.get("items") or {})
            item = items.get("item")

            if not item:
                break

            if isinstance(item, list):
                all_items.extend([x for x in item if isinstance(x, dict)])
            elif isinstance(item, dict):
                all_items.append(item)

            total_count = int(body.get("totalCount") or len(all_items))
            if len(all_items) >= total_count:
                break

            page_no += 1
            if page_no > max_pages:
                raise RuntimeError(f"Too many pages (>{max_pages}). Check query params.")

        return all_items

    def get_title_info(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
        plat_gb_cd: Optional[str] = "0",
    ) -> List[Dict[str, Any]]:
        params = {
            "sigunguCd": sigungu_cd,
            "bjdongCd": bjdong_cd,
            "bun": str(bun).zfill(4),
            "ji": str(ji).zfill(4),
        }
        if plat_gb_cd is not None:
            params["platGbCd"] = plat_gb_cd
        return self._request_items("getBrTitleInfo", params)

    def get_expos_units(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
        plat_gb_cd: Optional[str] = "0",
    ) -> List[Dict[str, Any]]:
        params = {
            "sigunguCd": sigungu_cd,
            "bjdongCd": bjdong_cd,
            "bun": str(bun).zfill(4),
            "ji": str(ji).zfill(4),
        }
        if plat_gb_cd is not None:
            params["platGbCd"] = plat_gb_cd
        return self._request_items("getBrExposInfo", params)


class AirbnbBuildingReporter:
    def __init__(self, service_key: str):
        self.client = BldRgstHubClient(service_key)

    @staticmethod
    def _classify_structure(strct_name: str) -> str:
        s = (strct_name or "").replace(" ", "")
        if not s:
            return "미확인"
        if "철근" in s and "콘크리트" in s:
            return "철근콘크리트(RC)"
        if "벽돌" in s:
            return "벽돌"
        if "철골" in s:
            return "철골"
        if "목" in s:
            return "목구조"
        return strct_name.strip() if strct_name else "미확인"

    @staticmethod
    def _parse_yyyymmdd(s: str) -> Optional[date]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y%m%d").date()
        except Exception:
            return None

    @staticmethod
    def _years_since(d: Optional[date]) -> Optional[int]:
        if not d:
            return None
        today = date.today()
        return today.year - d.year - ((today.month, today.day) < (d.month, d.day))

    @staticmethod
    def _fmt_date(d: Optional[date]) -> str:
        return d.isoformat() if d else "정보없음"

    @staticmethod
    def _to_int(x) -> Optional[int]:
        try:
            return int(str(x).strip())
        except Exception:
            return None

    def analyze(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
        plat_gb_cd: str = "0",
    ) -> Dict[str, Any]:
        title_items = self.client.get_title_info(sigungu_cd, bjdong_cd, bun, ji, plat_gb_cd=plat_gb_cd)
        if not title_items:
            return {"ok": False, "message": "❌ 표제부 조회 결과가 없습니다. 주소/지번을 확인해주세요."}

        title = title_items[0]

        # 주소(지번/도로명)  :contentReference[oaicite:1]{index=1}
        address_jibun = (title.get("platPlc") or "").strip()
        address_road = (title.get("newPlatPlc") or "").strip()
        address_display = address_road or address_jibun or "정보없음"

        q = quote(address_display)
        map_links = {
            "naver": f"https://map.naver.com/v5/search/{q}",
            "kakao": f"https://map.kakao.com/link/search/{q}",
            "google": f"https://www.google.com/maps/search/?api=1&query={q}",
        }


        bld_name = (title.get("bldNm") or "").strip() or "이름없는 건물"
        main_purpose = (title.get("mainPurpsCdNm") or "").strip() or "미지정"
        viol_building = ((title.get("violBldYn") or "0").strip() == "1")

        # 연식: 사용승인일 우선, 없으면 착공/허가일로 fallback
        use_apr = self._parse_yyyymmdd(title.get("useAprDay") or "")
        stcns = self._parse_yyyymmdd(title.get("stcnsDay") or "")
        pms = self._parse_yyyymmdd(title.get("pmsDay") or "")

        base_date = use_apr or stcns or pms
        base_src = "useAprDay(사용승인일)" if use_apr else ("stcnsDay(착공일)" if stcns else ("pmsDay(허가일)" if pms else "정보없음"))
        age_years = self._years_since(base_date)

        # 구조
        structure_raw = (title.get("strctCdNm") or title.get("etcStrct") or "").strip()
        structure_class = self._classify_structure(structure_raw)

        # 총 세대수(표제부)
        hhld_cnt = self._to_int(title.get("hhldCnt"))
        ho_cnt = self._to_int(title.get("hoCnt"))
        fmly_cnt = self._to_int(title.get("fmlyCnt"))

        # 층별 세대수(전유부)
        units = self.client.get_expos_units(sigungu_cd, bjdong_cd, bun, ji, plat_gb_cd=plat_gb_cd)
        per_floor: Dict[Tuple[str, Union[int, str]], int] = defaultdict(int)

        for u in units:
            dong = str(u.get("dongNm") or "").strip() or "미상동"
            flr = u.get("flrNo")
            flr = flr if flr is not None else "미상층"
            per_floor[(dong, flr)] += 1

        def sort_key(k):
            dong, flr = k
            try:
                flr_int = int(flr)
            except Exception:
                flr_int = 10**9
            return (dong, flr_int, str(flr))

        units_per_floor = [
            {"dong": dong, "floor": flr, "unit_count": cnt}
            for (dong, flr), cnt in sorted(per_floor.items(), key=lambda kv: sort_key(kv[0]))
        ]

        # 총 세대수 결정(표제부 우선, 없으면 전유부 합산)
        total_units = hhld_cnt or ho_cnt or fmly_cnt
        if not total_units and units_per_floor:
            total_units = sum(x["unit_count"] for x in units_per_floor)

        return {
            "ok": True,
            "building_name": bld_name,
            "main_purpose": main_purpose,
            "viol_building": viol_building,
            "age_years": age_years,
            "base_date": base_date,
            "base_date_source": base_src,
            "structure_raw": structure_raw,
            "structure_class": structure_class,
            "hhld_cnt": hhld_cnt,
            "ho_cnt": ho_cnt,
            "fmly_cnt": fmly_cnt,
            "total_units": total_units,
            "units_per_floor": units_per_floor,
            "address_jibun": address_jibun,
            "address_road": address_road,
            "address_display": address_display,
            "map_links": map_links,

        }

    def assess_and_print(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
        plat_gb_cd: str = "0",
    ) -> Dict[str, Any]:
        info = self.analyze(sigungu_cd, bjdong_cd, bun, ji, plat_gb_cd=plat_gb_cd)
        if not info.get("ok"):
            print(info.get("message", "❌ 알 수 없는 오류"))
            return info

        print("\n" + "=" * 60)
        print(f" - 지번주소: {info.get('address_jibun') or '정보없음'}")
        print(f" - 도로명주소: {info.get('address_road') or '정보없음'}")
        print(" - 지도 링크:")
        links = info.get("map_links") or {}
        if links:
            print(f"   · 네이버: {links.get('naver')}")
            print(f"   · 카카오: {links.get('kakao')}")

        print("🏢 에어비앤비 운영 가능성 점검(건축물대장 기반)")
        print(f" - 건물명: {info['building_name']}")
        print(f" - 주용도: {info['main_purpose']}")
        print("=" * 60)

        # 1. 불법여부확인 (위반이면 즉시 종료)
        if info["viol_building"]:
            print("1) 불법 여부 확인: ⛔ 위반건축물")
            print("\n⛔ 최종 판정: 운영 불가능 (위반건축물)")
            info["final_ok"] = False
            info["final_reasons"] = ["위반건축물"]
            return info
        else:
            print("1) 불법 여부 확인: ✅ 정상")

        # 2. 연식 확인 (30년 넘었는지, 넘어도 됨. 몇 년인지 계산)
        age = info.get("age_years")
        base_src = info.get("base_date_source") or "정보없음"
        base_date = info.get("base_date")
        if age is None:
            print(f"2) 연식 확인: 정보없음 (기준일자: {base_src})")
        else:
            flag = "⚠️ 30년 이상" if age >= 30 else "✅ 30년 미만"
            print(f"2) 연식 확인: {self._fmt_date(base_date)} 기준 / {age}년 경과 ({flag})")

        # 3. 용도 확인 + 구조 확인(철근콘크리트인지)
        allowed_purposes = ["단독주택", "다가구주택", "다세대주택", "연립주택", "아파트", "도시형생활주택"]
        purpose_ok = any(t in info["main_purpose"] for t in allowed_purposes)

        structure_ok = (info.get("structure_class") == "철근콘크리트(RC)")

        print("3) 용도/구조 확인:")
        print(f"   - 용도: {info['main_purpose']} " + ("✅" if purpose_ok else "⛔"))
        print(f"   - 구조: {info.get('structure_class','미확인')} (원문: {info.get('structure_raw','')}) " + ("✅" if structure_ok else "⛔"))

        # 4. 세대수 확인 (총 세대수 + 층별 세대수)
        print("4) 세대수 확인:")
        print(f"   - 총 세대수: {info.get('total_units') if info.get('total_units') else '정보없음'}")

        units_per_floor = info.get("units_per_floor") or []
        if not units_per_floor:
            print("   - 층별 세대수: 전유부(호/층) 정보 없음(단독/다가구 등은 비어있을 수 있음)")
        else:
            by_dong = defaultdict(list)
            for x in units_per_floor:
                by_dong[x["dong"]].append(x)

            for dong, rows in by_dong.items():
                print(f"   - {dong}:")
                for r in rows:
                    print(f"     · {r['floor']}층: {r['unit_count']}세대")

        # 최종 판정(요청하신 순서대로 1 통과 후 2,3,4 확인 -> 최종은 3의 조건으로 결정)
        reasons = []
        if not purpose_ok:
            reasons.append("주용도가 주거용 범주가 아님")
        if not structure_ok:
            reasons.append("구조가 철근콘크리트(RC)가 아님")

        final_ok = (len(reasons) == 0)

        print("\n" + ("✅ 최종 판정: 운영 가능" if final_ok else "⛔ 최종 판정: 운영 불가능"))
        if reasons:
            for r in reasons:
                print(f"   - 사유: {r}")

        info["final_ok"] = final_ok
        info["final_reasons"] = reasons
        return info


if __name__ == "__main__":
    # 서비스 키는 .env 파일에 MY_SERVICE_KEY로 설정하세요.
    SERVICE_KEY = os.getenv("MY_SERVICE_KEY", "").strip()
    if not SERVICE_KEY:
        raise ValueError(".env 파일에 MY_SERVICE_KEY를 설정해주세요.")

    reporter = AirbnbBuildingReporter(SERVICE_KEY)

    # 2) 조회할 지번코드 입력
    # 예시(사용자 제공 값):
    reporter.assess_and_print("11590", "10400", "48", "31")
    reporter.assess_and_print("11590", "10400", "50", "29")
    reporter.assess_and_print("11590", "10400", "49", "4")
    reporter.assess_and_print("11590", "10400", "51", "10")