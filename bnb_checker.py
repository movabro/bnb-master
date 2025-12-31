import requests
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import date, datetime
from dotenv import load_dotenv
import os

# .env 파일 로드
load_dotenv()



class BldRgstHubClient:
    """
    건축HUB 건축물대장정보(OpenAPI) 간단 클라이언트
    - getBrTitleInfo: 표제부(주용도/구조 등)
    - getBrExposInfo: 전유부(동/호/층 목록) -> 층별 세대(호) 수 집계 가능
    """

    BASE_URL = "https://apis.data.go.kr/1613000/BldRgstHubService"

    def __init__(self, service_key: str, timeout: int = 15):
        if not service_key:
            raise ValueError("service_key is required")
        self.service_key = service_key
        self.timeout = timeout

    def _request_items(
        self,
        operation: str,
        params: Dict[str, Any],
        *,
        num_of_rows: int = 100,
        max_pages: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        공통 요청 + 페이징 처리
        반환: items/item을 list[dict]로 정규화
        """
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
            # 서버가 500을 주는 경우가 있어, 응답 본문을 같이 보고 싶으면 아래 주석 해제
            # print("URL:", resp.url)
            # print("STATUS:", resp.status_code)
            # print(resp.text[:500])

            resp.raise_for_status()

            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype:
                raise RuntimeError(f"Unexpected Content-Type: {ctype}. Response head: {resp.text[:200]}")

            data = resp.json()

            header = data.get("response", {}).get("header", {})
            if header.get("resultCode") != "00":
                # 예: resultCode 03/99 등
                raise RuntimeError(f"API error {header.get('resultCode')}: {header.get('resultMsg')}")

            body = data.get("response", {}).get("body", {})
            items = body.get("items", {})
            item = items.get("item")

            if not item:
                break

            if isinstance(item, list):
                all_items.extend([x for x in item if isinstance(x, dict)])
            elif isinstance(item, dict):
                all_items.append(item)

            total_count = int(body.get("totalCount") or len(all_items))
            # 다음 페이지가 없으면 종료
            if len(all_items) >= total_count:
                break

            page_no += 1
            if page_no > max_pages:
                raise RuntimeError(f"Too many pages (>{max_pages}). Check query params.")
        return all_items

    # --- API wrappers ---
    def get_title_info(
        self, sigungu_cd: str, bjdong_cd: str, bun: str, ji: str = "0000", plat_gb_cd: Optional[str] = None
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
        self, sigungu_cd: str, bjdong_cd: str, bun: str, ji: str = "0000", plat_gb_cd: Optional[str] = None
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


class AirbnbChecker:
    def __init__(self, service_key: str):
        self.client = BldRgstHubClient(service_key)

    @staticmethod
    def _parse_yyyymmdd(s: str):
        s = (s or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y%m%d").date()
        except Exception:
            return None

    @staticmethod
    def _years_since(d):
        if not d:
            return None
        today = date.today()
        years = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
        return years

    @staticmethod
    def _fmt_date(d):
        return d.isoformat() if d else "정보없음"

    @staticmethod
    def _classify_structure(strct_name: str) -> str:
        """
        구조 문자열을 사람이 보기 좋은 카테고리로 단순 분류
        """
        s = (strct_name or "").replace(" ", "")
        if not s:
            return "미확인"

        # 예: 철근콘크리트구조, 철근콘크리트조
        if "철근" in s and "콘크리트" in s:
            return "철근콘크리트(RC)"
        if "벽돌" in s:
            return "벽돌"
        if "철골" in s:
            return "철골"
        if "목" in s:
            return "목구조"
        return strct_name

    def check_building(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
        *,
        include_units_per_floor: bool = True,
    ) -> Dict[str, Any]:
        title_items = self.client.get_title_info(sigungu_cd, bjdong_cd, bun, ji)
        if not title_items:
            return {"ok": False, "message": "❌ 표제부 조회 결과가 없습니다. 주소/지번을 확인해주세요."}

        title = title_items[0]

        bld_name = (title.get("bldNm") or "").strip() or "이름없는 건물"
        main_purps = (title.get("mainPurpsCdNm") or "").strip() or "미지정"
        viol_yn = (title.get("violBldYn") or "0").strip()

        # 연식(사용승인일 우선)  :contentReference[oaicite:6]{index=6}
        use_apr_raw = (title.get("useAprDay") or "").strip()
        stcns_raw = (title.get("stcnsDay") or "").strip()
        pms_raw = (title.get("pmsDay") or "").strip()

        used_date_src = None
        used_date = self._parse_yyyymmdd(use_apr_raw)
        if used_date:
            used_date_src = "useAprDay(사용승인일)"
        else:
            used_date = self._parse_yyyymmdd(stcns_raw)
            if used_date:
                used_date_src = "stcnsDay(착공일)"
            else:
                used_date = self._parse_yyyymmdd(pms_raw)
                if used_date:
                    used_date_src = "pmsDay(허가일)"

        age_years = self._years_since(used_date)

        # 구조 정보: strctCdNm / etcStrct :contentReference[oaicite:7]{index=7}
        strct_raw = (title.get("strctCdNm") or title.get("etcStrct") or "").strip()
        strct_class = self._classify_structure(strct_raw)

        # 총 세대수(표제부) :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9}
        def _to_int(x):
            try:
                return int(str(x).strip())
            except Exception:
                return None

        hhld_cnt = _to_int(title.get("hhldCnt"))
        ho_cnt = _to_int(title.get("hoCnt"))
        fmly_cnt = _to_int(title.get("fmlyCnt"))

        result: Dict[str, Any] = {
            "ok": True,
            "building_name": bld_name,
            "main_purpose": main_purps,
            "viol_building": (viol_yn == "1"),
            "structure_raw": strct_raw,
            "structure_class": strct_class,
            "use_date": used_date,
            "use_date_source": used_date_src,
            "age_years": age_years,
            "hhld_cnt": hhld_cnt,
            "ho_cnt": ho_cnt,
            "fmly_cnt": fmly_cnt,
        }

        if include_units_per_floor:
            units = self.client.get_expos_units(sigungu_cd, bjdong_cd, bun, ji)
            per_floor: Dict[Tuple[str, Union[int, str]], int] = defaultdict(int)

            for u in units:
                dong = (str(u.get("dongNm") or "").strip() or "미상동")
                flr = u.get("flrNo")
                flr = flr if flr is not None else "미상층"
                per_floor[(dong, flr)] += 1

            def _sort_key(k):
                dong, flr = k
                try:
                    flr_int = int(flr)
                except Exception:
                    flr_int = 10**9
                return (dong, flr_int, str(flr))

            result["units_per_floor"] = [
                {"dong": dong, "floor": flr, "unit_count": cnt}
                for (dong, flr), cnt in sorted(per_floor.items(), key=lambda kv: _sort_key(kv[0]))
            ]
        else:
            result["units_per_floor"] = []

        return result

    def assess_and_print(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
    ) -> Dict[str, Any]:
        info = self.check_building(sigungu_cd, bjdong_cd, bun, ji, include_units_per_floor=True)
        if not info.get("ok"):
            print(info.get("message", "❌ 알 수 없는 오류"))
            return info

        print("\n" + "=" * 50)
        print("🏢 건축물대장 기반 운영 가능성 점검")
        print(f"- 건물명: {info['building_name']}")
        print(f"- 주용도: {info['main_purpose']}")
        print("=" * 50)

        # 1) 불법 여부 확인 (위반이면 즉시 종료)
        if info["viol_building"]:
            print("1) 불법 여부(위반건축물): ⛔ 위반건축물")
            print("\n⛔ 최종 판정: 운영 불가능 (위반건축물)")
            info["final_ok"] = False
            info["final_reason"] = "위반건축물"
            return info
        print("1) 불법 여부(위반건축물): ✅ 정상")

        # 2) 연식 확인
        age = info.get("age_years")
        src = info.get("use_date_source") or "정보없음"
        d = info.get("use_date")
        if age is None:
            print(f"2) 연식: 정보없음 (기준일자 필드가 비어있음: {src})")
        else:
            over_30 = age >= 30
            print(f"2) 연식: {self._fmt_date(d)} 기준 / {age}년 경과" + (" (⚠️ 30년 이상)" if over_30 else ""))

        # 3) 용도 + 구조 확인 (RC 여부)
        allowed_types = ["단독주택", "다가구주택", "다세대주택", "연립주택", "아파트", "도시형생활주택"]
        purpose_ok = any(t in info["main_purpose"] for t in allowed_types)

        rc_ok = (info.get("structure_class") == "철근콘크리트(RC)")
        print(f"3) 용도: {info['main_purpose']} " + ("✅" if purpose_ok else "⛔"))
        print(f"   구조: {info.get('structure_class','미확인')} (원문: {info.get('structure_raw','')}) " + ("✅" if rc_ok else "⛔"))

        # 4) 세대수 확인(총 세대수 + 층별 세대수)
        units_per_floor = info.get("units_per_floor") or []

        # 총 세대수는 표제부 hhldCnt/hoCnt 우선, 없으면 전유부 합산
        total_units = info.get("hhld_cnt") or info.get("ho_cnt")
        if not total_units and units_per_floor:
            total_units = sum(x["unit_count"] for x in units_per_floor)

        print("4) 세대수:")
        print(f"   - 총 세대수: {total_units if total_units else '정보없음'} (표제부 hhldCnt/hoCnt 우선, 없으면 전유부 합산)")

        if not units_per_floor:
            print("   - 층별 세대수: 전유부(호/층) 정보 없음(단독/다가구 등은 비어있을 수 있음)")
        else:
            # 동별로 보기 좋게 출력
            by_dong = defaultdict(list)
            for x in units_per_floor:
                by_dong[x["dong"]].append(x)

            for dong, rows in by_dong.items():
                print(f"   - {dong}:")
                for r in rows:
                    print(f"     · {r['floor']}층: {r['unit_count']}세대")

        # 최종 판정
        final_ok = purpose_ok and rc_ok
        print("\n" + ("✅ 최종 판정: 운영 가능" if final_ok else "⛔ 최종 판정: 운영 불가능") )
        if not purpose_ok:
            print("   - 사유: 주용도가 주거용(단독/다가구/다세대/연립/아파트/도생) 범주가 아님")
        if not rc_ok:
            print("   - 사유: 구조가 철근콘크리트(RC) 기준을 충족하지 않음")

        info["final_ok"] = final_ok
        return info


if __name__ == "__main__":
    # 서비스 키는 .env 파일에 MY_SERVICE_KEY로 설정하세요.
    MY_SERVICE_KEY = os.getenv('MY_SERVICE_KEY')
    if not MY_SERVICE_KEY:
        raise ValueError(".env 파일에 MY_SERVICE_KEY를 설정해주세요.")
    bot = AirbnbChecker(MY_SERVICE_KEY)

    # info = bot.check_building("11590", "10400", "48", "31", include_units_per_floor=True)
    # print(info)
    bot.assess_and_print("11590", "10400", "48", "31")
    bot.assess_and_print("11590", "10400", "50", "29")
    bot.assess_and_print("11590", "10400", "49", "4")
    bot.assess_and_print("11590", "10400", "51", "10")
