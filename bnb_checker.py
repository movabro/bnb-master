import os
import math
import requests
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote


class BldRgstHubClient:
    """
    건축HUB 건축물대장정보(OpenAPI) 간단 클라이언트
    - getBrTitleInfo: 표제부(주용도/구조/층수/연면적/세대수/주소/사용승인일 등)
    - getBrExposInfo: 전유부(동/호/층 목록) -> 층별 세대(호) 수 집계
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
            # 500/비정상 응답 디버깅이 필요하면 아래 주석 해제
            # print("URL:", resp.url)
            # print("STATUS:", resp.status_code)
            # print(resp.text[:800])

            resp.raise_for_status()

            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype:
                raise RuntimeError(f"Unexpected Content-Type: {ctype}. head={resp.text[:200]}")

            data = resp.json()
            header = data.get("response", {}).get("header", {})
            if header.get("resultCode") != "00":
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
            if len(all_items) >= total_count:
                break

            page_no += 1
            if page_no > max_pages:
                raise RuntimeError(f"Too many pages (>{max_pages}). Check query params.")

        return all_items

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


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _parse_yyyymmdd(s: Any) -> Optional[date]:
    if s is None:
        return None
    t = str(s).strip()
    if len(t) != 8 or not t.isdigit():
        return None
    y, m, d = int(t[0:4]), int(t[4:6]), int(t[6:8])
    try:
        return date(y, m, d)
    except Exception:
        return None


def _years_since(d: Optional[date], today: Optional[date] = None) -> Optional[float]:
    if d is None:
        return None
    today = today or date.today()
    delta_days = (today - d).days
    return round(delta_days / 365.2425, 1)  # 평균 회귀년


def _map_link(addr: str) -> str:
    # 네이버지도 검색 링크 (사람이 클릭해서 확인하기 용도)
    q = quote(addr)
    return f"https://map.naver.com/v5/search/{q}"


@dataclass
class RuleResult:
    ok: bool
    label: str
    detail: str


class AirbnbCheckerRules:
    """
    사용자 요구 순서:
    1) 불법여부확인 (불법이면 즉시 종료)
    2) 연식 확인(30년 초과 여부 + 몇년)
    3) 용도/주택종류/구조(철근콘크리트 여부) + (주택종류별 제한 필터링)
    4) 세대수(총/층별)
    """

    # 동작구 안내문 기준 대상주택
    ALLOWED_HOUSE_TYPES = {"단독주택", "다가구주택", "아파트", "연립주택", "다세대주택"}

    # 안내문에 명시된 “등록 불가”
    DISALLOWED_KEYWORDS = ["오피스텔", "원룸", "다중주택", "위법", "위반"]  # 위반/위법은 별도 violBldYn로도 체크

    def __init__(self, service_key: str):
        self.client = BldRgstHubClient(service_key)

    @staticmethod
    def classify_structure(strct_name: str) -> str:
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
        return strct_name

    @staticmethod
    def detect_house_type(main_purpose: str, etc_purpose: str) -> str:
        hay = (main_purpose or "") + " " + (etc_purpose or "")
        hay = hay.replace(" ", "")

        # 우선순위(더 구체적인 것 먼저)
        if "다가구" in hay:
            return "다가구주택"
        if "다세대" in hay:
            return "다세대주택"
        if "연립" in hay:
            return "연립주택"
        if "아파트" in hay:
            return "아파트"
        if "단독" in hay:
            return "단독주택"

        # mainPurpsCdNm이 "공동주택"처럼 뭉뚱그려 오는 경우가 있어 fallback
        if "공동주택" in (main_purpose or ""):
            return "공동주택(세부미상)"
        return "미상"

    @staticmethod
    def check_house_type_constraints(
        house_type: str,
        *,
        grnd_floors: Optional[int],
        area_m2: Optional[float],
        total_units: Optional[int],
    ) -> List[RuleResult]:
        """
        동작구 안내문 표의 정의(층수/면적/세대수)를 “필터링 조건”으로 사용.
        주의: 안내문은 “주택으로 쓰는 층수/바닥면적 합계” 기준이며,
              API의 grndFlrCnt/totArea는 완전히 동일하진 않을 수 있음(필로티/지하주차장 제외 등).
              그래도 1차 자동 필터로는 충분히 유용.
        """
        rs: List[RuleResult] = []

        def need(v, name: str):
            if v is None:
                rs.append(RuleResult(False, f"{name} 확인", "값이 없어 요건 판단이 어렵습니다(API 응답 누락)."))

        # 공통 결측 경고
        need(grnd_floors, "지상층수")
        need(area_m2, "면적(㎡)")
        if "다가구" in house_type:
            need(total_units, "총 세대수")

        # 다가구
        if house_type == "다가구주택":
            if grnd_floors is not None:
                rs.append(RuleResult(grnd_floors <= 3, "다가구: 지상층수 ≤ 3", f"지상층수={grnd_floors}"))
            if area_m2 is not None:
                rs.append(RuleResult(area_m2 <= 660, "다가구: 면적 ≤ 660㎡", f"면적={area_m2:.2f}㎡"))
            if total_units is not None:
                rs.append(RuleResult(total_units <= 19, "다가구: 19세대 이하", f"총 세대수={total_units}"))

        # 다세대
        elif house_type == "다세대주택":
            if grnd_floors is not None:
                rs.append(RuleResult(grnd_floors <= 4, "다세대: 지상층수 ≤ 4", f"지상층수={grnd_floors}"))
            if area_m2 is not None:
                rs.append(RuleResult(area_m2 <= 660, "다세대: 면적 ≤ 660㎡", f"면적={area_m2:.2f}㎡"))

        # 연립
        elif house_type == "연립주택":
            if grnd_floors is not None:
                rs.append(RuleResult(grnd_floors <= 4, "연립: 지상층수 ≤ 4", f"지상층수={grnd_floors}"))
            if area_m2 is not None:
                rs.append(RuleResult(area_m2 > 660, "연립: 면적 > 660㎡", f"면적={area_m2:.2f}㎡"))

        # 아파트
        elif house_type == "아파트":
            if grnd_floors is not None:
                rs.append(RuleResult(grnd_floors >= 5, "아파트: 지상층수 ≥ 5", f"지상층수={grnd_floors}"))

        # 단독(표의 추가요건 없음)
        elif house_type == "단독주택":
            rs.append(RuleResult(True, "단독: 추가요건(표) 없음", "표 상 별도 제한 없음(기본 대상주택)"))

        return rs

    def run(
        self,
        sigungu_cd: str,
        bjdong_cd: str,
        bun: str,
        ji: str = "0000",
        *,
        require_rc: bool = True,
        include_units_per_floor: bool = True,
        verbose: bool = True,  # [추가] 프린트 on/off 옵션
    ) -> int:
        """
        리턴값 의미:
        0: 에어비앤비 운영 불가능
        1: 운영 가능 & 2층 이상 & 1세대 (예: 2층 단독주택 등)
        2: 운영 가능 & 1층(단층 or 지하1층+지상1층) & 1세대 이상
        3: 운영 가능 & 2~4층 & 지하1층 이하 & 2세대 이상 (다가구/다세대 등)
        4: 운영 가능 & 위 1,2,3 케이스 외 나머지
        """
        
        # 내부 헬퍼 함수: verbose가 True일 때만 출력
        def log(*args, **kwargs):
            if verbose:
                print(*args, **kwargs)

        title_items = self.client.get_title_info(sigungu_cd, bjdong_cd, bun, ji)
        if not title_items:
            log("❌ 표제부 조회 결과가 없습니다. 주소/지번을 확인해주세요.")
            return 0  # 불가능

        title = title_items[0]  # 여러 동이면 dongNm으로 필터 확장 가능

        # 주소
        plat_plc = (title.get("platPlc") or "").strip()
        new_plat_plc = (title.get("newPlatPlc") or "").strip()
        addr_for_map = new_plat_plc or plat_plc
        map_url = _map_link(addr_for_map) if addr_for_map else ""

        # 기본정보
        bld_name = (title.get("bldNm") or "").strip() or "(건물명 없음)"
        main_purps = (title.get("mainPurpsCdNm") or "").strip() or "(주용도 없음)"
        etc_purps = (title.get("etcPurps") or "").strip()

        viol_yn = str(title.get("violBldYn") or "0").strip()
        is_viol = (viol_yn == "1")

        # 구조
        strct_raw = (title.get("strctCdNm") or title.get("etcStrct") or "").strip()
        strct_class = self.classify_structure(strct_raw)

        # 연식/층수/면적/세대수
        use_apr = _parse_yyyymmdd(title.get("useAprDay"))
        age_years = _years_since(use_apr)

        grnd_floors = _to_int(title.get("grndFlrCnt"))
        ugrnd_floors = _to_int(title.get("ugrndFlrCnt"))

        # 면적: totDongTotArea가 있으면 우선, 없으면 totArea 사용
        area_m2 = _to_float(title.get("totDongTotArea"))
        if area_m2 is None:
            area_m2 = _to_float(title.get("totArea"))

        hhld_cnt = _to_int(title.get("hhldCnt"))
        fmly_cnt = _to_int(title.get("fmlyCnt"))
        ho_cnt = _to_int(title.get("hoCnt"))

        # 층별 세대(호) 수 집계
        units_per_floor: List[Dict[str, Any]] = []
        total_units_from_expos: Optional[int] = None
        if include_units_per_floor:
            expos = self.client.get_expos_units(sigungu_cd, bjdong_cd, bun, ji)
            per_floor: Dict[Tuple[str, Union[int, str]], int] = defaultdict(int)

            for u in expos:
                dong = str(u.get("dongNm") or "").strip() or "미상동"
                flr = u.get("flrNo")
                if flr is None:
                    flr = "미상층"
                per_floor[(dong, flr)] += 1

            def _sort_key(k: Tuple[str, Union[int, str]]):
                dong, flr = k
                try:
                    flr_int = int(flr)
                except Exception:
                    flr_int = 10**9
                return (dong, flr_int, str(flr))

            units_per_floor = [
                {"dong": dong, "floor": flr, "unit_count": cnt}
                for (dong, flr), cnt in sorted(per_floor.items(), key=lambda kv: _sort_key(kv[0]))
            ]
            total_units_from_expos = sum(x["unit_count"] for x in units_per_floor) if units_per_floor else 0


        # 총 세대수(우선순위: hhldCnt(세대수) -> fmlyCnt(가구수) -> 전유부 집계(호) -> hoCnt(호수))
        total_units = hhld_cnt
        if total_units is None:
            total_units = fmly_cnt
        if total_units is None and total_units_from_expos is not None:
            total_units = total_units_from_expos
        if total_units is None:
            total_units = ho_cnt


        # 주택종류 추정 + 필터 룰
        house_type = self.detect_house_type(main_purps, etc_purps)

        # ====== 출력(사람 보기 좋은 리포트) ======
        log("\n" + "=" * 72)
        log("🏠 외국인관광 도시민박업(에어비앤비) 가능성 1차 자동판정 리포트")
        log("=" * 72)
        log(f"• 건물명: {bld_name}")
        log(f"• 지번주소(platPlc): {plat_plc or '(없음)'}")
        log(f"• 도로명주소(newPlatPlc): {new_plat_plc or '(없음)'}")
        if map_url:
            log(f"• 지도: {map_url}")
        log(f"• 주용도(mainPurpsCdNm): {main_purps}")
        if etc_purps:
            log(f"• 세부용도(etcPurps): {etc_purps}")

        # 1) 불법여부확인 (불법이면 즉시 종료)
        log("\n[1] 불법여부확인")
        if is_viol:
            log("⛔ 위반/위법 건축물로 표시됨(violBldYn=1) → 즉시 ‘운영 불가능’ 판정")
            log("=" * 72 + "\n")
            return 0 # 불가능
        else:
            log("✅ 위반/위법 건축물 아님(violBldYn=0) → 다음 단계 진행")

        # 2) 연식 확인
        log("\n[2] 연식 확인")
        if use_apr:
            log(f"• 사용승인일(useAprDay): {use_apr.isoformat()}")
        else:
            log("• 사용승인일(useAprDay): (없음)")
        if age_years is not None:
            over_30 = age_years > 30
            log(f"• 경과년수: 약 {age_years}년" + (" (30년 초과)" if over_30 else ""))
        else:
            log("• 경과년수: 계산 불가(사용승인일 없음)")

        # 3) 용도/주택종류/구조 + 제한 필터링
        log("\n[3] 용도/주택종류/구조(필터링 포함)")
        log(f"• 주택종류(추정): {house_type}")
        log(f"• 층수: 지상 {grnd_floors if grnd_floors is not None else '?'}층 / 지하 {ugrnd_floors if ugrnd_floors is not None else '?'}층")
        log(f"• 면적(판정용): {area_m2:.2f}㎡" if area_m2 is not None else "• 면적(판정용): (없음)")

        # (a) 대상주택 필터
        allowed_house = house_type in self.ALLOWED_HOUSE_TYPES
        if allowed_house:
            log("✅ 대상주택 범주에 해당(단독/다가구/아파트/연립/다세대)")
        else:
            log("⛔ 대상주택 범주가 아니거나(또는 세부 미상) → 운영 불가능(구청 확인 필요)")
            log("   ※ ‘공동주택(세부미상)’이면 다세대/아파트/연립 중 무엇인지 추가 확인 필요")
            log("=" * 72 + "\n")
            return 0 # 불가능

        # (b) 안내문 명시 ‘등록 불가’ 키워드(보조 필터)
        combined = (main_purps + " " + etc_purps).replace(" ", "")
        bad_hit = [k for k in self.DISALLOWED_KEYWORDS if k.replace(" ", "") in combined]
        if bad_hit:
            log(f"⛔ 등록 불가 키워드 감지: {', '.join(bad_hit)} → 운영 불가능")
            log("=" * 72 + "\n")
            return 0 # 불가능
        else:
            log("✅ 등록 불가(오피스텔/원룸형/다중주택 등)로 보이는 키워드 없음")

        # (c) 등록기준(연면적 230㎡ 미만) 체크
        if area_m2 is not None:
            if area_m2 < 230:
                log(f"✅ 등록기준(연면적 230㎡ 미만) 충족: {area_m2:.2f}㎡")
            else:
                log(f"⛔ 등록기준(연면적 230㎡ 미만) 미충족: {area_m2:.2f}㎡ → 운영 불가능")
                log("=" * 72 + "\n")
                return 0 # 불가능
        else:
            log("⚠️ 연면적 값이 없어 230㎡ 기준 자동판정 불가(구청/등기/도면으로 확인 권장)")

        # (d) 주택종류별 제한(층수/면적/세대수) 필터
        log("\n• 주택종류별 요건 체크(동작구 안내문 표 기반)")
        rule_results = self.check_house_type_constraints(
            house_type,
            grnd_floors=grnd_floors,
            area_m2=area_m2,
            total_units=total_units,
        )
        for rr in rule_results:
            mark = "✅" if rr.ok else "⛔"
            log(f"  {mark} {rr.label} | {rr.detail}")

        hard_fail = [r for r in rule_results if (not r.ok and "값이 없어" not in r.detail)]
        if hard_fail:
            log("\n⛔ 주택종류 요건 미충족 항목 존재 → 운영 불가능")
            log("=" * 72 + "\n")
            return 0 # 불가능

        # (e) 구조(철근콘크리트 여부)
        log("\n• 구조 확인")
        log(f"  - 원문: {strct_raw or '(없음)'}")
        log(f"  - 분류: {strct_class}")
        if require_rc:
            if strct_class == "철근콘크리트(RC)":
                log("✅ (요청 기준) 철근콘크리트 구조 → 통과")
            else:
                log("⛔ (요청 기준) 철근콘크리트 구조 아님 → 운영 불가능 판정")
                log("=" * 72 + "\n")
                return 0 # 불가능

        # 4) 세대수 확인(총/층별)
        log("\n[4] 세대수 확인")
        log(f"• 총 세대수(판정용): {total_units if total_units is not None else '(없음)'}")
        log(f"  - 세대수(hhldCnt): {hhld_cnt if hhld_cnt is not None else '(없음)'}")
        log(f"  - 가구수(fmlyCnt): {fmly_cnt if fmly_cnt is not None else '(없음)'}")
        log(f"  - 호수(hoCnt): {ho_cnt if ho_cnt is not None else '(없음)'}")
        if total_units_from_expos is not None:
            log(f"  - 전유부(getBrExposInfo) 집계 호수: {total_units_from_expos}")

        if units_per_floor:
            log("\n• 층별 세대수(=전유부 호 수) 상세")
            for row in units_per_floor:
                log(f"  - {row['dong']} / {row['floor']}층: {row['unit_count']}세대")
        else:
            log("• 층별 세대수: (전유부 조회 결과 없음)")


        # 최종 결론
        log("\n" + "-" * 72)
        log("🎯 최종 판정: ✅ 운영 가능(1차 자동판정 기준 통과)")
        log("   ※ 실제 등록은 ‘거주 요건(주민 실거주)’, 공동주택 관리규약/동의 등 추가 요건 확인 필요")
        log("-" * 72)
        log("=" * 72 + "\n")

        # 리턴 로직 개선 (0~4)
        # 비교를 위해 None 값을 0으로 치환 (조건문 오류 방지)
        gf = grnd_floors if grnd_floors is not None else 0 # 지상층수
        uf = ugrnd_floors if ugrnd_floors is not None else 0 # 지하층수
        tu = total_units if total_units is not None else 0 # 총 세대수

        # Case 1: 운영 가능 & 2층 이상 & 1세대
        if (gf >= 2) and ((hhld_cnt == 1) or (fmly_cnt == 1) or (ho_cnt == 1)):
            return 1

        # Case 2: 운영 가능 & 1층(단층 또는 지하1층+지상1층) & 1세대 이상
        # (지상1층이면서 지하가 1층 이하인 경우를 포함)
        if (gf == 1) and (uf <= 1) and ((hhld_cnt >= 1) or (fmly_cnt >= 1) or (ho_cnt >= 1)):
            return 2

        # Case 3: 운영 가능 & 2~4층 & 지하1층 이하 & 2세대 이상
        if (2 <= gf <= 4) and (uf <= 1) and ((hhld_cnt >= 2) or (fmly_cnt >= 2) or (ho_cnt >= 2)):
            return 3

        # Case 4: 위 1,2,3 케이스에 해당하지 않는 경우
        return 4


if __name__ == "__main__":
    # 서비스 키는 .env 파일에 MY_SERVICE_KEY로 설정하세요.
    service_key = os.getenv("MY_SERVICE_KEY")
    if not service_key:
        raise ValueError(".env 파일에 MY_SERVICE_KEY를 설정해주세요.")

    bot = AirbnbCheckerRules(service_key)

    # 예시 1: 프린트 ON (기본값) -> 결과 출력됨
    print("--- Test Case 1: Verbose ON ---")
    result1 = bot.run("11590", "10400", "49", "4", include_units_per_floor=True, require_rc=False, verbose=True)
    print(f"Result Code: {result1}")

    # 예시 2: 프린트 OFF -> 결과 코드만 리턴
    print("\n--- Test Case 2: Verbose OFF ---")
    result2 = bot.run("11590", "10400", "49", "4", include_units_per_floor=True, require_rc=False, verbose=False)
    print(f"Result Code: {result2}")