import csv
import os
import sys
from typing import List, Dict, Set

# 같은 경로에 있는 bnb_checker.py를 임포트합니다.
try:
    from bnb_checker import AirbnbCheckerRules
except ImportError:
    print("❌ 오류: 'bnb_checker.py' 파일을 찾을 수 없습니다. 같은 디렉토리에 있는지 확인해주세요.")
    sys.exit(1)

def process_and_split_csv(
    input_file: str,
    service_key: str
):
    """
    1. CSV 파일을 읽어 주소(지번)를 정규화한 후 중복을 제거합니다.
    2. 중복이 제거된 데이터셋(unique_addresses)에 대해서만 판정을 수행합니다.
    3. 판정 결과(0~4)에 따라 별도의 CSV 파일로 저장합니다.
    """
    
    # 결과 파일명 매핑
    output_files = {
        0: "bnb_result_0_unsuitable.csv",       # 에어비앤비 부적합
        1: "bnb_result_1_low_chance.csv",       # 에어비앤비 적합 가능성 낮음
        2: "bnb_result_2_possible.csv",         # 에어비앤비 적합 가능성 있음
        3: "bnb_result_3_high_chance.csv",      # 에어비앤비 적합 가능성 높음
        4: "bnb_result_4_pending.csv"           # 에어비앤비 적합 판단 보류
    }

    # 결과 데이터를 담을 컨테이너 (0~4 리스트)
    classified_data = {k: [] for k in range(5)}
    
    # 통계용 카운터
    stats = {k: 0 for k in range(5)}
    total_processed = 0
    duplicate_count = 0

    # 1. 중복 제거 및 데이터 로딩
    # (여기서 저장된 데이터만 판정 로직으로 넘어갑니다)
    unique_addresses = []
    seen_keys: Set[tuple] = set()

    print(f"📂 입력 파일 로딩 중: {input_file} ...")
    
    if not os.path.exists(input_file):
        print(f"❌ 입력 파일이 존재하지 않습니다: {input_file}")
        return

    with open(input_file, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        fieldnames = reader.fieldnames
        if not fieldnames:
            print("❌ CSV 헤더를 읽을 수 없습니다.")
            return

        for row in reader:
            # 필수 키 컬럼 추출 (공백 제거)
            sigungu = row.get("시군구코드", "").strip()
            bjdong = row.get("법정동코드", "").strip()
            bun = row.get("번", "").strip()
            ji = row.get("지", "").strip()

            # [수정] 지번 정규화 로직을 여기로 이동
            # 빈 값("")과 "0000"이 섞여 있어 중복 제거가 안 되는 문제를 방지하기 위해 통일
            if not ji:
                ji = "0000"
            
            # 정규화된 값을 row에도 반영 (판정 시 사용)
            row['지'] = ji

            # 필수 정보가 없으면 스킵
            if not (sigungu and bjdong and bun):
                continue

            # 중복 키 생성 (시군구, 법정동, 번, 지)
            unique_key = (sigungu, bjdong, bun, ji)

            if unique_key in seen_keys:
                duplicate_count += 1
                continue # 중복이면 리스트에 넣지 않음 (판정 대상에서 제외)
            
            seen_keys.add(unique_key)
            unique_addresses.append(row)

    print(f"✅ 데이터 정제 완료: 총 {len(unique_addresses)}건 (중복 제거됨: {duplicate_count}건)")
    print("-" * 60)

    # 2. 판정 수행 (중복이 제거된 unique_addresses 만 사용)
    checker = AirbnbCheckerRules(service_key)
    
    print(f"🚀 총 {len(unique_addresses)}건에 대해 에어비앤비 적합성 판정 시작...")
    
    for idx, row in enumerate(unique_addresses, 1):
        sigungu = row.get("시군구코드", "")
        bjdong = row.get("법정동코드", "")
        bun = row.get("번", "")
        ji = row.get("지", "") # 위에서 이미 "0000"으로 정규화됨
        
        try:
            # bnb_checker 실행
            result_code = checker.run(
                sigungu_cd=sigungu,
                bjdong_cd=bjdong,
                bun=bun,
                ji=ji,
                require_rc=False,            # 철근콘크리트 필수 아님
                include_units_per_floor=True, # 층별 세대수 확인
                verbose=False                # 로그 출력 끔
            )
            
            # 결과값 범위 체크 (0~4)
            if result_code not in classified_data:
                result_code = 4

            # 결과 데이터 구성
            result_row = row.copy()
            result_row['판정코드'] = result_code
            result_row['판정의미'] = get_result_description(result_code)
            
            classified_data[result_code].append(result_row)
            stats[result_code] += 1
            total_processed += 1
            
            # 진행상황 출력
            if idx % 10 == 0 or idx == len(unique_addresses):
                print(f"   - 진행 중: {idx}/{len(unique_addresses)} 처리 완료...", end='\r')

        except Exception as e:
            print(f"❌ [Error] 처리 중 오류 ({sigungu}-{bjdong}-{bun}-{ji}): {e}")
            result_row = row.copy()
            result_row['판정코드'] = 4
            result_row['판정의미'] = f"Error: {str(e)}"
            classified_data[4].append(result_row)
            stats[4] += 1

    print(f"\n✅ 판정 완료! 총 {total_processed}건 처리됨.")
    print("-" * 60)

    # 3. 결과 파일 저장
    base_headers = fieldnames + ['판정코드', '판정의미']

    for code, filename in output_files.items():
        data_list = classified_data[code]
        count = len(data_list)
        
        if count > 0:
            with open(filename, mode='w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=base_headers)
                writer.writeheader()
                writer.writerows(data_list)
            print(f"💾 저장 완료: {filename} ({count}건)")
        else:
            print(f"ℹ️ 데이터 없음 (생성 안함): {filename}")

    # 최종 요약
    print("-" * 60)
    print("📊 [최종 결과 요약]")
    print(f"  • [0] 부적합: {stats[0]}건")
    print(f"  • [1] 적합 가능성 낮음: {stats[1]}건")
    print(f"  • [2] 적합 가능성 있음: {stats[2]}건")
    print(f"  • [3] 적합 가능성 높음: {stats[3]}건")
    print(f"  • [4] 판단 보류/기타: {stats[4]}건")
    print("-" * 60)


def get_result_description(code: int) -> str:
    desc_map = {
        0: "에어비앤비 부적합",
        1: "에어비앤비 적합 가능성 낮음",
        2: "에어비앤비 적합 가능성 있음",
        3: "에어비앤비 적합 가능성 높음",
        4: "에어비앤비 적합 판단 보류"
    }
    return desc_map.get(code, "알 수 없음")


if __name__ == "__main__":
    # 1. API 키 설정 (환경변수 또는 하드코딩)
    service_key = os.getenv("MY_SERVICE_KEY")
    if not service_key:
        raise ValueError(".env 파일에 MY_SERVICE_KEY를 설정해주세요.")
    
    # 2. 입력 파일명 (업로드된 파일명 사용)
    input_csv_file = "bondong.csv"
    
    # 3. 프로세스 실행
    process_and_split_csv(input_csv_file, service_key)