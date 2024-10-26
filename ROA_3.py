import io
import zipfile
import requests
from xml.etree.ElementTree import parse
import xml.etree.ElementTree as ET
import pandas as pd
import time
import datetime
import requests_cache
import matplotlib.pyplot as plt
import FinanceDataReader as fdr
import datetime


# requests_cache로 네트워크 호출 캐시 적용
requests_cache.install_cache('dart_cache_2', expire_after=3600)  # 60분 캐시 유지

corp_list = pd.read_csv('C:/WTF/회사상세정보.csv')
corp_list = corp_list.iloc[2520:]

# 회사별 전체재무제표 확인
result_all = pd.DataFrame()

print((corp_list.shape[0]))


crtfc_key = '15e5d18d0dc5e61c4c942b6833f1d45160a0badc'
bsns_year = '2023'
report_code = '11011' #1분기보고서: 11013 반기보고서: 11012 3분기보고서: 11014 사업보고서: 11011
fs_div = 'CFS'

for i, r in corp_list.iterrows():
    #전체재무제표 요청인자
    crtfc_key = crtfc_key
    corp_code = str(r['corp_code']).zfill(8)

    time.sleep(0.5)

    #print(i)

    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
    params = {
        'crtfc_key': crtfc_key,
        'corp_code' : corp_code,
        'bsns_year' : bsns_year,
        'reprt_code' : report_code,
        'fs_div' : fs_div,
    }
    
# 요청을 성공할 때까지 재시도하는 루프
    while True:
        try:
            result = requests.get(url, params=params).json()
            if result['status'] == '000':
                result_df = pd.DataFrame(result['list'])
                result_all = pd.concat([result_all, result_df])
            break  # 성공하면 루프 탈출

        except (requests.exceptions.RequestException, KeyError) as e:
            print(f"Request failed at index {i} with error: {e}. Retrying in 2 seconds...")
            time.sleep(2)  # 실패 시 2초 대기 후 재시도

# header = i == 0    
# result_all.head().to_csv('C:/WTF/example(result).csv', mode='a', header=header, index=False, encoding="utf-8-sig")
# pd.Series(result_all.columns).to_csv('C:/WTF/example(columns).csv', mode='a', header=header, index=False, encoding="utf-8-sig")


profit = result_all.loc[(result_all['account_id'] == 'ifrs-full_ProfitLossAttributableToOwnersOfParent') & (result_all['currency']=='KRW')]
profit = profit[['corp_code','thstrm_nm','thstrm_amount','frmtrm_nm', \
                 'frmtrm_amount','bfefrmtrm_nm','bfefrmtrm_amount','currency']]
profit.columns = ['corp_code','2023년','2023_당기순이익','2022년','2022_당기순이익', \
                  '2021년','2021_당기순이익','currency']

profit



print(str(corp_list.shape[0]) + ('개 회사의 자산 확인 중...'))

asset = result_all.loc[(result_all['account_id'] == 'ifrs-full_Assets') & (result_all['currency']=='KRW')]
asset = asset[['corp_code','thstrm_nm','thstrm_amount','frmtrm_nm','frmtrm_amount']]
asset.columns = ['corp_code','2023년','2023_자산총계','2022년','2022_자산총계']
asset



firstep=pd.merge(left=asset,right=profit,how='left',on='corp_code')
firstep=firstep.loc[(firstep['2023_당기순이익'].notnull()) & (firstep['2023_자산총계'].notnull())]
firstep['2023_당기순이익'] = pd.to_numeric(firstep['2023_당기순이익'], errors='coerce')
firstep['2023_자산총계'] = pd.to_numeric(firstep['2023_자산총계'], errors='coerce')
firstep['ROA'] = firstep['2023_당기순이익'] / firstep['2023_자산총계'] * 100
firstep

ROA_result = pd.DataFrame(firstep.sort_values('ROA', ascending=False)[['corp_code','ROA','2023_자산총계']])

# 첫 번째 루프에서는 헤더를 추가, 이후는 헤더 없이 데이터 추가
header = i == 0
ROA_result.to_csv('C:/WTF/ROA.csv', mode='a', header=header, index=False, encoding="utf-8-sig")

# 1. CSV 파일을 불러오되, 첫 번째 행을 데이터로 사용
df = pd.read_csv('C:/WTF/ROA.csv', encoding="utf-8-sig")

# 2. 열 이름을 수동으로 지정 (예: '종목명', 'ROA', '주가')
df.columns = ['corp_code','ROA','2023_자산총계']

# 3. 중복된 행 제거 (종목명, ROA, 주가가 동일한 행을 하나로 합침)
df = df.drop_duplicates(subset=['corp_code', 'ROA', '2023_자산총계'])

# 4. ROA 값을 기준으로 내림차순 정렬
df_sorted = df.sort_values(by='ROA', ascending=False)

# 5. 순위를 나타내는 열 추가 (1부터 시작하는 순위)
df_sorted['순위'] = df_sorted['ROA'].rank(method='min', ascending=False)

# NaN 값을 0으로 대체 (원하는 대체값을 넣을 수 있습니다)
df_sorted['순위'] = df_sorted['순위'].fillna(0).astype(int)

# 6. 열 순서 지정 (순위, 종목명, ROA, 주가 순으로)
df_sorted = df_sorted[['corp_code', 'ROA', '2023_자산총계', '순위']]

# 7. 정렬된 데이터를 다시 CSV 파일로 저장
df_sorted.to_csv('C:/WTF/ROA_sorted.csv', index=False, encoding="utf-8-sig")
