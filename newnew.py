import io
import zipfile
import asyncio
import aiohttp
import pandas as pd
import time
import datetime
import random

# API 키
crtfc_keys = [
    'fee1dd02086668bbca7e8b91f0fc7a6b15b0d52b',
    'e5d7ed4120cc74ac5df3dbaa79e5f16edc09f80a',
    '15e5d18d0dc5e61c4c942b6833f1d45160a0badc'
]

# 회사 리스트 불러오기
corp_list = pd.read_csv('C:/WTF/회사상세정보.csv')

# 세마포어를 설정하여 동시 연결 수를 제한합니다.
semaphore = asyncio.Semaphore(10)  # 동시 요청 수를 10개로 제한

async def fetch_financial_data(session, url, params, retries=3):
    async with semaphore:  # 세마포어 사용
        for attempt in range(retries):
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        print(f"Failed with status {response.status}. Retrying...")
            except aiohttp.ClientDisconnectedError:
                print("Server disconnected. Retrying...")
                await asyncio.sleep(random.uniform(1, 3))  # 재시도 전에 지연 시간 추가
        return None  # 여러 번 시도해도 실패한 경우 None 반환

# 각 API 키로 수집할 회사 범위 설정
chunks = [corp_list.iloc[1:1260], corp_list.iloc[1260:2520], corp_list.iloc[2520:]]
result_all = pd.DataFrame()

async def gather_data(corp_chunk, crtfc_key):
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
    bsns_year = '2023'
    report_code = '11011' # 사업보고서 코드
    fs_div = 'CFS'
    
    results = []
    timeout = aiohttp.ClientTimeout(total=10)  # 10초 타임아웃 설정
    async with aiohttp.ClientSession(timeout=timeout) as session:

        tasks = []
        for _, r in corp_chunk.iterrows():
            corp_code = str(r['corp_code']).zfill(8)
            params = {
                'crtfc_key': crtfc_key,
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': report_code,
                'fs_div': fs_div,
            }
            
            task = fetch_financial_data(session, url, params)
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        
        # 응답 처리
        for result in responses:
            await asyncio.sleep(0.4)
            if result['status'] == '000':
                result_df = pd.DataFrame(result['list'])
                results.append(result_df)
    
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

async def main():
    global result_all
    tasks = [gather_data(chunk, crtfc_key) for chunk, crtfc_key in zip(chunks, crtfc_keys)]
    results = await asyncio.gather(*tasks)
    result_all = pd.concat(results, ignore_index=True)

# 비동기 이벤트 루프 실행
asyncio.run(main())

# 필요한 데이터 추출
profit = result_all.loc[(result_all['account_id'] == 'ifrs-full_ProfitLossAttributableToOwnersOfParent')]
profit = profit[['corp_code','thstrm_nm','thstrm_amount','frmtrm_nm', \
                 'frmtrm_amount','bfefrmtrm_nm','bfefrmtrm_amount','currency']]
profit.columns = ['corp_code','2023년','2023_당기순이익','2022년','2022_당기순이익', \
                  '2021년','2021_당기순이익','currency']

# 이후의 코드도 profit 데이터프레임을 활용해 계속 진행

import matplotlib.pyplot as plt
import FinanceDataReader as fdr
import datetime

# 비동기 수집 후의 데이터 처리
profit = result_all.loc[(result_all['account_id'] == 'ifrs-full_ProfitLossAttributableToOwnersOfParent')]
profit = profit[['corp_code','thstrm_nm','thstrm_amount','frmtrm_nm', \
                 'frmtrm_amount','bfefrmtrm_nm','bfefrmtrm_amount','currency']]
profit.columns = ['corp_code','2023년','2023_당기순이익','2022년','2022_당기순이익', \
                  '2021년','2021_당기순이익','currency']

# 전체 결과 저장
result_stocks = []

async def fetch_stock_data(session, url, params, retries=3):
    async with semaphore:  # 세마포어 사용
        for attempt in range(retries):
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        print(f"Failed with status {response.status}. Retrying...")
            except aiohttp.ClientDisconnectedError:
                print("Server disconnected. Retrying...")
                await asyncio.sleep(random.uniform(1, 3))  # 재시도 전에 지연 시간 추가
        return None  # 여러 번 시도해도 실패한 경우 None 반환

async def gather_stock_data(corp_chunk, crtfc_key):
    url = 'https://opendart.fss.or.kr/api/stockTotqySttus.json'
    bsns_year = '2023'
    reprt_code = '11011'
    results = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, r in corp_chunk.iterrows():
            corp_code = str(r['corp_code']).zfill(8)
            corp_name = r['corp_name']
            stock_code = str(r['stock_code']).zfill(6)
            params = {
                'crtfc_key': crtfc_key,
                'corp_code': corp_code,
                'bsns_year': str(bsns_year),
                'reprt_code': reprt_code,
            }
            task = fetch_stock_data(session, url, params)
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        
        for result, r in zip(responses, corp_chunk.iterrows()):
            await asyncio.sleep(0.4)
            if result['status'] == '000':
                for item in result['list']:
                    if item['se'] in ['보통주', '우선주', '합계']:
                        result_dic = {
                            'se': item['se'],
                            'istc_totqy': int(item['istc_totqy'].replace(',', '')),
                            'corp_code': r[1]['corp_code'],
                            'corp_name': r[1]['corp_name'],
                            'stock_code': stock_code
                        }
                        results.append(result_dic)

    return pd.DataFrame(results)

async def main_stock_data():
    global result_stocks
    tasks = [gather_stock_data(chunk, crtfc_key) for chunk, crtfc_key in zip(chunks, crtfc_keys)]
    results = await asyncio.gather(*tasks)
    result_stocks = pd.concat(results, ignore_index=True)

asyncio.run(main_stock_data())

# Merge stock data with financial data
stocks = pd.DataFrame(result_stocks)
df = pd.merge(left=profit, right=stocks, how='left', on='corp_code')
df1 = df.loc[(df['se'] == '보통주') & (df['currency'] == 'KRW')]
df1['stock_code'] = df1['stock_code'].str.zfill(6)

# 현재 날짜 계산 (주가 정보를 불러오기 위해)
now = datetime.datetime.now()
market_close_time = datetime.datetime(now.year, now.month, now.day, 15, 30)
today = (now - datetime.timedelta(days=1)).strftime('%Y%m%d') if now < market_close_time else now.strftime('%Y%m%d')

price_all = pd.DataFrame()
for _, r in df1.iterrows():
    stock_code = r['stock_code']
    price = fdr.DataReader(stock_code, today, today)[['Close']]
    price['stock_code'] = stock_code
    price_all = pd.concat([price_all, price])

df2 = pd.merge(left=df1, right=price_all, how='left', on='stock_code')

# PER 계산
df2['PER'] = df2['Close'] * df2['istc_totqy'] / df2['2021_당기순이익']
df2 = df2.loc[df2['2023_당기순이익'] != '', ]
df2 = df2.astype({'2023_당기순이익': 'int64'})

# 상위 20개 PER 결과
finalresult = df2[df2['PER'] > 0].sort_values('PER').iloc[:20][['corp_name', 'PER', 'Close']]
finalresult.to_csv('C:/WTF/PER_TOP20.csv', index=False, encoding="utf-8-sig")

print("Top 20 PER 기업 데이터를 'PER_TOP20.csv' 파일에 저장 완료")
