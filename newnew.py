import aiohttp
import asyncio
import pandas as pd
import requests_cache
import FinanceDataReader as fdr
import datetime

# 캐시 설정
requests_cache.install_cache('dart_cache', expire_after=1800)

# API key 설정
crtfc_keys = [
    'fee1dd02086668bbca7e8b91f0fc7a6b15b0d52b', 
    'e5d7ed4120cc74ac5df3dbaa79e5f16edc09f80a',
    '15e5d18d0dc5e61c4c942b6833f1d45160a0badc'
]

# 회사 리스트 불러오기
corp_list = pd.read_csv('C:/CloudJYK/회사상세정보.csv')

# 회사 리스트를 3개로 분할
chunks = [
    corp_list.iloc[:1259],
    corp_list.iloc[1259:2518],
    corp_list.iloc[2518:]
]

# 현재 시간 구하기
now = datetime.datetime.now()
market_close_time = datetime.datetime(now.year, now.month, now.day, 15, 30)

# 장이 종료되기 전이면 어제 날짜로 설정
if now < market_close_time:
    today = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')
else:
    today = now.strftime('%Y%m%d')

# 동시 요청 수 제한 (최대 10개의 요청을 동시에 처리)
semaphore = asyncio.Semaphore(10)

# 비동기 요청 함수
async def fetch_financial_data(session: aiohttp.ClientSession, crtfc_key: str, corp_code: str, bsns_year: str, report_code: str, fs_div: str):
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
    params = {
        'crtfc_key': crtfc_key,
        'corp_code': corp_code,
        'bsns_year': bsns_year,
        'reprt_code': report_code,
        'fs_div': fs_div
    }
    await asyncio.sleep(0.4)
    async with semaphore:
        try:
            async with session.get(url, params=params) as response:
                return await response.json()
        except aiohttp.ClientConnectorError:
            print(f"Error connecting to {url}. Retrying...")
            return None

async def fetch_stock_data(session: aiohttp.ClientSession, crtfc_key: str, corp_code: str, bsns_year: str, report_code: str):
    url = 'https://opendart.fss.or.kr/api/stockTotqySttus.json'
    params = {
        'crtfc_key': crtfc_key,
        'corp_code': corp_code,
        'bsns_year': bsns_year,
        'reprt_code': report_code
    }
    await asyncio.sleep(0.4)
    async with semaphore:
        try:
            async with session.get(url, params=params) as response:
                return await response.json()
        except aiohttp.ClientConnectorError:
            print(f"Error connecting to {url}. Retrying...")
            return None

# 데이터 수집 함수
async def collect_data(corp_list_chunk, crtfc_key, bsns_year='2023', report_code='11011', fs_div='CFS'):
    result_all = pd.DataFrame()
    stock_result_all = pd.DataFrame()

    async with aiohttp.ClientSession() as session:
        financial_tasks = []
        stock_tasks = []
        
        for _, r in corp_list_chunk.iterrows():
            corp_code = str(r['corp_code']).zfill(8)
            
            financial_task = fetch_financial_data(session, crtfc_key, corp_code, bsns_year, report_code, fs_div)
            stock_task = fetch_stock_data(session, crtfc_key, corp_code, bsns_year, report_code)
            
            financial_tasks.append(financial_task)
            stock_tasks.append(stock_task)
        
        # 재무 데이터 수집
        financial_responses = await asyncio.gather(*financial_tasks)
        for response in financial_responses:
            if response['status'] == '000':
                result_df = pd.DataFrame(response['list'])
                result_all = pd.concat([result_all, result_df])

        # 주식 데이터 수집
        stock_responses = await asyncio.gather(*stock_tasks)
        stock_result_list = []
        for response in stock_responses:
            if response['status'] == '000':
                for stock_item in response['list']:
                    if stock_item['se'] == '보통주':
                        stock_result_list.append({
                            'corp_code': response['list'][0]['corp_code'],
                            'istc_totqy': stock_item['istc_totqy']
                        })
        stock_result_all = pd.DataFrame(stock_result_list)

    return result_all, stock_result_all

# 병렬 실행
async def main():
    tasks = [
        collect_data(chunks[0], crtfc_keys[0]),
        collect_data(chunks[1], crtfc_keys[1]),
        collect_data(chunks[2], crtfc_keys[2])
    ]

    results = await asyncio.gather(*tasks)

    # 재무 데이터와 주식 데이터 병합
    financial_result = pd.concat([res[0] for res in results])
    stock_result = pd.concat([res[1] for res in results])

    # 당기순이익 필터링
    profit = financial_result.loc[financial_result['account_id'] == 'ifrs-full_ProfitLossAttributableToOwnersOfParent']
    profit = profit[['corp_code', 'thstrm_amount']]
    profit.columns = ['corp_code', '2023_당기순이익']
    profit = profit[profit['2023_당기순이익'] != '']

    # 주식 총수 데이터 병합
    stock_result['istc_totqy'] = stock_result['istc_totqy'].str.replace(',', '').astype('int64')
    merged_df = pd.merge(profit, stock_result, how='left', on='corp_code')

    # 주가 데이터 수집
    price_all = pd.DataFrame()
    for i, r in merged_df.iterrows():
        stock_code = str(r['corp_code']).zfill(6)
        price = fdr.DataReader(stock_code, today, today)[['Close']]
        price['corp_code'] = stock_code
        price_all = pd.concat([price_all, price])

    # 최종 병합
    final_df = pd.merge(merged_df, price_all, how='left', on='corp_code')

    # PER 계산
    final_df['2023_당기순이익'] = final_df['2023_당기순이익'].astype('int64')
    final_df['PER'] = final_df['Close'] * final_df['istc_totqy'] / final_df['2023_당기순이익']

    # 상위 20개 기업 저장
    final_result = final_df.loc[final_df['PER'] > 0].sort_values('PER').head(20)
    print(final_result)

    final_result.to_csv('C:/CloudJYK/PER_TOP20.csv', index=False, encoding="utf-8-sig")

# 비동기 루프 실행
asyncio.run(main())
