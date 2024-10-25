import io
import zipfile
import requests
from xml.etree.ElementTree import parse
import xml.etree.ElementTree as ET
import pandas as pd
import time
import datetime
import requests_cache

# requests_cache로 네트워크 호출 캐시 적용
requests_cache.install_cache('dart_cache_3', expire_after=1800)  # 30분 캐시 유지

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

    time.sleep(0.4)

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

profit = result_all.loc[(result_all['account_id'] == 'ifrs-full_ProfitLossAttributableToOwnersOfParent')]
profit = profit[['corp_code','thstrm_nm','thstrm_amount','frmtrm_nm', \
                 'frmtrm_amount','bfefrmtrm_nm','bfefrmtrm_amount','currency']]
profit.columns = ['corp_code','2023년','2023_당기순이익','2022년','2022_당기순이익', \
                  '2021년','2021_당기순이익','currency']

profit
#profit.to_csv('C:/CloudJYK/PER_list.csv', index = False, encoding="utf-8-sig")

#######################################################################################

#전체결과저장
result_stocks=[]

print('총 회사수는 : ' + str(corp_list.shape[0]))

# 수집한 회사에 대해서 for문.
for i, r in corp_list.iterrows():
    #if i == 100:
    #    break
    
    #없으면 어느 시점에서 에러발생
    time.sleep(0.4)
    
    #print('i = ' + str(i))
    corp_code=str(r['corp_code']).zfill(8)
    corp_name=r['corp_name']
    stock_code=str(r['stock_code'])
    bsns_year=2023
    reprt_code='11011' 
    #1분기보고서 : 11013, 반기보고서 : 11012, 3분기보고서 : 11014, 사업보고서 : 11011

    #print(corp_code)
    url = 'https://opendart.fss.or.kr/api/stockTotqySttus.json'
    params = {
        'crtfc_key': crtfc_key,
        'corp_code' : corp_code,
        'bsns_year' : str(bsns_year),
        'reprt_code' : reprt_code,
    }

    while True:
        try:
            results = requests.get(url, params=params).json()
            # 응답이 정상 '000' 일 경우에만 데이터 수집
            if results['status'] == '000':
                for result in results['list']:
                    if result['se'] in ['보통주','우선주','합계']:
                        result_dic={}
                        result_dic['se']=result['se']
                        result_dic['istc_totqy']=result['istc_totqy']
                        result_dic['corp_code']=corp_code
                        result_dic['corp_name']=corp_name
                        result_dic['stock_code']=stock_code
                        result_stocks.append(result_dic)  
            break
        except (requests.exceptions.RequestException, KeyError) as e:
            print(f"Request failed at index {i} with error: {e}. Retrying in 2 seconds...")
            time.sleep(2)    

stocks=pd.DataFrame(result_stocks)
stocks

df=pd.merge(left=profit,right=stocks,how='left',on='corp_code')
df1=df.loc[(df['se']=='보통주') & (df['currency']=='KRW')]
df1.loc[:, 'stock_code'] = df1['stock_code'].str.zfill(6)
df1

import matplotlib.pyplot as plt
import FinanceDataReader as fdr
import datetime

plt.rc('font', family='NanumGothic') 

#현재 시간 구하기
now = datetime.datetime.now()

# 장 종료 시간을 15:30으로 설정 (한국 주식 시장 기준)
market_close_time = datetime.datetime(now.year, now.month, now.day, 15, 30)

# 오늘 날짜가 토요일이나 일요일이면 가장 최근 금요일로 설정
if now.weekday() == 5:  # 토요일인 경우
    today = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')
elif now.weekday() == 6:  # 일요일인 경우
    today = (now - datetime.timedelta(days=2)).strftime('%Y%m%d')
else:
    # 평일인 경우, 장 종료 전이라면 어제 날짜로 설정
    if now < market_close_time:
        today = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')
    else:
        # 장이 종료된 이후라면 오늘 날짜로 설정
        today = now.strftime('%Y%m%d')

price_all=pd.DataFrame()
for i, r in df1.iterrows():
    stock_code = r['stock_code']
    
    
    #주가정보
    code=stock_code
    today=today
    price=fdr.DataReader(code,today,today)[['Close']]
    price['stock_code']=stock_code
    price_all = pd.concat([price_all,price])

df2=pd.merge(left=df1,right=price_all,how='left',on='stock_code')
df2

df2['istc_totqy']=df2['istc_totqy'].str.replace(',','').astype('int64')
df2=df2.loc[df2['2023_당기순이익']!='',]
df2=df2.astype({'2023_당기순이익':'int64'})
df2['2023_당기순이익'] = pd.to_numeric(df2['2023_당기순이익'], errors='coerce')
df2['PER'] = df2['Close'] * df2['istc_totqy'] / df2['2023_당기순이익']
df2

finalresult = pd.DataFrame(df2.loc[df2['PER']>0].sort_values('PER')[['corp_name','PER','Close']].iloc[:20,])

finalresult.to_csv('C:/WTF/PER_TOP20_intermediate_3.csv', mode='a', header=not bool(i), index=False, encoding="utf-8-sig")