import io
import zipfile
import requests
from xml.etree.ElementTree import parse
import xml.etree.ElementTree as ET
import pandas as pd
import time
import datetime

crtfc_key = 'fee1dd02086668bbca7e8b91f0fc7a6b15b0d52b'

path = 'C:/CloudJYK'
filename = '/corpcode.zip'

url = 'https://opendart.fss.or.kr/api/corpCode.xml'
params = {
    'crtfc_key' : crtfc_key
}
result = requests.get(url, params=params)

file = open(path+filename, 'wb')
file.write(result.content)
file.close()

zipfile.ZipFile(path+filename).extractall(path)

tree = parse(path + '/CORPCODE.xml')
root = tree.getroot()
li = root.findall('list')
corp_code, corp_name,stock_code, modify_date = [], [], [], []

####################################################################
# 각 회사를 순회하면서 corp_code에 여러 개의 코드가 있는 경우 처리
# for d in li:
#     corp_code_str = d.find('corp_code').text
#     corp_name_str = d.find('corp_name').text
#     stock_code_str = d.find('stock_code').text
#     modify_date_str = d.find('modify_date').text
    
#     # corp_code에 공백이 있으면 여러 개로 분리되어 있을 수 있음
#     corp_code_list = corp_code_str.split()  # 공백 기준으로 분리
    
#     for code in corp_code_list:
#         corp_code.append(code)
#         corp_name.append(corp_name_str)
#         stock_code.append(stock_code_str)
#         modify_date.append(modify_date_str)
##########################################################
# for d in li:
#     corp_code.append(d.find('corp_code').text)
#     corp_name.append(d.find('corp_name').text)
#     stock_code.append(d.find('stock_code').text)
#     modify_date.append(d.find('modify_date').text)
##################################################################
for d in li:
    # stock_code가 None이 아니고, 빈 문자열이 아닐 때만 추가
    stock_code_value = d.find('stock_code').text
    
    if stock_code_value and stock_code_value.strip():  # stock_code가 공백이 아닌 경우에만 처리
        corp_code.append(d.find('corp_code').text)
        corp_name.append(d.find('corp_name').text)
        stock_code.append(stock_code_value)
        modify_date.append(d.find('modify_date').text)
    
corps_df = pd.DataFrame({'corp_code':corp_code, 'corp_name':corp_name, 
                         'stock_code':stock_code, 'modify_date':modify_date})
corp_df = corps_df.loc[corps_df['stock_code'] !='',:].reset_index(drop=True)

result_all = []
corp_detail = pd.DataFrame()
corp_df.to_csv('C:/CloudJYK/corp_detail.csv', index = False, encoding="utf-8-sig")
print('총 회사 수는 : ' + str(corps_df.shape[0]))

for i, r in corps_df.iterrows():
    #if i == 2:
    #break

    #없으면 어느 시점에서 에러발생
    time.sleep(0.05)

    #print('i = ' + str(i))
    corp_code = str(r['corp_code'])
    corp_name = r['corp_name']

    url = 'https://opendart.fss.or.kr/api/company.json'
    params = {
        'crtfc_key' : crtfc_key, 
        'corp_code' : corp_code,
    }

    results = requests.get(url, params=params).json()

    if results['status'] == '000':
        result_all.append(results)

corp_detail = pd.DataFrame(result_all)

corp_detail.to_csv('C:/CloudJYK/회사상세정보.csv', index = False, encoding="utf-8-sig")

##---------------------------------------------###

# result_all = pd.DataFrame()

# crtfc_key = 'fee1dd02086668bbca7e8b91f0fc7a6b15b0d52b'
# bsns_year = '2023'
# report_code = '11011' #1분기보고서: 11013 반기보고서: 11012 3분기보고서: 11014 사업보고서: 11011
# fs_div = 'CFS'

# for i, r in corp_detail.iterrows():
#     #전체재무제표 요청인자
#     crtfc_key = crtfc_key
#     corp_code = str(r['corp_code']).zfill(8)

#     #if i == 100:
#     #break

#     time.sleep(0.3)

#     print(i)

#     url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
#     params = {
#         'crtfc_key': crtfc_key,
#         'corp_code' : corp_code,
#         'bsns_year' : bsns_year,
#         'reprt_code' : report_code,
#         'fs_div' : fs_div,
#     }
    
#     #결과를 json형태로 저장
#     result = requests.get(url, params=params).json()

#     if results['status'] == '000':
#         result_df = pd.DataFrame(results['list'])
#         result_all = pd.concat([result_all,result_df])

# profit = result_all.loc[(result_all['account_id'] == 'ifrs-full_ProfitLossAttributableToOwnersOfParent')]
# profit = profit[['corp_code','thstrm_nm','thstrm_amount','frmtrm_nm', \
#                  'frmtrm_amount','bfefrmtrm_nm','bfefrmtrm_amount','currency']]
# profit.columns = ['corp_code','2023년','2023_당기순이익','2022년','2022_당기순이익', \
#                   '2021년','2021_당기순이익','currency']
# profit
