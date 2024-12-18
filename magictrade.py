import pandas as pd

# 1. PER_list, ROA_list 불러오기
PER_list = pd.read_csv('C:/WTF/PER.csv')
ROA_list = pd.read_csv('C:/WTF/ROA.csv')


# 2. 열 이름 다시 정하기 (순위 -> PER순위 ROA 순위)
PER_list.columns = ['corp_code','종목명', 'PER', '주가']
ROA_list.columns = ['corp_code','ROA','2023_자산총계']

PER_list['PER순위'] = PER_list['PER'].rank(method='min').astype(float)
ROA_list['ROA순위'] = ROA_list['ROA'].rank(method='min', ascending=False)

# PER_list.rename(columns={'순위':'PER순위'}, inplace = True)
# PER_list.rename(columns={'corp_name':'종목명'}, inplace = True)
# PER_list.rename(columns={'Close':'주가'}, inplace = True)
# ROA_list.rename(columns={'순위':'ROA순위'}, inplace = True)

# 3. corpcode 기준으로 병합하기
# 4. PER이나 ROA 없는 행 지우기

PEROA = pd.merge(PER_list,ROA_list, how='inner',on='corp_code')

# 5. 순위 합 열 만들기
PEROA['순위합'] = PEROA['PER순위'] + PEROA['ROA순위']

# 6. 순위합 기준으로 오름차순으로 정렬하기
PEROA['순위'] = PEROA['순위합'].rank(method='min').astype(float)
PEROA = PEROA.sort_values('순위')

# 7. 열 순서 지정
PEROA = PEROA[['순위','순위합','PER순위','ROA순위', '종목명', 'corp_code', 'PER', 'ROA', '2023_자산총계', '주가']]
PEROA.columns = ['순위','순위합','PER순위','ROA순위', '종목명', 'corp_code', 'PER', 'ROA', '자산총계', '주가']

# 7. CSV 파일로 저장하기

PEROA.to_csv('C:/WTF/MagicTrade.csv', index=False, encoding="utf-8-sig")
