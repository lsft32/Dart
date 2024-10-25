import subprocess
import pandas as pd
import time

# 각 파일을 동시에 실행하기
file_paths = ['C:/WTF/git/newnew.py', 'C:/WTF/git/newnew2.py', 'C:/WTF/git/newnew3.py']
processes = [subprocess.Popen(['python', file_path]) for file_path in file_paths]

# 모든 프로세스가 완료될 때까지 대기
for process in processes:
    process.wait()

# 각 파일에서 생성한 중간 결과를 합쳐서 최종 정렬
combined_df = pd.read_csv('C:/CloudJYK/PER_TOP20_intermediate.csv')
sorted_result = combined_df.sort_values('PER', ascending=False)

# 최종 결과 저장
sorted_result.to_csv('C:/CloudJYK/PER_TOP20_final.csv', index=False, encoding="utf-8-sig")
