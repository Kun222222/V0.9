import csv
import pandas as pd
from tabulate import tabulate
import os
import glob
from datetime import datetime

# crosskimp/results 폴더에서 가장 최신 CSV 파일 찾기
test_dir = '/Users/kun/Desktop/CrossKimpArbitrage/v0.6/tests/crosskimp/results'
csv_files = glob.glob(os.path.join(test_dir, 'kimp_backtest_results_*.csv'))

if not csv_files:
    print("백테스트 결과 파일을 찾을 수 없습니다.")
    exit(1)

# 파일 수정 시간 기준으로 가장 최신 파일 선택
latest_file = max(csv_files, key=os.path.getmtime)
print(f"가장 최신 백테스트 결과 파일: {latest_file}")

# CSV 파일 읽기
results = []

with open(latest_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # 모든 필드가 있는지 확인
        required_fields = ['transfer_coin', 'profit_coin', 'transfer_premium', 'profit_premium', 
                          'net_profit', 'transfer_pair', 'profit_pair']
        if not all(field in row for field in required_fields):
            continue
            
        # 데이터 추가
        entry = {
            'transfer_coin': row['transfer_coin'],
            'profit_coin': row['profit_coin'],
            'coin_combination': f"{row['transfer_coin']}-{row['profit_coin']}",
            'transfer_pair': row['transfer_pair'],
            'profit_pair': row['profit_pair'],
            'transfer_premium': float(row['transfer_premium']) * 100,
            'profit_premium': float(row['profit_premium']) * 100,
            'net_profit': float(row['net_profit']) * 100,
            'total_efficiency': float(row.get('total_efficiency', '0')) * 100,
            'usdt_krw': float(row.get('usdt_krw', 0))
        }
        
        # 가격 정보 추가 (있는 경우만)
        if 'dom_transfer_buy' in row and row['dom_transfer_buy']:
            entry['dom_transfer_buy'] = float(row['dom_transfer_buy'])
        else:
            entry['dom_transfer_buy'] = 0
            
        if 'for_transfer_sell' in row and row['for_transfer_sell']:
            entry['for_transfer_sell'] = float(row['for_transfer_sell'])
        else:
            entry['for_transfer_sell'] = 0
            
        if 'for_profit_buy' in row and row['for_profit_buy']:
            entry['for_profit_buy'] = float(row['for_profit_buy'])
        else:
            entry['for_profit_buy'] = 0
            
        if 'dom_profit_sell' in row and row['dom_profit_sell']:
            entry['dom_profit_sell'] = float(row['dom_profit_sell'])
        else:
            entry['dom_profit_sell'] = 0
            
        # 수수료 정보 추가
        if 'total_fee' in row and row['total_fee']:
            entry['total_fee'] = float(row['total_fee'])
        else:
            entry['total_fee'] = 0.42  # 기본 수수료 값
            
        # 순이익금 계산 (500만원 기준)
        initial_amount = 5000000  # 500만원
        profit_amount = initial_amount * (entry['total_efficiency'] / 100)
        entry['profit_amount'] = profit_amount - initial_amount
            
        results.append(entry)

# 순이익 기준으로 정렬
results.sort(key=lambda x: x['net_profit'], reverse=True)

# 표 형식 데이터 생성
table_data = []
for idx, r in enumerate(results, 1):
    # 환율을 이용한 원화 환산 계산
    for_transfer_sell_krw = r['for_transfer_sell'] * r['usdt_krw'] if r['for_transfer_sell'] > 0 else 0
    for_profit_buy_krw = r['for_profit_buy'] * r['usdt_krw'] if r['for_profit_buy'] > 0 else 0
    
    table_data.append([
        idx,
        r['coin_combination'],
        r['transfer_pair'],
        f"{r['dom_transfer_buy']:,.2f}원" if r['dom_transfer_buy'] > 0 else "N/A",
        f"{r['for_transfer_sell']:.6f}$ ({for_transfer_sell_krw:,.2f}원)" if r['for_transfer_sell'] > 0 else "N/A",
        f"{r['transfer_premium']:.2f}%",
        r['profit_pair'],
        f"{r['for_profit_buy']:.6f}$ ({for_profit_buy_krw:,.2f}원)" if r['for_profit_buy'] > 0 else "N/A",
        f"{r['dom_profit_sell']:,.2f}원" if r['dom_profit_sell'] > 0 else "N/A",
        f"{r['profit_premium']:.2f}%",
        f"{r['total_fee']:.2f}%",
        f"{r['profit_amount']:,.2f}원",
        f"{r['net_profit']:.2f}%",
        f"{r['usdt_krw']:,.2f}원"
    ])

# 표 헤더
headers = [
    '순위', '코인 조합', '전송 경로', '전송 국내 매수가', '전송 해외 매도가', 
    '전송 김프', '수익 경로', '수익 해외 매수가', '수익 국내 매도가', 
    '수익 김프', '총 수수료', '순이익금', '순이익률', 'USDT/KRW'
]

# 표 생성
table = tabulate(table_data, headers=headers, tablefmt='grid')

# 현재 시간으로 결과 파일 이름 생성
current_time = datetime.now().strftime("%y%m%d_%H%M%S")

# 결과 파일에 저장
output_file = os.path.join('/Users/kun/Desktop/CrossKimpArbitrage/v0.6/tests/crosskimp/results', f"kimp_profit_table_{current_time}.txt")
with open(output_file, 'w') as f:
    f.write('# 실제 수수료 적용 시 500만원 기준 김프 차익거래 결과\n\n')
    f.write(f"총 {len(results)}개 기회 발견\n\n")
    f.write(table)

print(f'결과 표가 {output_file}에 저장되었습니다.')

# HTML 표로도 저장
df = pd.DataFrame(table_data, columns=headers)
html_table = df.to_html(index=False)
html_output = os.path.join('/Users/kun/Desktop/CrossKimpArbitrage/v0.6/tests/crosskimp/results', f"kimp_profit_table_{current_time}.html")
with open(html_output, 'w') as f:
    f.write('<html><head><title>김프 차익거래 백테스트 결과</title>')
    f.write('<style>')
    f.write('table {border-collapse: collapse; width: 100%;} ')
    f.write('th, td {padding: 8px; text-align: left; border: 1px solid #ddd;} ')
    f.write('th {background-color: #f2f2f2;} ')
    f.write('tr:nth-child(even) {background-color: #f9f9f9;} ')
    f.write('tr:hover {background-color: #f5f5f5;} ')
    f.write('</style>')
    f.write('</head><body>')
    f.write(f'<h1>실제 수수료 적용 시 500만원 기준 김프 차익거래 결과</h1>')
    f.write(f'<p>총 {len(results)}개 기회 발견</p>')
    f.write(html_table)
    f.write('</body></html>')

print(f'HTML 표도 {html_output}에 저장되었습니다.') 