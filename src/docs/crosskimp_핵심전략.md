# 교차 김치 프리미엄 차익거래 실시간 계산 로직 설계

## 전략 개요

교차 **김치 프리미엄** 차익거래는 국내 거래소와 해외 거래소 간의 가격 차이를 이용하여 **무위험 수익**을 얻는 전략입니다. 이 전략에서는 두 종류의 암호화폐를 사용합니다:
- **전송코인**: 국내에서 해외로 가치 이동에 사용
- **수익코인**: 해외에서 국내로 가져와 프리미엄을 실현

두 코인의 흐름을 분리하여 각각 헤지(hedge)함으로써 가격 변동 위험을 최소화합니다. 각 단계에서 필요한 KRW와 USDT는 미리 준비되어 있어 **환전 과정 없이** 진행됩니다. 모든 가격은 실시간 **USDT/KRW 환율**을 기준으로 KRW 값으로 통일하여 비교합니다.

---

## 전송 코인 경로: 국내 → 해외 자금 이동 (KRW → USDT)

전송코인 경로는 **국내 자산을 해외 자산(USDT)**으로 바꾸는 흐름입니다. 국내 거래소에서 전송코인을 매수하고 동시에 해외 거래소에서 해당 전송코인을 마진대출하여 매도하면서 가격 변동을 헤지합니다. 이후 해외로 전송 된 전송코인을 통해 대출 상환합니다.

> **예시:** 전송코인으로 XRP를 선택했다면 업비트에서 XRP를 매수한 뒤 바이낸스로 전송하고, 전송되는 동안 바이낸스에서 같은 양의 XRP를 미리 대출(마진 숏)하여 매도합니다.

### 주요 단계

1. **국내 전송코인 매수:**
   - 국내 거래소(업비트 또는 빗썸)에서 전송코인을 시장가로 매수
   - 예: 10,000만 원 상당의 XRP 매수
   - **국내 매수 수수료**(0.05~0.1%) 발생
   - **출금 수수료** 고려하여 여유 있게 추가 매수 (예: XRP 출금 수수료 10 XRP)
   - **거래소 설정파일** 에 각 거래소 수수료, 전송 수수료(코인) 설정 예정
   - 만약 거래소에서 API를 통해 전송 코인 수수료를 수신 할 수 있다면 활용

2. **해외 거래소에서 전송코인 공매도(헤지):**
   - 국내 매수 직후, **해외 거래소(바이낸스/바이빗)**에서 동일 수량을 **USDT 담보 마진 거래로 대출 후 즉시 매도**
   - 예: 업비트에서 15,000 XRP 매수/출금 → 바이낸스에서 15,000 XRP 대출하여 매도
   - **해외 매도 수수료**(~0.1%) 발생
   - 레버리지 활용 가능하나, 충분한 증거금 USDT 유지 필요

3. **코인 전송 및 대출 상환:**
   - 국내에서 전송한 코인이 **해외 거래소 지갑에 도착**하면 공매도 포지션 청산
   - **반환**(마진 대출 상환)으로 숏 포지션 종료
   - 10~20분 이내 단기 대출로 **이자 비용 무시**
   - 가격 변동 위험 헤지 완료

### 전송코인 경로 수익/비용 계산

전송코인 경로를 통해 **KRW → USDT 환전**이 이뤄집니다. 효과적인 환율과 비용 계산을 위한 **전송코인 경로 수익률**을 다음과 같이 정의합니다:

> 💡 **핵심 공식**: 전송코인 경로 수익률 RT
> 
> **RT = (해외에서 확보한 USDT) ÷ (국내에서 사용한 KRW)**
> 
> 👉 전송코인 경로에서 **국내 1 KRW로 얻은 USDT 비율**을 나타냅니다.

#### 이상적인 환율과 실제 수익률 비교

* **이상적인 기준환율**: 1 KRW = (1/E) USDT
  * 여기서 E는 실시간 USDT/KRW 환율
  
* **실제 RT 값**: 각종 비용과 가격 차이를 반영하여 이상적 환율과 차이가 발생
  * RT > (1/E) : 전송코인 경로가 직접 환전보다 유리 (추가 이익)
  * RT < (1/E) : 전송코인 경로가 직접 환전보다 불리 (비용 발생)

#### RT 계산 시 고려 요소:

| 요소 | 설명 |
|------|------|
| **국내 매수가격** | 전송코인 국내 매수 평균단가 (KRW) |
| **국내 수수료** | 매수금액의 ~0.05% (실질 지출 KRW 증가) |
| **해외 매도가격** | 전송코인 해외 공매도 가격 (USDT) |
| **해외 수수료** | 매도금액의 ~0.1% (실질 확보 USDT 감소) |
| **출금 수수료** | 전송코인 출금 시 차감되는 코인 수량 |

이 요소들을 반영하여 RT를 계산하면 **전송코인 경로의 순환환율**을 얻습니다.

> **예시 계산:** 10,000,000 KRW로 XRP를 매수/전송하여 해외에서 7,700 USDT를 확보했다면 RT = 7,700 ÷ 10,000,000 ≈ 0.00077 USDT/KRW입니다. 이는 약 1298 KRW/USDT에 해당합니다.

RT가 (1/E)보다 높으면(전송코인 경로가 직접 환전 대비 이득) 추가 수익에 기여하며, 낮으면 비용으로 작용합니다.

### 전송코인 경로 리스크

이 경로는 **국내 자산→해외 자산 이동**이 목적이므로, 가격 변동 리스크는 공매도로 대부분 제거됩니다. 남은 리스크:
- 네트워크 전송 지연이나 실패
- **전송코인 가격의 국내-해외 가격차 변화**
- 레버리지 사용 시 마진콜 가능성 (단, 충분한 증거금과 짧은 시간으로 위험 통제)

요약하면, 전송코인 경로의 주요 비용은 수수료 형태로 고정되고, 가격 위험은 헤지로 제거되어 **환율 효율(RT)**만이 이익에 영향을 줍니다.

---

## 수익 코인 경로: 해외 → 국내 자금 이동 (USDT → KRW)

수익코인 경로는 해외의 USDT를 국내 KRW로 **회수하면서 프리미엄 차익**을 실현하는 흐름입니다. 해외 거래소에서 수익코인을 매수하여 국내로 송금하고, 도착한 코인을 국내 거래소에서 매도함으로써 국내 프리미엄을 얻습니다.

> **예시:** 수익코인으로 비트코인(BTC)을 선택했다면, 바이낸스에서 BTC를 USDT로 매수한 후 바로 동일 수량의 BTC를 선물시장에 숏하고, BTC를 국내로 전송합니다.

### 주요 단계

1. **해외 수익코인 매수:**
   - 해외 거래소의 현물마켓에서 수익코인을 USDT로 시장가 매수
   - 예: 7,700 USDT로 BTC 매수
   - **해외 매수 수수료**(~0.1%) 발생
   - 출금 수수료 고려하여 추가 매수 (예: BTC 출금 수수료 0.0005 BTC)

2. **해외 선물 숏 헤지:**
   - 매수 직후 **해외 선물 거래소**에서 동일 수량 **선물 숏 포지션** 구축
   - 예: 매수한 0.2 BTC에 대해 0.2 BTC의 perpetual 선물 숏 오픈
   - 현물-선물 가격 변동 상쇄로 헤지 효과
   - **선물 거래 수수료**(왕복 ~0.04-0.1%) 발생
   - 짧은 보유로 **펀딩비 등의 비용은 무시** 가능

3. **코인 전송:**
   - 해외에서 매수한 수익코인을 국내 거래소 지갑으로 **출금 전송**
   - 예: 0.2005 BTC 출금 → 수수료 0.0005 BTC 제외 → **0.20 BTC 국내 도착**
   - 전송 시간은 약 20분 가정

4. **국내 수익코인 매도 및 헤지 마무리:**
   - 수익코인 도착 즉시 **시장가 매도** 실행
   - 국내 KRW 마켓에 판매하여 현금 확보
   - **국내 매도 수수료**(~0.05-0.1%) 차감
   - 해외 선물 숏 포지션 **매수 청산**으로 종료
   - 결과적으로 해외-국내 가격차(김치 프리미엄)만 실현

### 수익코인 경로 수익/비용 계산

수익코인 경로는 **USDT → KRW 환전**에 해당하며, **수익코인 경로 수익률** RP를 다음과 같이 정의합니다:

> 💡 **핵심 공식**: 수익코인 경로 수익률 RP
> 
> **RP = (국내에서 회수한 KRW) ÷ (해외에서 사용한 USDT)**
> 
> 👉 해외 1 USDT를 투입하여 국내에서 얻은 KRW 비율입니다.

해외 1 USDT를 투입하여 국내에서 얻은 KRW 비율입니다. 기본 환율은 1 USDT = E KRW이나, 김치 프리미엄이 있다면 RP는 E보다 높게 형성됩니다.

#### RP 계산 시 고려 요소:

| 요소 | 설명 |
|------|------|
| **해외 매수가격** | 수익코인 해외 매수 평균단가 (USDT) |
| **해외 수수료** | 매수 금액의 ~0.1% (실제 코인 수량 감소) |
| **출금 수수료** | 해외 코인 출금 시 차감되는 코인 수량 |
| **국내 매도가격** | 수익코인 국내 매도 평균단가 (KRW) |
| **국내 수수료** | 매도 금액의 ~0.05% (최종 KRW 감소) |

> **예시 계산:** 해외에서 7,700 USDT로 BTC를 구매/전송하여 국내에서 10,050만 KRW를 확보했다면, RP = 10,050만 ÷ 7,700 ≈ 1305.19 KRW/USDT가 됩니다.

이 값이 실시간 환율 E보다 높으면 프리미엄 차익이 있다는 뜻입니다. 두 코인 경로 완료 시 **최종 이익은 RT와 RP의 곱으로 결정**됩니다.

### 수익코인 경로 리스크

수익코인 경로에서도 **가격 변동 위험은 선물 숏 헤지**로 대부분 제거됩니다. 남은 주요 위험:
- **국내 프리미엄 변동** (전송 중 프리미엄 하락 시 이익 감소)
- 선물 헤지는 글로벌 가격 변동만 커버, 한국 시장 프리미엄 변동은 헤지 불가
- 전송 지연 또는 입출금 지연

리스크 완화 방법:
- **최소 수익률 요건**(0.1%) 설정으로 안전마진 확보
- 전송속도 빠른 코인/네트워크 선택
- **충분한 자본 버퍼** 유지

---

## 수익률 계산 및 환율 통일 기준

두 경로 합산 시 **KRW → USDT → KRW** 순환이 완성되며, 김치 프리미엄에 따른 차익이 발생합니다.

### 통합 수익률 계산

- **통합 수익률 (배수)** = RT × RP
- **통합 이익률 (%)** = (RT × RP - 1) × 100%

여기서 RT = (USDT_out) ÷ (KRW_in), RP = (KRW_out) ÷ (USDT_in)이며, 이상적으로 USDT_out = USDT_in일 때 성립합니다.

### USDT/KRW 환율 통일

실시간으로 **업비트/빗썸의 USDT-KRW 마켓 가격**을 환율 E로 사용하여 국내외 가격을 같은 기준으로 비교합니다.

#### 간단 검증 방법
- **수익코인 국내가격** vs **수익코인 해외가격×E** 비교
- **전송코인 국내가격** vs **전송코인 해외가격×E** 비교

> **예시:**
> - BTC 국내가격 = 40,000,000 KRW, BTC 해외가격 = 30,000 USDT, E = 1300 KRW/USDT
> - BTC의 KRW환산 해외가격 = 39,000,000 KRW → **김치 프리미엄 약 2.56%**
> - XRP 국내가격 = 500 KRW, XRP 해외가격 = 0.38 USDT
> - XRP KRW환산 해외가격 = 494 KRW → **국내 XRP 프리미엄 1.2%**
> - 실제 계산: RT ≈ (0.38 × (1-0.001)) ÷ (500 × (1+0.001)), RP ≈ (40,000,000 × (1-0.0005)) ÷ (30,000 × (1+0.001))
> - 두 값을 곱한 후 -1 하면 수익률% 산출

### 수익률 기준

실시간 계산을 통해 **예상 수익률이 0.1% (0.001 배) 이상**이면 해당 **교차 차익거래 조합을 실행**합니다. 0.1% 미만은 실익이 없어 트리거하지 않습니다.

---

## 거래 경로별 비용 요소 및 리스크 요약

### 전송코인 경로 (국내 → 해외 환전)

#### 절차
국내 KRW로 전송코인 매수 → 코인 해외송금 → 해외에서 동일 코인 공매도 후 도착 시 상환.

#### 비용 요소
- 국내 매수 수수료 (KRW 기반, ~0.05-0.1%)
- 해외 매도 수수료 (USDT 기반, ~0.1%)
- 출금 수수료 (코인 수량 고정)
- 숨은 비용: 호가 스프레드 및 슬리피지

#### 성과 지표
RT (전송환율 효율) – 국내 1 KRW로 얻은 USDT 비율

#### 리스크
- 전송 지연/실패
- 국내-해외 해당 코인 가격차 변화
- 유동성 부족으로 인한 슬리피지
- 전체적으로 변동성 위험은 낮음

### 수익코인 경로 (해외 → 국내 환전)

#### 절차
해외 USDT로 수익코인 매수 → 수익코인 해외송금 → 국내 도착 즉시 매도 (KRW 획득) → 해외 선물 숏 포지션 청산.

#### 비용 요소
- 해외 매수 수수료 (USDT 기반, ~0.1%)
- 해외 선물 거래 수수료 (왕복 0.04-0.1%)
- 출금 수수료 (코인 수량 고정)
- 국내 매도 수수료 (KRW 기반, ~0.05%)
- 추가 비용: 호가 스프레드 및 슬리피지

#### 성과 지표
RP (프리미엄 환전 효율) – 해외 1 USDT로 얻은 KRW 비율

#### 리스크
- 국내 프리미엄 변동 위험
- 시장 임팩트 위험 (동시 다량 실행 시)
- 전송 지연 시 선물 포지션 보유 시간 증가
- 증거금 관리 필요

---

## 실시간 계산 시뮬레이션 예시

본 섹션에서는 인덱스 기반 하이브리드 구조를 활용한 실시간 김프율 계산과 거래 의사결정 과정을 시뮬레이션 예시로 설명합니다. 효율적인 데이터 처리와 최적 거래 기회 포착 과정을 단계별로 제시합니다.

### 초기 시스템 상태

시스템은 다음과 같은 상태로 초기화되어 있다고 가정합니다:

1. **거래소 설정**
   - 국내: 빗썸, 업비트
   - 해외: 바이낸스, 바이빗

2. **코인 목록**
   - 트래킹 코인: BTC, ETH, XRP, DOGE, SOL, SUI, MATIC, AVAX, LAYER 등
   - 거래소별 지원 코인은 상이 (예: 업비트는 LAYER 지원, 빗썸은 미지원)

3. **인덱스 기반 데이터 구조**
   ```
   # 인덱스 매핑
   coin_to_idx = {'BTC': 0, 'ETH': 1, 'XRP': 2, 'DOGE': 3, ...}
   dom_to_idx = {'빗썸': 0, '업비트': 1}
   for_to_idx = {'바이낸스': 0, '바이빗': 1}
   
   # 김프율 배열 (3차원: 코인×국내거래소×해외거래소)
   transfer_premiums = np.full((len(coins), 2, 2), np.nan)  # 전송코인 김프율
   profit_premiums = np.full((len(coins), 2, 2), np.nan)    # 수익코인 김프율
   
   # 유효 페어 마스크
   valid_pairs = np.zeros((len(coins), 2, 2), dtype=bool)
   ```

4. **초기 USDT/KRW 환율**
   - 환율 = 1,350원/USDT (빗썸/업비트 USDT 마켓 기준)

5. **거래 수수료**
   - 국내: 매수/매도 각 0.05%
   - 해외: 매수/매도 각 0.1%
   - 출금 수수료: 코인별 상이 (예: BTC 0.0005, XRP 0.25)

### 시뮬레이션 케이스 1: 바이낸스 SUI 오더북 업데이트

**1. 이벤트 수신**
```
수신 데이터: 바이낸스 SUI/USDT 오더북 업데이트
시간: 2023-04-01 15:30:25.123
매도호가(asks): [[2.29, 5000], [2.30, 8000], ...]  # [가격, 수량]
매수호가(bids): [[2.28, 4000], [2.27, 6000], ...]
```

**2. 관련 페어 찾기**
```python
# SUI 코인 인덱스
coin_idx = coin_to_idx['SUI']  # 예: 5
foreign_idx = for_to_idx['바이낸스']  # 0

# 유효한 국내 거래소 찾기
related_pairs = []
for dom_idx in range(len(domestic_exchanges)):
    if valid_pairs[coin_idx, dom_idx, foreign_idx]:
        related_pairs.append((coin_idx, dom_idx, foreign_idx))
# 결과: [(5, 0, 0), (5, 1, 0)]  # (SUI, 빗썸, 바이낸스), (SUI, 업비트, 바이낸스)
```

**3. 오더북 캐시 업데이트**
```python
# 오더북 캐시 업데이트
orderbooks['바이낸스']['SUI'] = {
    'asks': [[2.29, 5000], [2.30, 8000], ...],
    'bids': [[2.28, 4000], [2.27, 6000], ...],
    'timestamp': 1680359425123
}
```

**4. 관련 페어 김프율 계산**

**빗썸-바이낸스 SUI 페어 계산:**
```python
# 빗썸 SUI 오더북 데이터 조회
bithumb_sui = orderbooks['빗썸']['SUI']
# 시장가 계산 (1000만원어치 매수 또는 150 SUI 매도 가정)
domestic_buy_price = calculate_market_buy_price(bithumb_sui, 10_000_000)  # 예: 3,200원
domestic_sell_price = calculate_market_sell_price(bithumb_sui, 150)  # 예: 3,180원

# 바이낸스 SUI 시장가 계산 (150 SUI 매수 또는 매도 가정)
binance_sui = orderbooks['바이낸스']['SUI']
foreign_buy_price = calculate_market_buy_price(binance_sui, 150)  # 예: 2.29 USDT
foreign_sell_price = calculate_market_sell_price(binance_sui, 150)  # 예: 2.28 USDT

# 환율 적용 (1,350원/USDT)
foreign_buy_price_krw = foreign_buy_price * 1350  # 3,091.5원
foreign_sell_price_krw = foreign_sell_price * 1350  # 3,078원

# 김프율 계산
transfer_premium = (domestic_buy_price / foreign_sell_price_krw) - 1  # (3,200 / 3,078) - 1 = 3.96%
profit_premium = (domestic_sell_price / foreign_buy_price_krw) - 1  # (3,180 / 3,091.5) - 1 = 2.86%

# 배열에 저장
transfer_premiums[coin_idx, 0, 0] = transfer_premium  # SUI, 빗썸, 바이낸스
profit_premiums[coin_idx, 0, 0] = profit_premium
```

**업비트-바이낸스 SUI 페어도 동일 방식으로 계산**

**5. 최적 페어 갱신 검토**
```python
# 전송코인 최소값 확인 (마스킹 적용)
masked_transfer = np.where(valid_pairs, transfer_premiums, np.inf)
min_transfer_idx = np.unravel_index(np.argmin(masked_transfer), masked_transfer.shape)
min_transfer_premium = masked_transfer[min_transfer_idx]

# 기존 최적값과 비교
if min_transfer_premium < best_transfer['premium']:
    best_transfer['premium'] = min_transfer_premium
    best_transfer['indices'] = min_transfer_idx
    print(f"새로운 최적 전송코인: {idx_to_coin[min_transfer_idx[0]]} ({idx_to_dom[min_transfer_idx[1]]}→{idx_to_for[min_transfer_idx[2]]}), 프리미엄: {min_transfer_premium*100:.2f}%")

# 수익코인도 유사한 방식으로 최대값 탐색 및 갱신
```

**6. 거래 기회 평가**
```python
# 최적 전송코인 및 수익코인 조합의 기대 수익률 계산
expected_return = best_profit['premium'] - best_transfer['premium'] - total_fee_rate
if expected_return >= 0.001:  # 0.1% 이상
    # 거래 트리거
    print(f"거래 기회 발견! 수익률: {expected_return*100:.2f}%")
    print(f"전송코인: {idx_to_coin[best_transfer['indices'][0]]} ({idx_to_dom[best_transfer['indices'][1]]}→{idx_to_for[best_transfer['indices'][2]]})")
    print(f"수익코인: {idx_to_coin[best_profit['indices'][0]]} ({idx_to_for[best_profit['indices'][2]]}→{idx_to_dom[best_profit['indices'][1]]})")
```

### 시뮬레이션 케이스 2: USDT/KRW 환율 변동

**1. 이벤트 수신**
```
수신 데이터: 빗썸 USDT/KRW 가격 변동
시간: 2023-04-01 15:31:05.456
새 환율: 1,345원/USDT (이전: 1,350원/USDT)
```

**2. 환율 업데이트 및 재계산 대상 선정**
```python
# 환율 업데이트
old_rate = usdt_krw['rate']  # 1,350
usdt_krw = {'rate': 1345, 'timestamp': 1680359465456}

# 환율 변동률 계산
rate_change_ratio = usdt_krw['rate'] / old_rate - 1  # -0.0037 (약 -0.37%)

# 변동폭이 일정 수준 이상인 경우에만 주요 페어 재계산
if abs(rate_change_ratio) >= 0.001:  # 0.1% 이상 변동
    # 최적 전송코인 및 수익코인 페어 재계산
    recalculate_pair_premium(best_transfer['indices'])
    recalculate_pair_premium(best_profit['indices'])
    
    # 상위 N개 유망 페어 재계산
    for pair in promising_pairs[:10]:
        recalculate_pair_premium(pair)
```

**3. 최적 기회 재평가**
```python
# 최적 조합의 새로운 기대 수익률 계산
expected_return = best_profit['premium'] - best_transfer['premium'] - total_fee_rate

if expected_return >= 0.001:
    print(f"환율 변동 후에도 거래 기회 유지! 수익률: {expected_return*100:.2f}%")
else:
    print(f"환율 변동으로 거래 기회 사라짐. 새 수익률: {expected_return*100:.2f}%")
```

### 시뮬레이션 케이스 3: 다중 이벤트 동시 처리

**1. 상황 설정**
```
- 빗썸 XRP 오더북 업데이트와 바이빗 DOGE 오더북 업데이트가 거의 동시에 도착
- 처리 우선순위: 타임스탬프 기준 순차 처리
```

**2. 여러 이벤트 큐잉**
```python
# 이벤트 큐에 추가
event_queue.put({
    'type': 'orderbook_update',
    'exchange': '빗썸',
    'symbol': 'XRP',
    'data': {...},
    'timestamp': 1680359510123
})

event_queue.put({
    'type': 'orderbook_update',
    'exchange': '바이빗',
    'symbol': 'DOGE',
    'data': {...},
    'timestamp': 1680359510456
})
```

**3. 순차 처리**
```python
# 각 이벤트를 순차적으로 처리
event1 = event_queue.get()
process_orderbook_update(event1)

event2 = event_queue.get()
process_orderbook_update(event2)

# 최종 최적 조합 재평가
evaluate_best_opportunity()
```

### 시뮬레이션 케이스 4: 최적 조합 발견 시 거래 실행

**1. 수익성 있는 조합 발견**
```
최적 전송코인: LAYER (업비트→바이넌스), 김프율: -2.8%
최적 수익코인: DOGE (바이빗→빗썸), 김프율: +5.5%
기대 수익률: 7.9% (수수료 제외 전)
총 수수료 및 비용: 약 0.4%
순 기대 수익률: 7.5% > 0.1% (기준 임계값)
```

**2. 거래 자금 할당**
```python
# 거래 규모 설정
transaction_size_krw = 10_000_000  # 1천만원
transaction_size_usdt = 7_700  # 약 7,700 USDT (환율 1,300원 기준)

# 자금 풀에서 락(lock)
lock_funds('KRW', transaction_size_krw)
lock_funds('USDT', transaction_size_usdt)
```

**3. 4개 주문 동시 실행**
```python
# 전송코인 경로
order1_result = place_order('업비트', 'LAYER', 'buy', transaction_size_krw)
order2_result = place_order('바이낸스', 'LAYER', 'margin_sell', order1_result['quantity'])

# 수익코인 경로
order3_result = place_order('바이빗', 'DOGE', 'buy', transaction_size_usdt)
order4_result = place_order('바이빗', 'DOGE-PERP', 'short', order3_result['quantity'])
```

**4. 전송 모니터링 및 완료 처리**
```python
# 전송 요청
withdrawal_id = withdraw_coin('업비트', 'LAYER', order1_result['quantity'] - withdrawal_fee, 'binance_address')

# 거래 상태 기록
transaction_id = generate_uuid()
active_transactions[transaction_id] = {
    'status': 'in_progress',
    'transfer_coin': 'LAYER',
    'profit_coin': 'DOGE',
    'transfer_order_id': (order1_result['id'], order2_result['id']),
    'profit_order_id': (order3_result['id'], order4_result['id']),
    'withdrawal_id': withdrawal_id,
    'start_time': current_timestamp,
    'expected_return': 0.075,
    'funds': {'KRW': transaction_size_krw, 'USDT': transaction_size_usdt}
}

# 완료 처리 함수 (비동기로 완료 상태 모니터링)
async def monitor_completion(transaction_id):
    transaction = active_transactions[transaction_id]
    
    # 전송코인 도착 확인
    while True:
        deposit_status = check_deposit('바이낸스', 'LAYER', transaction['withdrawal_id'])
        if deposit_status == 'completed':
            # 전송코인 마진 대출 상환
            repay_margin_loan('바이낸스', 'LAYER', transaction['transfer_order_id'][1])
            break
        await asyncio.sleep(30)  # 30초마다 체크
    
    # 수익코인 국내 도착 확인
    withdraw_profit_coin('바이빗', 'DOGE', adjusted_quantity, 'bithumb_address')
    
    while True:
        deposit_status = check_deposit('빗썸', 'DOGE', expected_id)
        if deposit_status == 'completed':
            # 국내 수익코인 매도
            sell_result = place_order('빗썸', 'DOGE', 'sell', final_quantity)
            # 선물 포지션 청산
            close_result = place_order('바이빗', 'DOGE-PERP', 'close_short', position_size)
            break
        await asyncio.sleep(30)
    
    # 거래 완료 및 실제 수익 계산
    final_krw = sell_result['total'] * (1 - fee_rate['bithumb']['sell'])
    profit_krw = final_krw - transaction['funds']['KRW']
    profit_percentage = profit_krw / transaction['funds']['KRW'] * 100
    
    # 거래 결과 기록
    transaction['status'] = 'completed'
    transaction['end_time'] = current_timestamp
    transaction['actual_return'] = profit_percentage / 100
    transaction['profit_krw'] = profit_krw
    
    # 자금 풀 반환
    release_funds('KRW', final_krw)
    release_funds('USDT', transaction['funds']['USDT'])
    
    print(f"거래 완료! 예상 수익률: {transaction['expected_return']*100:.2f}%, 실제 수익률: {profit_percentage:.2f}%")
```

### 실제 사례 기반 수익성 분석

#### 사례 1: XRP 전송 / BTC 수익 조합
```
조건:
- 국내 XRP 구매가: 720원
- 해외 XRP 판매가: 0.53 USDT (USDT/KRW 1,350원 기준 = 715.5원)
- XRP 김프율: +0.63%
- 국내 BTC 판매가: 80,100,000원
- 해외 BTC 구매가: 58,500 USDT (USDT/KRW 1,350원 기준 = 78,975,000원)
- BTC 김프율: +1.42%

계산:
- 전송코인(XRP) 효율: 715.5 / 720 = 0.9937 (약 -0.63%)
- 수익코인(BTC) 효율: 80,100,000 / 78,975,000 = 1.0142 (약 +1.42%)
- 총 효율: 0.9937 * 1.0142 = 1.0077
- 총 수익률(수수료 전): 0.77%
- 수수료 등 비용: 약 0.4%
- 순 수익률: 약 0.37%
```

#### 사례 2: AVAX 전송 / ETH 수익 조합
```
조건:
- 국내 AVAX 구매가: 51,200원
- 해외 AVAX 판매가: 39.80 USDT (USDT/KRW 1,350원 기준 = 53,730원)
- AVAX 김프율: -4.71% (역프리미엄)
- 국내 ETH 판매가: 4,950,000원
- 해외 ETH 구매가: 3,550 USDT (USDT/KRW 1,350원 기준 = 4,792,500원)
- ETH 김프율: +3.29%

계산:
- 전송코인(AVAX) 효율: 53,730 / 51,200 = 1.0495 (약 +4.95%)
- 수익코인(ETH) 효율: 4,950,000 / 4,792,500 = 1.0329 (약 +3.29%)
- 총 효율: 1.0495 * 1.0329 = 1.0840
- 총 수익률(수수료 전): 8.40%
- 수수료 등 비용: 약 0.4%
- 순 수익률: 약 8.00%
```

### 구현 시 고려사항 및 최적화 전략

1. **효율적인 데이터 구조**
   - 인덱스 기반 하이브리드 구조로 메모리 효율화
   - 마스킹 기법을 통한 유효 페어만 계산
   - NumPy 배열 활용으로 벡터화 연산 가능

2. **계산 최적화**
   - 영향 받는 페어만 선택적 재계산
   - 환율 변동 시 중요 페어만 우선 업데이트
   - 임계값 필터링으로 불필요한 연산 제거

3. **동시성 관리**
   - 멀티스레딩으로 이벤트 처리 병렬화
   - 락 프리(lock-free) 구조 활용
   - 원자적 업데이트로 데이터 정합성 유지

4. **거래 실행 최적화**
   - 주문 동시 실행으로 시간차 최소화
   - 비동기 모니터링으로 리소스 효율화
   - 단일 트랜잭션 ID로 관련 주문 그룹화

이러한 시뮬레이션 예시와 최적화 전략을 기반으로 초당 1,000개 이상의 오더북 이벤트를 효율적으로 처리하면서 0.1% 이상의 수익 기회를 실시간으로 포착하고 실행하는 시스템을 구현할 수 있습니다.

---

## 거래 경로별 수수료 내역 상세

교차 김치 프리미엄 차익거래는 여러 단계의 거래가 필요하며, 각 단계마다 수수료가 발생합니다. 아래는 전송코인과 수익코인 경로에서 발생하는 모든 수수료 항목을 상세하게 정리한 내용입니다.

### 전송코인 경로 수수료 (국내→해외)

1. **국내 거래소 매수 수수료** (현물 테이커)
   - 업비트: 0.05%
   - 빗썸: 0.04%
   - 실제 효과: 동일 금액으로 매수 가능한 코인 수량 감소

2. **해외 거래소 매도 수수료** (현물 테이커)
   - 바이낸스: 0.1%
   - 바이빗: 0.1%
   - 실제 효과: 매도 금액에서 차감되어 확보 USDT 감소

3. **해외 거래소 대출 수수료**
   - 단기간(10~20분) 사용으로 효과 미미하여 계산에서 제외
   - 실제 효과: 무시 가능 수준

4. **국내 거래소 전송 수수료** (코인)
   - 코인별 고정 수수료 (예: BTC 0.0005, XRP 0.25)
   - 실제 효과: 전송 가능 코인 수량 감소


### 수익코인 경로 수수료 (해외→국내)

1. **해외 거래소 매수 수수료** (현물 테이커)
   - 바이낸스: 0.075%
   - 바이빗: 0.1%
   - 실제 효과: 동일 USDT로 매수 가능한 코인 수량 감소

2. **해외 거래소 숏 포지션 수수료** (숏 오픈 테이커)
   - 바이낸스 선물: 0.5%
   - 바이빗 선물:  0.055%
   - 실제 효과: 헤지 포지션 구축 비용

3. **해외 거래소 전송 수수료** (코인)
   - 코인별 고정 수수료 (예: BTC 0.0005, ETH 0.005)
   - 실제 효과: 국내로 전송 가능한 코인 수량 감소

4. **국내 거래소 매도 수수료** (현물 테이커)
   - 업비트: 0.05%
   - 빗썸: 0.04%
   - 실제 효과: 매도 금액에서 차감되어 획득 KRW 감소

5. **해외 거래소 숏 포지션 수수료** (숏 클로즈 테이커)
   - 바이낸스 선물: 0.05%
   - 바이빗 선물:  0.055%
   - 실제 효과: 포지션 청산 비용

* 향후 등급에 따라 수수료 낮아짐
최대 총 수수료 = 0.45%
>>>>> 전송 코인 합계(최대): 0.05%(국내 매수) + 0.1%(해외 매도) + 0.02%(전송 수수료/500만) = 0.17%
>>>>> 수익 코인 합계(최대): 0.1%(해외 매수) + 0.055%(숏 오픈) + 0.02%(전송 수수료/500만) + 0.05%(국내 매도) + 0.055%(숏 청산) = 0.28%

이러한 모든 수수료를 고려하면 교차 김프 차익거래 한 사이클당 약 0.3%~0.4%의 총 수수료 비용이 발생합니다. 따라서 최소 0.1% 이상의 순수익률을 확보하기 위해서는 총수익이 최소 0.4~0.5% 이상 되어야 합니다.