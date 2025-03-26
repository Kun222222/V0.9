#!/bin/bash
# 오더북 검증 도구 실행 예시

# 결과 저장 디렉토리 생성
RESULT_DIR="results"
mkdir -p $RESULT_DIR

# 로그 파일 기본 경로
LOG_DIR="/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/raw_data"

# 예시 1: 단일 로그 파일 검증 (업비트)
echo "===== 예시 1: 업비트 로그 파일 검증 ====="
python delta_validator.py "${LOG_DIR}/250326_061501_upbit_raw_logger.log" \
  --max-lines 100000 \
  > "${RESULT_DIR}/upbit_validation_result.txt"
echo "결과가 ${RESULT_DIR}/upbit_validation_result.txt에 저장되었습니다."

# 예시 2: 특정 심볼에 대한 단일 로그 검증 (바이낸스 BTC)
echo "===== 예시 2: 바이낸스 BTC 심볼 검증 ====="
python delta_validator.py "${LOG_DIR}/250326_061501_binance_raw_logger.log" \
  --max-lines 100000 \
  --symbols BTC \
  > "${RESULT_DIR}/binance_btc_validation_result.txt"
echo "결과가 ${RESULT_DIR}/binance_btc_validation_result.txt에 저장되었습니다."

# 예시 3: 모든 로그 파일 검증
echo "===== 예시 3: 모든 로그 파일 검증 ====="
python validate_all.py \
  --log-dir "${LOG_DIR}" \
  --max-lines 50000 \
  --output "${RESULT_DIR}/all_exchanges_validation.json"
echo "결과가 ${RESULT_DIR}/all_exchanges_validation.json에 저장되었습니다."

# 예시 4: 특정 심볼의 오더북 시각화 (업비트 BTC)
echo "===== 예시 4: 업비트 BTC 오더북 시각화 ====="
python visualize_orderbook.py \
  "${LOG_DIR}/250326_061501_upbit_raw_logger.log" \
  BTC \
  --max-lines 100000 \
  --output-dir "${RESULT_DIR}/upbit_btc_viz"
echo "시각화 결과가 ${RESULT_DIR}/upbit_btc_viz 디렉토리에 저장되었습니다."

# 예시 5: 특정 심볼의 오더북 시각화 (바이낸스 ETH)
echo "===== 예시 5: 바이낸스 ETH 오더북 시각화 ====="
python visualize_orderbook.py \
  "${LOG_DIR}/250326_061501_binance_raw_logger.log" \
  ETH \
  --max-lines 100000 \
  --output-dir "${RESULT_DIR}/binance_eth_viz"
echo "시각화 결과가 ${RESULT_DIR}/binance_eth_viz 디렉토리에 저장되었습니다."

echo "모든 예시 실행 완료!" 