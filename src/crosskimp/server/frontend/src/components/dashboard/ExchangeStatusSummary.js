import React from 'react';
import { Paper, Grid, Typography, Box, makeStyles } from '@material-ui/core';

const useStyles = makeStyles((theme) => ({
  paper: {
    padding: theme.spacing(2),
    height: '100%',
    display: 'flex',
    flexDirection: 'column',
    backgroundColor: '#ffffff',
    borderRadius: '8px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
  },
  priceBox: {
    textAlign: 'center',
    marginTop: theme.spacing(2)
  },
  priceLabel: {
    color: '#666666',
    fontSize: '0.9rem',
    marginBottom: theme.spacing(1)
  },
  priceValue: {
    color: '#1a237e',
    fontSize: '1.2rem',
    fontWeight: 500
  },
  exchangePrice: {
    color: '#666666',
    fontSize: '0.8rem',
    marginTop: theme.spacing(0.5)
  }
}));

function ExchangeStatusSummary({ data }) {
  const classes = useStyles();
  const websocketStats = data?.websocket_stats || {};

  // 디버깅: 전체 데이터 구조 출력
  console.log('=== 디버깅 시작 ===');
  console.log('전체 데이터:', data);
  console.log('웹소켓 상태:', websocketStats);

  const totalExchanges = 6; // 전체 거래소 수
  const connectedExchanges = Object.values(websocketStats).filter(stat => stat.connected).length;

  // 디버깅: 각 거래소별 상태 출력
  Object.entries(websocketStats).forEach(([exchange, stats]) => {
    console.log(`${exchange} 상태 상세:`, {
      connected: stats.connected,
      fullStatsKeys: Object.keys(stats),
      fullStatsValues: Object.entries(stats).map(([key, value]) => `${key}: ${JSON.stringify(value)}`),
      message_count: stats.message_count,
      messages_per_second: stats.messages_per_second,
      error_count: stats.error_count,
      latency_ms: stats.latency_ms
    });
  });

  // 메시지 레이트 계산 과정 디버깅
  const totalMessagesPerSecond = Object.values(websocketStats)
    .reduce((acc, stat) => {
      // Use messages_per_second from server data
      const rate = stat.messages_per_second || 0;
      console.log('메시지 레이트 상세:', {
        exchange: stat.exchange,
        message_count: stat.message_count,
        messages_per_second: stat.messages_per_second,
        error_count: stat.error_count,
        rate,
        accumulator: acc
      });
      return acc + rate;
    }, 0);

  console.log('최종 초당 메시지 수:', totalMessagesPerSecond);

  // USDT/KRW 가격 정보
  const usdtPrices = data?.usdt_prices || {};
  const upbitPrice = usdtPrices.upbit || 0;
  const bithumbPrice = usdtPrices.bithumb || 0;

  console.log('=== 디버깅 종료 ===');

  return (
    <Paper className={classes.paper}>
      <Box p={2}>
        <Typography variant="h6" gutterBottom>Exchange Status Summary</Typography>
        <Grid container spacing={2}>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="subtitle2">System Uptime</Typography>
              <Typography variant="h4" color="primary">
                {data?.uptime?.formatted || "00:00:00"}
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="subtitle2">Connected Exchanges</Typography>
              <Typography variant="h4" color="primary">
                {connectedExchanges}/{totalExchanges}
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={6} md={3}>
            <Box textAlign="center">
              <Typography variant="subtitle2">Total Messages/sec</Typography>
              <Typography variant="h4" color="primary">
                {totalMessagesPerSecond.toFixed(1)}
              </Typography>
            </Box>
          </Grid>
          <Grid item xs={6} md={3}>
            <Box className={classes.priceBox}>
              <Typography variant="subtitle2">USDT/KRW Rate</Typography>
              <Typography className={classes.priceValue}>
                Upbit: {upbitPrice.toLocaleString()} KRW
              </Typography>
              <Typography className={classes.priceValue}>
                Bithumb: {bithumbPrice.toLocaleString()} KRW
              </Typography>
            </Box>
          </Grid>
        </Grid>
      </Box>
    </Paper>
  );
}

export default ExchangeStatusSummary; 