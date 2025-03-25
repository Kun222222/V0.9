import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Paper,
  Typography,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  makeStyles
} from '@material-ui/core';

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
  profitPositive: {
    color: '#4caf50',
  },
  profitNegative: {
    color: '#f44336',
  }
}));

function ProfitStats() {
  const classes = useStyles();
  const [timeframe, setTimeframe] = useState('day');
  const [trades, setTrades] = useState({ summary: [], recent: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    fetch('http://localhost:8000/api/trades')
      .then(res => res.json())
      .then(data => {
        setTrades(data || { summary: [], recent: [] });
        setError(null);
      })
      .catch(err => {
        console.error('Failed to fetch trades:', err);
        setError('Failed to load trade data');
      })
      .finally(() => setLoading(false));
  }, [timeframe]);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" p={3}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" p={3}>
        <Typography color="error">{error}</Typography>
      </Box>
    );
  }

  return (
    <div>
      <Box mb={2}>
        <Tabs
          value={timeframe}
          onChange={(e, newValue) => setTimeframe(newValue)}
          indicatorColor="primary"
          textColor="primary"
        >
          <Tab value="day" label="Daily" />
          <Tab value="month" label="Monthly" />
          <Tab value="year" label="Yearly" />
        </Tabs>
      </Box>

      <Grid container spacing={3}>
        <Grid item xs={12}>
          <Paper className={classes.paper}>
            <Typography variant="h6" gutterBottom>
              Profit Summary
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Date</TableCell>
                    <TableCell align="right">Total Profit</TableCell>
                    <TableCell align="right">Trade Count</TableCell>
                    <TableCell align="right">Win Rate</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {(trades.summary || []).map((row) => (
                    <TableRow key={row.date}>
                      <TableCell>{row.date}</TableCell>
                      <TableCell align="right" className={
                        (row.profit || 0) >= 0 ? classes.profitPositive : classes.profitNegative
                      }>
                        {(row.profit || 0).toLocaleString()} KRW
                      </TableCell>
                      <TableCell align="right">{(row.trade_count || 0).toLocaleString()}</TableCell>
                      <TableCell align="right">
                        {((row.win_rate || 0) * 100).toFixed(1)}%
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        <Grid item xs={12}>
          <Paper className={classes.paper}>
            <Typography variant="h6" gutterBottom>
              Recent Trades
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Time</TableCell>
                    <TableCell>Symbol</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell align="right">Entry Price</TableCell>
                    <TableCell align="right">Exit Price</TableCell>
                    <TableCell align="right">Profit/Loss</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {(trades.recent || []).map((trade) => (
                    <TableRow key={trade.id}>
                      <TableCell>{new Date(trade.timestamp || 0).toLocaleString()}</TableCell>
                      <TableCell>{trade.symbol || '-'}</TableCell>
                      <TableCell>{trade.type || '-'}</TableCell>
                      <TableCell align="right">{(trade.entry_price || 0).toLocaleString()}</TableCell>
                      <TableCell align="right">{(trade.exit_price || 0).toLocaleString()}</TableCell>
                      <TableCell align="right" className={
                        (trade.profit || 0) >= 0 ? classes.profitPositive : classes.profitNegative
                      }>
                        {(trade.profit || 0).toLocaleString()} KRW
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>
      </Grid>
    </div>
  );
}

export default ProfitStats; 