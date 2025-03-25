import React from 'react';
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  makeStyles
} from '@material-ui/core';

const useStyles = makeStyles((theme) => ({
  table: {
    minWidth: 650,
    backgroundColor: '#ffffff'
  },
  tableContainer: {
    backgroundColor: '#ffffff',
  },
  chip: {
    margin: theme.spacing(0.5),
  },
  connected: {
    backgroundColor: '#4caf50',
    color: '#ffffff'
  },
  disconnected: {
    backgroundColor: '#f44336',
    color: '#ffffff'
  },
  tableCell: {
    color: '#000000'
  },
  tableHead: {
    backgroundColor: '#f5f5f5'
  }
}));

function DashboardTable({ data }) {
  const classes = useStyles();
  
  const exchanges = [
    { name: 'Binance', key: 'binance' },
    { name: 'BinanceFuture', key: 'binancefuture' },
    { name: 'Bybit', key: 'bybit' },
    { name: 'BybitFuture', key: 'bybitfuture' },
    { name: 'Upbit', key: 'upbit' },
    { name: 'Bithumb', key: 'bithumb' }
  ];

  return (
    <TableContainer component={Paper} className={classes.tableContainer}>
      <Table className={classes.table}>
        <TableHead className={classes.tableHead}>
          <TableRow>
            <TableCell className={classes.tableCell}>Exchange</TableCell>
            <TableCell className={classes.tableCell}>Status</TableCell>
            <TableCell className={classes.tableCell} align="right">Messages</TableCell>
            <TableCell className={classes.tableCell} align="right">Rate (msg/s)</TableCell>
            <TableCell className={classes.tableCell} align="right">Errors</TableCell>
            <TableCell className={classes.tableCell} align="right">Latency (ms)</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {exchanges.map((exchange) => {
            const stats = data?.websocket_stats?.[exchange.key] || {};
            return (
              <TableRow key={exchange.key}>
                <TableCell className={classes.tableCell}>{exchange.name}</TableCell>
                <TableCell>
                  <Chip
                    label={stats.connected ? 'Connected' : 'Disconnected'}
                    className={`${classes.chip} ${
                      stats.connected ? classes.connected : classes.disconnected
                    }`}
                  />
                </TableCell>
                <TableCell className={classes.tableCell} align="right">
                  {stats.message_count?.toLocaleString() || 0}
                </TableCell>
                <TableCell className={classes.tableCell} align="right">
                  {stats.messages_per_second?.toFixed(1) || 0}
                </TableCell>
                <TableCell className={classes.tableCell} align="right">{stats.error_count || 0}</TableCell>
                <TableCell className={classes.tableCell} align="right">
                  {stats.latency_ms?.toFixed(1) || 0}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </TableContainer>
  );
}

export default DashboardTable; 