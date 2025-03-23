// file: monitoring-frontend/src/App.js

import React, { useState, useEffect } from 'react';
import {
  Container,
  Grid,
  Paper,
  Typography,
  Box,
  CircularProgress,
  Tabs,
  Tab,
} from '@material-ui/core';
import { makeStyles } from '@material-ui/core/styles';
import SystemResourcesChart from './components/charts/SystemResourcesChart';
import NetworkChart from './components/charts/NetworkChart';
import DashboardTable from './components/dashboard/DashboardTable';
import ExchangeStatusSummary from './components/dashboard/ExchangeStatusSummary';
import ProfitStats from './components/dashboard/ProfitStats';
import Settings from './components/settings/SettingsPage';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    padding: theme.spacing(3),
    backgroundColor: '#f5f5f5',
    minHeight: '100vh',
  },
  paper: {
    padding: theme.spacing(2),
    height: '100%',
    display: 'flex',
    flexDirection: 'column',
    backgroundColor: '#ffffff',
    borderRadius: '8px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
  },
  title: {
    marginBottom: theme.spacing(3),
    color: '#1a237e',
    fontWeight: 600
  },
  exchangeTitle: {
    color: '#666666',
    marginBottom: theme.spacing(1),
    fontWeight: 500
  },
  loading: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: '100vh'
  },
  chartContainer: {
    height: 300,
    marginTop: theme.spacing(2)
  }
}));

function TabPanel({ children, value, index }) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
    >
      {value === index && (
        <Box p={3}>
          {children}
        </Box>
      )}
    </div>
  );
}

function App() {
  const classes = useStyles();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [tabValue, setTabValue] = useState(0);
  const [metricsHistory, setMetricsHistory] = useState([]);
  const [settings, setSettings] = useState(null);

  useEffect(() => {
    let ws = null;
    let reconnectTimeout = null;
    let reconnectAttempts = 0;
    const maxReconnectAttempts = 10;
    const baseReconnectDelay = 1000;
    
    const connectWebSocket = () => {
      if (reconnectAttempts >= maxReconnectAttempts) {
        console.error('최대 재연결 시도 횟수 초과');
        setError('서버 연결에 실패했습니다. 페이지를 새로고침해주세요.');
        return;
      }
      
      try {
        ws = new WebSocket('ws://localhost:8000/ws');
        
        ws.onopen = () => {
          console.log('WebSocket Connected');
          setLoading(false);
          setError(null);
          reconnectAttempts = 0;
        };
        
        ws.onmessage = (event) => {
          try {
            const parsedData = JSON.parse(event.data);
            setData(parsedData);
            
            if (parsedData.system_metrics) {
              setMetricsHistory(prev => {
                const newHistory = [...prev, parsedData.system_metrics];
                return newHistory.slice(-30);
              });
            }
            
            setError(null);
          } catch (err) {
            console.error('Failed to parse WebSocket message:', err);
          }
        };
        
        ws.onerror = (error) => {
          console.error('WebSocket Error:', error);
          setError('연결 오류가 발생했습니다.');
        };
        
        ws.onclose = (event) => {
          console.log('WebSocket Disconnected');
          setError('연결이 종료되었습니다. 재연결 시도 중...');
          
          const delay = Math.min(baseReconnectDelay * Math.pow(2, reconnectAttempts), 30000);
          reconnectAttempts++;
          
          reconnectTimeout = setTimeout(() => {
            console.log(`재연결 시도 ${reconnectAttempts}/${maxReconnectAttempts}`);
            connectWebSocket();
          }, delay);
        };
      } catch (error) {
        console.error('WebSocket 연결 시도 중 오류:', error);
        setError('연결을 시도하는 중 오류가 발생했습니다.');
        
        reconnectTimeout = setTimeout(() => {
          console.log('연결 재시도...');
          connectWebSocket();
        }, baseReconnectDelay);
      }
    };
    
    connectWebSocket();
    
    const pingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send('ping');
      }
    }, 30000);
    
    return () => {
      if (ws) {
        ws.close();
      }
      if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
      }
      clearInterval(pingInterval);
    };
  }, []);

  if (loading) {
    return (
      <Box className={classes.loading}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box className={classes.loading}>
        <Typography color="error">{error}</Typography>
      </Box>
    );
  }

  return (
    <div className={classes.root}>
      <Container>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h4" className={classes.title}>
            CrossKimp Monitoring
          </Typography>
        </Box>
        
        <Tabs 
          value={tabValue} 
          onChange={(e, newValue) => setTabValue(newValue)}
          indicatorColor="primary"
          textColor="primary"
        >
          <Tab label="Dashboard" />
          <Tab label="Trades" />
          <Tab label="Settings" />
        </Tabs>

        <TabPanel value={tabValue} index={0}>
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <ExchangeStatusSummary data={data} />
            </Grid>
            <Grid item xs={12}>
              <DashboardTable data={data} />
            </Grid>
            <Grid container spacing={2}>
              <Grid item xs={12} md={6}>
                <Paper className={classes.paper}>
                  <Typography variant="h6" className={classes.exchangeTitle}>
                    System Resources
                  </Typography>
                  <SystemResourcesChart data={metricsHistory} />
                </Paper>
              </Grid>
              <Grid item xs={12} md={6}>
                <Paper className={classes.paper}>
                  <Typography variant="h6" className={classes.exchangeTitle}>
                    Network Traffic
                  </Typography>
                  <NetworkChart data={metricsHistory} />
                </Paper>
              </Grid>
            </Grid>
          </Grid>
        </TabPanel>

        <TabPanel value={tabValue} index={1}>
          <ProfitStats />
        </TabPanel>

        <TabPanel value={tabValue} index={2}>
          <Settings 
            settings={settings}
            onSave={setSettings}
          />
        </TabPanel>
      </Container>
    </div>
  );
}

export default App;