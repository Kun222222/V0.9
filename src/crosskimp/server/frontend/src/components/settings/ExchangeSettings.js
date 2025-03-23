import React, { useState, useEffect } from 'react';
import {
  Paper,
  Typography,
  Grid,
  TextField,
  Button,
  Box,
  IconButton,
  Snackbar,
  makeStyles
} from '@material-ui/core';
import { Visibility, VisibilityOff } from '@material-ui/icons';
import MuiAlert from '@material-ui/lab/Alert';

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
  settingsContainer: {
    marginTop: theme.spacing(3),
  },
  exchangeSection: {
    marginTop: theme.spacing(2),
    padding: theme.spacing(2),
    border: '1px solid #e0e0e0',
    borderRadius: '4px'
  },
  saveButton: {
    marginTop: theme.spacing(3),
  },
  textField: {
    position: 'relative',
  },
  visibilityButton: {
    position: 'absolute',
    right: theme.spacing(1),
    top: '50%',
    transform: 'translateY(-50%)',
  }
}));

const EXCHANGE_NAMES = {
  binance: "바이낸스",
  bybit: "바이비트",
  upbit: "업비트",
  bithumb: "빗썸"
};

function Alert(props) {
  return <MuiAlert elevation={6} variant="filled" {...props} />;
}

function ExchangeAPISettings() {
  const classes = useStyles();
  const [apiKeys, setApiKeys] = useState({});
  const [loading, setLoading] = useState(true);
  const [showPasswords, setShowPasswords] = useState({});
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: '',
    severity: 'success'
  });

  useEffect(() => {
    fetchApiKeys();
  }, []);

  const fetchApiKeys = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/api-keys');
      if (!response.ok) {
        throw new Error('Failed to fetch API keys');
      }
      const data = await response.json();
      setApiKeys(data);
    } catch (error) {
      console.error('Failed to fetch API keys:', error);
      setSnackbar({
        open: true,
        message: 'Failed to load API keys',
        severity: 'error'
      });
    } finally {
      setLoading(false);
    }
  };

  const handleExchangeChange = (exchange, field, value) => {
    setApiKeys(prev => ({
      ...prev,
      [exchange]: {
        ...prev[exchange],
        [field]: value
      }
    }));
  };

  const togglePasswordVisibility = (exchange) => {
    setShowPasswords(prev => ({
      ...prev,
      [exchange]: !prev[exchange]
    }));
  };

  const handleSave = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/api-keys', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(apiKeys),
      });

      if (!response.ok) {
        throw new Error('Failed to save API keys');
      }

      setSnackbar({
        open: true,
        message: 'API keys saved successfully',
        severity: 'success'
      });
    } catch (error) {
      console.error('Error saving API keys:', error);
      setSnackbar({
        open: true,
        message: 'Failed to save API keys',
        severity: 'error'
      });
    }
  };

  const handleCloseSnackbar = () => {
    setSnackbar(prev => ({ ...prev, open: false }));
  };

  if (loading) {
    return <div>Loading API keys...</div>;
  }

  return (
    <div className={classes.settingsContainer}>
      <Paper className={classes.paper}>
        <Typography variant="h6" gutterBottom>Exchange API Settings</Typography>
        
        {Object.entries(EXCHANGE_NAMES).map(([exchange, name]) => (
          <div key={exchange} className={classes.exchangeSection}>
            <Typography variant="subtitle1" gutterBottom>{name}</Typography>
            
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <div className={classes.textField}>
                  <TextField
                    label="API Key"
                    type={showPasswords[exchange] ? 'text' : 'password'}
                    value={apiKeys[exchange]?.api_key || ''}
                    onChange={(e) => handleExchangeChange(exchange, 'api_key', e.target.value)}
                    fullWidth
                  />
                  <IconButton
                    className={classes.visibilityButton}
                    onClick={() => togglePasswordVisibility(exchange)}
                    edge="end"
                  >
                    {showPasswords[exchange] ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                </div>
              </Grid>
              <Grid item xs={12} md={6}>
                <div className={classes.textField}>
                  <TextField
                    label="API Secret"
                    type={showPasswords[exchange] ? 'text' : 'password'}
                    value={apiKeys[exchange]?.api_secret || ''}
                    onChange={(e) => handleExchangeChange(exchange, 'api_secret', e.target.value)}
                    fullWidth
                  />
                  <IconButton
                    className={classes.visibilityButton}
                    onClick={() => togglePasswordVisibility(exchange)}
                    edge="end"
                  >
                    {showPasswords[exchange] ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                </div>
              </Grid>
            </Grid>
          </div>
        ))}

        <Box mt={3}>
          <Button
            variant="contained"
            color="primary"
            onClick={handleSave}
            className={classes.saveButton}
          >
            Save API Settings
          </Button>
        </Box>
      </Paper>

      <Snackbar 
        open={snackbar.open} 
        autoHideDuration={6000} 
        onClose={handleCloseSnackbar}
      >
        <Alert onClose={handleCloseSnackbar} severity={snackbar.severity}>
          {snackbar.message}
        </Alert>
      </Snackbar>
    </div>
  );
}

export default ExchangeAPISettings;