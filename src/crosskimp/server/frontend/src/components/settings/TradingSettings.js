import React, { useState } from 'react';
import {
  Paper,
  Typography,
  Grid,
  TextField,
  Button,
  Box,
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
  settingsContainer: {
    marginTop: theme.spacing(3),
  },
  section: {
    marginTop: theme.spacing(3),
  },
  saveButton: {
    marginTop: theme.spacing(3),
  }
}));

function TradingSettings({ settings, onSave }) {
  const classes = useStyles();
  const [localSettings, setLocalSettings] = useState(settings || {});

  if (!settings) {
    return null;
  }

  const handleChange = (section, field, value) => {
    setLocalSettings(prev => ({
      ...prev,
      trading: {
        ...prev.trading,
        [section]: {
          ...prev.trading?.[section],
          [field]: value
        }
      }
    }));
  };

  const handleSave = () => {
    onSave(localSettings);
  };

  return (
    <div className={classes.settingsContainer}>
      <Paper className={classes.paper}>
        <Typography variant="h6" gutterBottom>Trading Settings</Typography>

        {/* General Trading Settings */}
        <div className={classes.section}>
          <Typography variant="subtitle1" gutterBottom>General Settings</Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <TextField
                label="Base Order Amount (KRW)"
                type="number"
                value={settings.trading?.general?.base_order_amount_krw || ''}
                onChange={(e) => handleChange('general', 'base_order_amount_krw', Number(e.target.value))}
                fullWidth
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                label="Minimum Volume (KRW)"
                type="number"
                value={settings.trading?.general?.min_volume_krw || ''}
                onChange={(e) => handleChange('general', 'min_volume_krw', Number(e.target.value))}
                fullWidth
              />
            </Grid>
          </Grid>
        </div>

        {/* Risk Management Settings */}
        <div className={classes.section}>
          <Typography variant="subtitle1" gutterBottom>Risk Management</Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <TextField
                label="Maximum Position (KRW)"
                type="number"
                value={settings.trading?.risk?.max_position_krw || ''}
                onChange={(e) => handleChange('risk', 'max_position_krw', Number(e.target.value))}
                fullWidth
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                label="Stop Loss (%)"
                type="number"
                value={settings.trading?.risk?.stop_loss_percentage || ''}
                onChange={(e) => handleChange('risk', 'stop_loss_percentage', Number(e.target.value))}
                fullWidth
              />
            </Grid>
            <Grid item xs={12} md={4}>
              <TextField
                label="Take Profit (%)"
                type="number"
                value={settings.trading?.risk?.take_profit_percentage || ''}
                onChange={(e) => handleChange('risk', 'take_profit_percentage', Number(e.target.value))}
                fullWidth
              />
            </Grid>
          </Grid>
        </div>

        {/* Exchange Fee Settings */}
        <div className={classes.section}>
          <Typography variant="subtitle1" gutterBottom>Exchange Fees</Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <TextField
                label="Maker Fee (%)"
                type="number"
                value={settings.trading?.fees?.maker_fee_percentage || ''}
                onChange={(e) => handleChange('fees', 'maker_fee_percentage', Number(e.target.value))}
                fullWidth
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                label="Taker Fee (%)"
                type="number"
                value={settings.trading?.fees?.taker_fee_percentage || ''}
                onChange={(e) => handleChange('fees', 'taker_fee_percentage', Number(e.target.value))}
                fullWidth
              />
            </Grid>
          </Grid>
        </div>

        <Box mt={3}>
          <Button
            variant="contained"
            color="primary"
            onClick={handleSave}
            className={classes.saveButton}
          >
            Save Trading Settings
          </Button>
        </Box>
      </Paper>
    </div>
  );
}

export default TradingSettings; 