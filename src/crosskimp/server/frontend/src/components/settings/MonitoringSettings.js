import React, { useState } from 'react';
import {
  Paper,
  Typography,
  Box,
  Grid,
  TextField,
  Switch,
  FormControlLabel,
  Slider,
  Button,
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
  }
}));

function MonitoringSettings({ settings, onSave }) {
  const classes = useStyles();
  const [thresholds, setThresholds] = useState(settings?.thresholds || {
    cpu: 80,
    memory: 85,
    network: 100
  });
  const [notifications, setNotifications] = useState(settings?.notifications || {
    telegram: true,
    email: false
  });
  const [refreshInterval, setRefreshInterval] = useState(settings?.refreshInterval || 1000);

  const handleSave = () => {
    onSave({
      thresholds,
      notifications,
      refreshInterval
    });
  };

  return (
    <Paper className={classes.paper}>
      <Box p={2}>
        <Typography variant="h6" gutterBottom>Monitoring Settings</Typography>
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <Typography variant="subtitle2" gutterBottom>Alert Thresholds</Typography>
            <TextField
              label="CPU Usage (%)"
              type="number"
              value={thresholds.cpu}
              onChange={(e) => setThresholds({...thresholds, cpu: Number(e.target.value)})}
              size="small"
              style={{marginRight: 16}}
            />
            <TextField
              label="Memory Usage (%)"
              type="number"
              value={thresholds.memory}
              onChange={(e) => setThresholds({...thresholds, memory: Number(e.target.value)})}
              size="small"
              style={{marginRight: 16}}
            />
            <TextField
              label="Network Usage (MB/s)"
              type="number"
              value={thresholds.network}
              onChange={(e) => setThresholds({...thresholds, network: Number(e.target.value)})}
              size="small"
            />
          </Grid>
          <Grid item xs={12}>
            <Typography variant="subtitle2" gutterBottom>Notifications</Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={notifications.telegram}
                  onChange={(e) => setNotifications({...notifications, telegram: e.target.checked})}
                />
              }
              label="Telegram Notifications"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={notifications.email}
                  onChange={(e) => setNotifications({...notifications, email: e.target.checked})}
                />
              }
              label="Email Notifications"
            />
          </Grid>
          <Grid item xs={12}>
            <Typography variant="subtitle2" gutterBottom>Refresh Interval</Typography>
            <Slider
              value={refreshInterval}
              onChange={(e, newValue) => setRefreshInterval(newValue)}
              min={500}
              max={5000}
              step={100}
              valueLabelDisplay="auto"
              valueLabelFormat={(value) => `${value}ms`}
            />
          </Grid>
          <Grid item xs={12}>
            <Button variant="contained" color="primary" onClick={handleSave}>
              Save Settings
            </Button>
          </Grid>
        </Grid>
      </Box>
    </Paper>
  );
}

export default MonitoringSettings; 