import React, { useState, useEffect } from 'react';
import {
  Tabs,
  Tab,
  Box,
  Paper,
  makeStyles
} from '@material-ui/core';
import TradingSettings from './TradingSettings';
import ExchangeAPISettings from './ExchangeSettings';
import MonitoringSettings from './MonitoringSettings';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
    backgroundColor: theme.palette.background.paper,
    padding: theme.spacing(3),
  },
  tabs: {
    borderBottom: `1px solid ${theme.palette.divider}`,
    marginBottom: theme.spacing(3),
  }
}));

function TabPanel({ children, value, index }) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`settings-tabpanel-${index}`}
      aria-labelledby={`settings-tab-${index}`}
    >
      {value === index && <Box>{children}</Box>}
    </div>
  );
}

function SettingsPage() {
  const classes = useStyles();
  const [activeTab, setActiveTab] = useState(0);
  const [settings, setSettings] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchSettings();
  }, []);

  const fetchSettings = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/settings');
      const data = await response.json();
      setSettings(data);
      setLoading(false);
    } catch (error) {
      console.error('Failed to fetch settings:', error);
      setLoading(false);
    }
  };

  const handleTabChange = (event, newValue) => {
    setActiveTab(newValue);
  };

  const handleSettingsUpdate = async (updatedSettings) => {
    try {
      const response = await fetch('http://localhost:8000/api/settings', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(updatedSettings),
      });

      if (response.ok) {
        setSettings(updatedSettings);
        alert('Settings saved successfully');
      } else {
        throw new Error('Failed to save settings');
      }
    } catch (error) {
      console.error('Error saving settings:', error);
      alert('Failed to save settings');
    }
  };

  if (loading || !settings) {
    return <div>Loading settings...</div>;
  }

  return (
    <div className={classes.root}>
      <Paper>
        <Tabs
          value={activeTab}
          onChange={handleTabChange}
          indicatorColor="primary"
          textColor="primary"
          className={classes.tabs}
        >
          <Tab label="Trading Settings" />
          <Tab label="Exchange API Settings" />
          <Tab label="Monitoring Settings" />
        </Tabs>

        <TabPanel value={activeTab} index={0}>
          <TradingSettings
            settings={settings}
            onSave={handleSettingsUpdate}
          />
        </TabPanel>

        <TabPanel value={activeTab} index={1}>
          <ExchangeAPISettings />
        </TabPanel>

        <TabPanel value={activeTab} index={2}>
          <MonitoringSettings
            settings={settings}
            onSave={handleSettingsUpdate}
          />
        </TabPanel>
      </Paper>
    </div>
  );
}

export default SettingsPage; 