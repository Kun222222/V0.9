import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';

function SystemResourcesChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart
        data={data}
        margin={{
          top: 5,
          right: 30,
          left: 20,
          bottom: 5,
        }}
      >
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="time" />
        <YAxis domain={[0, 100]} />
        <Tooltip />
        <Legend />
        <Line
          type="monotone"
          dataKey="cpu_percent"
          stroke="#2196f3"
          name="CPU Usage (%)"
          isAnimationActive={false}
        />
        <Line
          type="monotone"
          dataKey="memory_percent"
          stroke="#4caf50"
          name="Memory Usage (%)"
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}

export default SystemResourcesChart; 