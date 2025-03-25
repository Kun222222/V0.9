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

function NetworkChart({ data }) {
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
        <YAxis />
        <Tooltip />
        <Legend />
        <Line
          type="monotone"
          dataKey="network_out"
          stroke="#f44336"
          name="Upload (MB/s)"
          isAnimationActive={false}
        />
        <Line
          type="monotone"
          dataKey="network_in"
          stroke="#2196f3"
          name="Download (MB/s)"
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}

export default NetworkChart; 