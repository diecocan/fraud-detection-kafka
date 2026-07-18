import { use } from 'react';
import { getStatsPromise } from '../api';
import { Cell, Legend, Pie, PieChart, Tooltip, ResponsiveContainer } from 'recharts';

const COLORS = { VELOCITY: '#8884d8', AMOUNT_ANOMALY: '#82ca9d', IMPOSSIBLE_GEO: '#ffc658' };
const renderPercentLabel = ({ percent }) => `${(percent * 100).toFixed(0)}%`;

export default function ReasonChart() {
    const stats = use(getStatsPromise());
    const total = stats.reduce((sum, s) => sum + s.count, 0);
    

    return (
        <div className='card chart-card'>
            <h2>Alerts by Reason</h2>
            <p className='card-subtitle'>{total.toLocaleString()} total alerts</p>
            <div className='chart-fill'>
                <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                        <Pie 
                            data={stats}
                            dataKey="count"
                            nameKey="reason"
                            cx="50%"
                            cy="50%"
                            outerRadius="70%"
                            label={renderPercentLabel}
                        >
                            {stats.map((entry) => (<Cell key={entry.reason} fill={COLORS[entry.reason]} />))}
                        </Pie>
                        <Tooltip />
                        <Legend />
                    </PieChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
}