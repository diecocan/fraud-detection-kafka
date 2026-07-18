import { useEffect, useState } from 'react';
import { riskClass, relativeTime } from '../utils';

export default function AlertFeed() {
    const [alerts, setAlerts] = useState([]);
    const [connected, setConnected] = useState(false);

    useEffect(() => {
        const source = new EventSource('/api/alerts/stream');

        source.onopen = () => setConnected(true);

        source.addEventListener('alert', (event) => {
            const alert = JSON.parse(event.data);
            setAlerts((prev) => [alert, ...prev].slice(0, 50)); // first 50 alerts
        });

        source.onerror = () => {
            setConnected(false);
            console.error('SSE connection error');
        };

        return () => source.close();
    }, []);

    return (
        <div
            aria-live='polite'
            aria-label='Live fraud alerts'
            className='card'
        >
            <h2>Live Alerts</h2>
            <span className={connected ? 'connection-status status-live' : 'connection-status status-down'}>
                {connected ? 'Live' : 'Disconnected'}
            </span>

            <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
                <table>
                    <thead>
                        <tr>
                            <th scope='col'>Reason</th>
                            <th scope='col'>Account</th>
                            <th scope='col'>Risk</th>
                            <th scope='col'>Detected</th>
                        </tr>
                    </thead>
                    <tbody>
                        {
                            alerts.map((a) => (
                                <tr key={a.alertId}>
                                    <td><span className={`badge badge-${a.reason}`}>{a.reason}</span></td>
                                    <td>{a.accountId}</td>
                                    <td><span className={`risk-badge ${riskClass(a.riskScore)}`}>{(a.riskScore * 100).toFixed(0)}%</span></td>
                                    <td>{relativeTime(a.detectedAt)}</td>
                                </tr>
                            ))
                        }
                    </tbody>
                </table>
            </div>
        </div>
    );
}