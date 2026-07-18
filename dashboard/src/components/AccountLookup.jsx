import { useState, useMemo } from 'react';
import { riskClass, relativeTime } from '../utils';

const REASONS = ['VELOCITY', 'AMOUNT_ANOMALY', 'IMPOSSIBLE_GEO'];

export default function AccountLookup() {
    const [accountId, setAccountId] = useState('');
    const [alerts, setAlerts] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const [sortColumn, setSortColumn] = useState('detectedAt');
    const [sortOrder, setSortOrder] = useState('desc');
    const [reasonFilter, setReasonFilter] = useState('');
    const [minRisk, setMinRisk] = useState('');

    function toggleSort(column) {
        if (sortColumn == column) {
            setSortOrder((prev) => (prev === 'desc' ? 'asc' : 'desc'));
        } else {
            setSortColumn(column);
            setSortOrder('desc');
        }
        
    }

    async function handleSearch(e) {
        e.preventDefault();
        setLoading(true);
        setError(null);
        let data = null;

        try {
            if (accountId) {
                const res = await fetch(`/api/alerts/account/${accountId}`);

                if (!res.ok) throw new Error(`Request failed: ${res.status}`);
                
                data = await res.json();
            }
            
            setAlerts(data);
        } catch (err) {
            setError(err.message);
            setAlerts(null);
        } finally {
            setLoading(false);
        }
    }

    const visibleAlerts = useMemo(() => {
        if (!alerts) return null;

        let result = alerts;
        if (reasonFilter) result = reasonFilter === 'ALL' ? result : result.filter((a) => a.reason === reasonFilter);
        if (minRisk != '') result = result.filter((a) => a.riskScore * 100 >= Number(minRisk));

        return [...result].sort((a, b) => {
            let diff;

            if (sortColumn == 'detectedAt') diff = new Date(a.detectedAt) - new Date(b.detectedAt);
            else if (sortColumn === 'riskScore') diff = a.riskScore - b.riskScore;
            else diff = a.reason.localeCompare(b.reason);

            return sortOrder === 'asc' ? diff : -diff;
        });
    }, [alerts, reasonFilter, minRisk, sortColumn, sortOrder]);

    function sortIndicator(column) {
        return sortColumn === column ? (sortOrder === 'desc' ? '↓' : '↑') : '';
    }

    function ariaSortFor(column) {
        return sortColumn === column ? (sortOrder === 'desc' ? 'descending' : 'ascending') : 'none';
    }

    return (
        <div className='card card-full'>
            <h2>Account Lookup</h2>

            <form onSubmit={handleSearch} className='search-form'>
                <label htmlFor='account-search'>Account ID</label>
                <input 
                    id='account-search'
                    value={accountId}
                    onChange={(e) => setAccountId(e.target.value)}
                    placeholder="e.g. account_3"
                />
                <button type='submit' disabled={loading}>Search</button>
            </form>

            {loading && <p>Loading...</p>}
            {error && <p role="alert" style={{ color: 'red' }}>Error: {error}</p>}
            {!loading && !error && !alerts && (
                <p>Search for an account to see its alerts.</p>
            )}
            {alerts && (
                <>
                    {alerts.length > 0 && (
                        <p className='card-subtitle'>{alerts.length.toLocaleString()} total alerts</p>
                    )}
                    <div className='results-scroll'>
                        {
                            alerts.length === 0 
                                ? (<p>No alerts found for this account.</p>)
                                : (
                                    <table>
                                        <thead>
                                            <tr>
                                                <th scope='col' aria-sort={ariaSortFor('reason')}>
                                                    <button className='sort-button' onClick={() => toggleSort('reason')}>
                                                        Reason {sortIndicator('reason')}
                                                    </button>
                                                </th>
                                                <th scope='col' aria-sort={ariaSortFor('riskScore')}>
                                                    <button className='sort-button' onClick={() => toggleSort('riskScore')}>
                                                        Risk {sortIndicator('riskScore')}
                                                    </button>
                                                </th>
                                                <th scope='col' aria-sort={ariaSortFor('detectedAt')}>
                                                    <button className='sort-button' onClick={() => toggleSort('detectedAt')}>
                                                        Detected {sortIndicator('detectedAt')}
                                                    </button>
                                                </th>
                                            </tr>
                                            <tr className='filter-row'>
                                                <th scope='col'>
                                                    <select
                                                        value={reasonFilter}
                                                        onChange={(e) => setReasonFilter(e.target.value)}
                                                        aria-label='Filter by reason'
                                                    >
                                                        <option>ALL</option>
                                                        {REASONS.map((r) => <option key={r} value={r}>{r}</option>)}
                                                    </select>
                                                </th>
                                                <th scope='col'>
                                                    <input
                                                        type='number'
                                                        min='0'
                                                        max='100'
                                                        value={minRisk}
                                                        onChange={(e) => setMinRisk(e.target.value)}
                                                        placeholder='Min %'
                                                        arial-label='Minimum risk percentage'
                                                    />
                                                </th>
                                                <th scope='col'></th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {
                                                visibleAlerts.map((a) => (
                                                    <tr key={a.alertId}>
                                                        <td><span className={`badge badge-${a.reason}`}>{a.reason}</span></td>
                                                        <td><span className={`risk-badge ${riskClass(a.riskScore)}`}>{(a.riskScore * 100).toFixed(0)}%</span></td>
                                                        <td>{relativeTime(a.detectedAt)}</td>
                                                    </tr>
                                                ))
                                            }
                                        </tbody>
                                    </table>
                                )
                        }
                    </div>
                </>
            )}
        </div>
    );
}