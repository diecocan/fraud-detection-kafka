import { lazy, Suspense } from 'react';
import './App.css';

const AlertFeed = lazy(() => import('./components/AlertFeed'));
const AccountLookup = lazy(() => import('./components/AccountLookup'))
const ReasonChart = lazy(() => import('./components/ReasonChart'));

export default function App() {
    return (
        <>
            <header>
                <h1>Fraud Detection Dashboard</h1>
            </header>
            <main>
                <Suspense fallback={<p>Loading live feed...</p>}>
                <AlertFeed />
                </Suspense>
                <Suspense fallback={<p>Loading chart...</p>}>
                    <ReasonChart />
                </Suspense>
                <Suspense fallback={<p>Loading account lookup...</p>}>
                    <AccountLookup />
                </Suspense>
            </main>
        </>
    );
}
