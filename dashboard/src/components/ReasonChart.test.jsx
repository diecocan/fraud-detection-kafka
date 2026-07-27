import { describe, it, expect, vi } from 'vitest';
import { Suspense } from 'react';
import { render, screen, act } from '@testing-library/react';
import ReasonChart from './ReasonChart';
import { getStatsPromise } from '../api';

vi.mock('../api', () => ({
    getStatsPromise: vi.fn(),
}));

async function renderChart() {
    let result;
    await act(async () => {
        result = render(
            <Suspense fallback={<p>loading</p>}>
                <ReasonChart />
            </Suspense>
        );
    });
    return result;
}

describe('ReasonChart', () => {
    it('renders the total alert count from the stats', async () => {
        getStatsPromise.mockReturnValue(Promise.resolve([
            { reason: 'VELOCITY', count: 7 },
            { reason: 'AMOUNT_ANOMALY', count: 3 },
        ]));

        await renderChart();

        expect(screen.getByText('10 total alerts')).toBeInTheDocument();
    });

    it('renders zero total alerts when there are no stats', async () => {
        getStatsPromise.mockReturnValue(Promise.resolve([]));

        await renderChart();

        expect(screen.getByText('0 total alerts')).toBeInTheDocument();
    });
});
