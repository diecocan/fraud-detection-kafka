import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import AccountLookup from './AccountLookup';

const alerts = [
    { alertId: '1', reason: 'VELOCITY', riskScore: 0.5, detectedAt: '2026-01-01T00:00:00Z' },
    { alertId: '2', reason: 'AMOUNT_ANOMALY', riskScore: 0.95, detectedAt: '2026-01-02T00:00:00Z' },
    { alertId: '3', reason: 'IMPOSSIBLE_GEO', riskScore: 0.2, detectedAt: '2026-01-03T00:00:00Z' },
];

function mockFetchOnce(body, ok = true, status = 200) {
    globalThis.fetch = vi.fn().mockResolvedValue({
        ok,
        status,
        json: () => Promise.resolve(body),
    });
}

async function searchFor(accountId) {
    const user = userEvent.setup();
    render(<AccountLookup />);

    await user.type(screen.getByLabelText('Account ID'), accountId);
    await user.click(screen.getByRole('button', { name: 'Search' }));
}

describe('AccountLookup', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('shows a prompt before any search has been made', () => {
        render(<AccountLookup />);
        expect(screen.getByText('Search for an account to see its alerts.')).toBeInTheDocument();
    });

    it('fetches and displays alerts for the searched account', async () => {
        mockFetchOnce(alerts);

        await searchFor('account_3');

        await waitFor(() => expect(screen.getByText('3 total alerts')).toBeInTheDocument());
        expect(fetch).toHaveBeenCalledWith('/api/alerts/account/account_3');
        expect(screen.getAllByRole('row')).toHaveLength(2 + alerts.length); // header + filter row + data rows
    });

    it('shows a message when no alerts are found', async () => {
        mockFetchOnce([]);

        await searchFor('account_9');

        await waitFor(() => expect(screen.getByText('No alerts found for this account.')).toBeInTheDocument());
    });

    it('shows an error message when the request fails', async () => {
        mockFetchOnce({}, false, 500);

        await searchFor('account_3');

        await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('Request failed: 500'));
    });

    it('filters visible alerts by minimum risk percentage', async () => {
        mockFetchOnce(alerts);
        const user = userEvent.setup();
        render(<AccountLookup />);

        await user.type(screen.getByLabelText('Account ID'), 'account_3');
        await user.click(screen.getByRole('button', { name: 'Search' }));
        await waitFor(() => expect(screen.getByText('3 total alerts')).toBeInTheDocument());

        await user.type(screen.getByPlaceholderText('Min %'), '70');

        const dataRows = screen.getAllByRole('row').slice(2); // skip header + filter row
        expect(dataRows).toHaveLength(1);
        expect(dataRows[0]).toHaveTextContent('95%');
    });

    it('filters visible alerts by reason', async () => {
        mockFetchOnce(alerts);
        const user = userEvent.setup();
        render(<AccountLookup />);

        await user.type(screen.getByLabelText('Account ID'), 'account_3');
        await user.click(screen.getByRole('button', { name: 'Search' }));
        await waitFor(() => expect(screen.getByText('3 total alerts')).toBeInTheDocument());

        await user.selectOptions(screen.getByLabelText('Filter by reason'), 'VELOCITY');

        const dataRows = screen.getAllByRole('row').slice(2);
        expect(dataRows).toHaveLength(1);
        expect(dataRows[0]).toHaveTextContent('VELOCITY');
    });

    it('sorts visible alerts when a column header is clicked', async () => {
        mockFetchOnce(alerts);
        const user = userEvent.setup();
        render(<AccountLookup />);

        await user.type(screen.getByLabelText('Account ID'), 'account_3');
        await user.click(screen.getByRole('button', { name: 'Search' }));
        await waitFor(() => expect(screen.getByText('3 total alerts')).toBeInTheDocument());

        await user.click(screen.getByRole('button', { name: /Risk/ }));

        const dataRows = screen.getAllByRole('row').slice(2);
        expect(dataRows[0]).toHaveTextContent('95%');
        expect(dataRows[2]).toHaveTextContent('20%');
    });
});
