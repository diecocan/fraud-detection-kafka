import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import AlertFeed from './AlertFeed';

class MockEventSource {
    static instances = [];

    constructor(url) {
        this.url = url;
        this.listeners = {};
        MockEventSource.instances.push(this);
    }

    addEventListener(type, handler) {
        this.listeners[type] = handler;
    }

    close() {
        this.closed = true;
    }

    emitOpen() {
        this.onopen?.();
    }

    emitAlert(alert) {
        this.listeners['alert']?.({ data: JSON.stringify(alert) });
    }

    emitError() {
        this.onerror?.();
    }
}

describe('AlertFeed', () => {
    beforeEach(() => {
        MockEventSource.instances = [];
        vi.stubGlobal('EventSource', MockEventSource);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('connects to the alerts stream endpoint', () => {
        render(<AlertFeed />);
        expect(MockEventSource.instances).toHaveLength(1);
        expect(MockEventSource.instances[0].url).toBe('/api/alerts/stream');
    });

    it('shows Disconnected until the stream opens', () => {
        render(<AlertFeed />);
        expect(screen.getByText('Disconnected')).toBeInTheDocument();
    });

    it('shows Live once the stream opens', () => {
        render(<AlertFeed />);
        act(() => MockEventSource.instances[0].emitOpen());
        expect(screen.getByText('Live')).toBeInTheDocument();
    });

    it('renders an incoming alert', () => {
        render(<AlertFeed />);
        act(() => MockEventSource.instances[0].emitAlert({
            alertId: 'a1',
            accountId: 'account_5',
            reason: 'VELOCITY',
            riskScore: 0.8,
            detectedAt: new Date().toISOString(),
        }));

        expect(screen.getByText('account_5')).toBeInTheDocument();
        expect(screen.getByText('VELOCITY')).toBeInTheDocument();
        expect(screen.getByText('80%')).toBeInTheDocument();
    });

    it('caps the feed at 50 alerts, most recent first', () => {
        render(<AlertFeed />);
        const source = MockEventSource.instances[0];

        act(() => {
            for (let i = 0; i < 55; i++) {
                source.emitAlert({
                    alertId: `a${i}`,
                    accountId: `account_${i}`,
                    reason: 'VELOCITY',
                    riskScore: 0.5,
                    detectedAt: new Date().toISOString(),
                });
            }
        });

        expect(screen.getAllByRole('row')).toHaveLength(1 + 50); // header + 50 alerts
        expect(screen.getByText('account_54')).toBeInTheDocument();
        expect(screen.queryByText('account_0')).not.toBeInTheDocument();
    });

    it('shows Disconnected again on a stream error', () => {
        render(<AlertFeed />);
        act(() => MockEventSource.instances[0].emitOpen());
        expect(screen.getByText('Live')).toBeInTheDocument();

        act(() => MockEventSource.instances[0].emitError());
        expect(screen.getByText('Disconnected')).toBeInTheDocument();
    });
});
