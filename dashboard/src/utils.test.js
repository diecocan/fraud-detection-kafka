import { describe, it, expect, vi, afterEach } from 'vitest';
import { riskClass, relativeTime } from './utils';

describe('riskClass', () => {
    it('returns risk-critical at or above 0.9', () => {
        expect(riskClass(0.9)).toBe('risk-critical');
        expect(riskClass(0.95)).toBe('risk-critical');
    });

    it('returns risk-warning between 0.7 and 0.9', () => {
        expect(riskClass(0.7)).toBe('risk-warning');
        expect(riskClass(0.89)).toBe('risk-warning');
    });

    it('returns risk-caution below 0.7', () => {
        expect(riskClass(0.69)).toBe('risk-caution');
        expect(riskClass(0)).toBe('risk-caution');
    });
});

describe('relativeTime', () => {
    afterEach(() => {
        vi.useRealTimers();
    });

    it('formats sub-minute durations in seconds', () => {
        vi.useFakeTimers();
        vi.setSystemTime(new Date('2026-01-01T00:01:00Z'));

        expect(relativeTime('2026-01-01T00:00:30Z')).toBe('30s ago');
    });

    it('formats sub-hour durations in minutes', () => {
        vi.useFakeTimers();
        vi.setSystemTime(new Date('2026-01-01T00:10:00Z'));

        expect(relativeTime('2026-01-01T00:00:00Z')).toBe('10m ago');
    });

    it('formats durations of an hour or more in hours', () => {
        vi.useFakeTimers();
        vi.setSystemTime(new Date('2026-01-01T03:00:00Z'));

        expect(relativeTime('2026-01-01T00:00:00Z')).toBe('3h ago');
    });
});
