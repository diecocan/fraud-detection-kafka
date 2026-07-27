import '@testing-library/jest-dom/vitest';

// jsdom doesn't implement ResizeObserver, which recharts' ResponsiveContainer needs.
globalThis.ResizeObserver = class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
};
