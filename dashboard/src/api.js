let statsPromise = null;

export function getStatsPromise() {
    if (!statsPromise) {
        statsPromise = fetch('/api/alerts/stats')
        .then((res) => res.json());
    }

    return statsPromise;
}