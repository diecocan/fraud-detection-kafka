export function riskClass(score) {
    if (score >= 0.9) return 'risk-critical';
    if (score >= 0.7) return 'risk-warning';
    return 'risk-caution';
}

export function relativeTime(timeStamp) {
    const seconds = Math.floor((Date.now() - new Date(timeStamp).getTime()) / 1000);

    if (seconds < 60) return `${seconds}s ago`;

    const minutes = Math.floor(seconds / 60);

    if (minutes < 60) return `${minutes}m ago`;

    const hours = Math.floor(minutes / 60);
    
    return `${hours}h ago`;
}