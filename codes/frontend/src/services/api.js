const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

export async function getNetworkSummary(asOf = "") {
    let url = `${API_BASE_URL}/network/summary`;

    if (asOf.trim() !== "") {
        url += `?as_of=${encodeURIComponent(asOf.trim())}`;
    }

    const response = await fetch(url);

    if (!response.ok) {
        let errorMessage = `Request failed with status ${response.status}`;

        try {
            const errorData = await response.json();

            if (errorData.detail) {
                errorMessage = errorData.detail;
            }
        } catch {
            // Keep default error message
        }

        throw new Error(errorMessage);
    }

    return response.json();
}




// ---------------------------------------------------------
// HOTSPOTS
// ---------------------------------------------------------

export async function getHotspots(limit = 10, asOf = "") {
    let url = `${API_BASE_URL}/network/hotspots`;

    const params = new URLSearchParams();

    params.append("limit", limit);

    if (asOf.trim() !== "") {
        params.append("as_of", asOf.trim());
    }

    url += `?${params.toString()}`;

    const response = await fetch(url);

    if (!response.ok) {
        let errorMessage = `Request failed with status ${response.status}`;

        try {
            const errorData = await response.json();

            if (errorData.detail) {
                errorMessage = errorData.detail;
            }
        } catch {
            // Keep default error message
        }

        throw new Error(errorMessage);
    }

    return response.json();
}


// ---------------------------------------------------------
// ALERTS
// ---------------------------------------------------------

export async function getAlerts(
    limit = 10,
    severity = "",
    asOf = ""
) {
    let url = `${API_BASE_URL}/network/alerts`;

    const params = new URLSearchParams();

    params.append("limit", limit);

    if (severity.trim() !== "") {
        params.append("severity", severity.trim());
    }

    if (asOf.trim() !== "") {
        params.append("as_of", asOf.trim());
    }

    url += `?${params.toString()}`;

    const response = await fetch(url);

    if (!response.ok) {
        let errorMessage = `Request failed with status ${response.status}`;

        try {
            const errorData = await response.json();

            if (errorData.detail) {
                errorMessage = errorData.detail;
            }
        } catch {
            // Keep default error message
        }

        throw new Error(errorMessage);
    }

    return response.json();
}


export async function predictNetworkRisk(gridId, featureTimestamp) {

    const url = `${API_BASE_URL}/network/predict-risk`;

    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            grid_id: gridId,
            feature_timestamp: featureTimestamp
        })
    });

    if (!response.ok) {

        let errorMessage =
            `Request failed with status ${response.status}`;

        try {

            const errorData = await response.json();

            if (errorData.detail) {
                errorMessage = errorData.detail;
            }

        } catch {}

        throw new Error(errorMessage);
    }

    return response.json();
}