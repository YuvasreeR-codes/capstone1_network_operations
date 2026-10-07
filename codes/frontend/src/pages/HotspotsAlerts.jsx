import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { GeoJSON, MapContainer, TileLayer } from "react-leaflet";
import "leaflet/dist/leaflet.css";

import {
    getHotspots,
    getAlerts
} from "../services/api";


function HotspotsAlerts() {

    const navigate = useNavigate();

    // -----------------------------------------------------
    // API STATE
    // -----------------------------------------------------

    const [hotspots, setHotspots] = useState([]);
    const [alerts, setAlerts] = useState([]);

    const [loadingHotspots, setLoadingHotspots] = useState(false);
    const [loadingAlerts, setLoadingAlerts] = useState(false);

    const [hotspotError, setHotspotError] = useState("");
    const [alertError, setAlertError] = useState("");


    // -----------------------------------------------------
    // FILTER STATE
    // -----------------------------------------------------

    const [hotspotLimit, setHotspotLimit] = useState("10");
    const [alertLimit, setAlertLimit] = useState("10");

    const [severity, setSeverity] = useState("");

    const [asOf, setAsOf] = useState("");


    // -----------------------------------------------------
    // GEOJSON STATE
    // -----------------------------------------------------

    const [geoJson, setGeoJson] = useState(null);
    const [geoJsonError, setGeoJsonError] = useState("");
    const [geoJsonLoading, setGeoJsonLoading] = useState(true);


    // -----------------------------------------------------
    // SELECTED GRID
    // -----------------------------------------------------

    const [selectedGrid, setSelectedGrid] = useState(null);


    // -----------------------------------------------------
    // LOAD GEOJSON ONCE
    // -----------------------------------------------------

    useEffect(() => {

        async function loadGeoJson() {

            try {

                setGeoJsonLoading(true);
                setGeoJsonError("");

                const response = await fetch(
                    "/reference/milano-grid.geojson"
                );

                if (!response.ok) {
                    throw new Error(
                        `Unable to load Milan GeoJSON (${response.status})`
                    );
                }

                const data = await response.json();

                setGeoJson(data);

            } catch (error) {

                setGeoJsonError(error.message);

            } finally {

                setGeoJsonLoading(false);

            }
        }

        loadGeoJson();

    }, []);


    // -----------------------------------------------------
    // LOAD HOTSPOTS
    // -----------------------------------------------------

    async function loadHotspots() {

        try {

            setLoadingHotspots(true);
            setHotspotError("");

            const result = await getHotspots(
                Number(hotspotLimit),
                asOf
            );

            setHotspots(result.data || []);

        } catch (error) {

            setHotspotError(error.message);
            setHotspots([]);

        } finally {

            setLoadingHotspots(false);

        }
    }


    // -----------------------------------------------------
    // LOAD ALERTS
    // -----------------------------------------------------

    async function loadAlerts() {

        try {

            setLoadingAlerts(true);
            setAlertError("");

            const result = await getAlerts(
                Number(alertLimit),
                severity,
                asOf
            );

            setAlerts(result.data || []);

        } catch (error) {

            setAlertError(error.message);
            setAlerts([]);

        } finally {

            setLoadingAlerts(false);

        }
    }


    // -----------------------------------------------------
    // INITIAL API LOAD
    // -----------------------------------------------------

    useEffect(() => {

        loadHotspots();
        loadAlerts();

    }, []);


    // -----------------------------------------------------
    // APPLY FILTERS
    // -----------------------------------------------------

    function handleApply() {

        loadHotspots();
        loadAlerts();

    }


    // -----------------------------------------------------
    // CLEAR FILTERS
    // -----------------------------------------------------

    function handleClear() {

        setHotspotLimit("10");
        setAlertLimit("10");
        setSeverity("");
        setAsOf("");

        getHotspots(10, "")
            .then(result => {
                setHotspots(result.data || []);
                setHotspotError("");
            })
            .catch(error => {
                setHotspotError(error.message);
                setHotspots([]);
            });

        getAlerts(10, "", "")
            .then(result => {
                setAlerts(result.data || []);
                setAlertError("");
            })
            .catch(error => {
                setAlertError(error.message);
                setAlerts([]);
            });

    }


    // -----------------------------------------------------
    // MAP STATUS BY GRID
    // -----------------------------------------------------
    //
    // HOTSPOT = ORANGE
    // ALERT   = RED
    //
    // If a grid appears in both, ALERT wins.
    // -----------------------------------------------------

    const gridStatus = useMemo(() => {

        const statusMap = new Map();


        // -------------------------
        // HOTSPOTS
        // -------------------------

        hotspots.forEach(item => {

            const gridId = String(item.grid_id);

            statusMap.set(gridId, {
                type: "HOTSPOT",
                severity: String(
                    item.severity || ""
                ).toUpperCase()
            });

        });


        // -------------------------
        // ALERTS
        // -------------------------

        alerts.forEach(item => {

            const gridId = String(item.grid_id);

            statusMap.set(gridId, {
                type: "ALERT",
                severity: String(
                    item.severity || ""
                ).toUpperCase()
            });

        });


        return statusMap;

    }, [hotspots, alerts]);


    // -----------------------------------------------------
    // MAP POLYGON STYLE
    // -----------------------------------------------------

    function getPolygonStyle(feature) {

        const gridId = String(
            feature?.properties?.cellId
        );

        const info = gridStatus.get(gridId);


        // -------------------------
        // NORMAL
        // -------------------------

        if (!info) {

            return {
                color: "#166534",
                fillColor: "#22c55e",
                fillOpacity: 0.15,
                weight: 1
            };

        }


        // -------------------------
        // ALERT → RED
        // -------------------------

        if (info.type === "ALERT") {

            return {
                color: "#991b1b",
                fillColor: "#ef4444",
                fillOpacity: 0.65,
                weight: 2
            };

        }


        // -------------------------
        // HOTSPOT → ORANGE
        // -------------------------

        if (info.type === "HOTSPOT") {

            return {
                color: "#9a3412",
                fillColor: "#f97316",
                fillOpacity: 0.65,
                weight: 2
            };

        }


        // Fallback

        return {
            color: "#166534",
            fillColor: "#22c55e",
            fillOpacity: 0.15,
            weight: 1
        };

    }


    // -----------------------------------------------------
    // MAP POLYGON EVENTS
    // -----------------------------------------------------

    // function onEachFeature(feature, layer) {

    //     const cellId = feature?.properties?.cellId;

    //     if (cellId === undefined) {
    //         return;
    //     }

    //     const gridId = String(cellId);

    //     const info = gridStatus.get(gridId);


    //     // -------------------------
    //     // TOOLTIP TEXT
    //     // -------------------------

    //     let tooltipText = `Grid ${gridId} — NORMAL`;

    //     if (info) {

    //         tooltipText =
    //             `Grid ${gridId} — ${info.type} (${info.severity})`;

    //     }


    //     layer.bindTooltip(
    //         tooltipText,
    //         {
    //             sticky: true
    //         }
    //     );


    //     // -------------------------
    //     // EVENTS
    //     // -------------------------

    //     layer.on({

    //         click: () => {

    //             setSelectedGrid(gridId);

    //         },


    //         mouseover: event => {

    //             event.target.setStyle({

    //                 weight: 3,
    //                 fillOpacity: 0.8

    //             });

    //             event.target.bringToFront();

    //         },


    //         mouseout: event => {

    //             event.target.setStyle(
    //                 getPolygonStyle(feature)
    //             );

    //         }

    //     });

    // }
    function onEachFeature(feature, layer) {
    const cellId = feature?.properties?.cellId;

    if (cellId === undefined) {
        return;
    }

    const gridId = String(cellId);
    const info = gridStatus.get(gridId);

    let tooltipText = `Grid ${gridId} — NORMAL`;

    if (info) {
        tooltipText =
            `Grid ${gridId} — ${info.type} (${info.severity})`;
    }

    layer.on({

        mouseover: (event) => {

            // close any tooltip already open
            layer._map.eachLayer((l) => {
                if (l.closeTooltip) {
                    l.closeTooltip();
                }
            });

            event.target.bindTooltip(tooltipText, {
                sticky: true
            }).openTooltip();

            event.target.setStyle({
                weight: 3,
                fillOpacity: 0.8
            });

            event.target.bringToFront();
        },

        mouseout: (event) => {

            event.target.closeTooltip();

            event.target.setStyle(
                getPolygonStyle(feature)
            );
        },

        click: () => {
            setSelectedGrid(gridId);
        }

    });
}


    // -----------------------------------------------------
    // OPEN GRID EXPLORER
    // -----------------------------------------------------

    function openGridExplorer() {

        if (!selectedGrid) {
            return;
        }

        navigate(
            `/grid-explorer?grid_id=${encodeURIComponent(
                selectedGrid
            )}`
        );

    }


    // -----------------------------------------------------
    // FORMAT TIMESTAMP
    // -----------------------------------------------------

    function formatTimestamp(timestamp) {

        if (!timestamp) {
            return "-";
        }

        return new Date(timestamp).toLocaleString();

    }


    // -----------------------------------------------------
    // RENDER
    // -----------------------------------------------------

    return (

        <div className="page">

            <style>{`

                * {
                    box-sizing: border-box;
                }

                body {
                    margin: 0;
                }

                .page {
                    min-height: 100vh;
                    padding: 30px;
                    background: #f5f7fb;
                    font-family: Arial, sans-serif;
                    color: #1f2937;
                }

                .header {
                    margin-bottom: 24px;
                }

                .header h1 {
                    margin: 0 0 8px;
                    font-size: 30px;
                }

                .header p {
                    margin: 0;
                    color: #6b7280;
                }

                .filters {
                    background: white;
                    padding: 20px;
                    border-radius: 12px;
                    margin-bottom: 24px;
                    display: flex;
                    flex-wrap: wrap;
                    gap: 16px;
                    align-items: end;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
                }

                .filter-group {
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .filter-group label {
                    font-size: 13px;
                    font-weight: 600;
                }

                .filter-group input,
                .filter-group select {
                    padding: 9px 11px;
                    border: 1px solid #d1d5db;
                    border-radius: 7px;
                    min-width: 130px;
                    font-size: 14px;
                }

                .filter-group .asof {
                    min-width: 220px;
                }

                button {
                    border: none;
                    border-radius: 7px;
                    padding: 10px 16px;
                    cursor: pointer;
                    font-weight: 600;
                }

                .apply-button {
                    background: #2563eb;
                    color: white;
                }

                .clear-button {
                    background: #e5e7eb;
                    color: #111827;
                }

                .content-grid {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 20px;
                    margin-bottom: 24px;
                }

                .card {
                    background: white;
                    border-radius: 12px;
                    padding: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
                    overflow-x: auto;
                }

                .card h2 {
                    margin-top: 0;
                    margin-bottom: 16px;
                    font-size: 20px;
                }

                .error {
                    background: #fee2e2;
                    color: #991b1b;
                    padding: 12px;
                    border-radius: 7px;
                    margin-bottom: 12px;
                }

                table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 13px;
                }

                th,
                td {
                    padding: 10px 8px;
                    text-align: left;
                    border-bottom: 1px solid #e5e7eb;
                    white-space: nowrap;
                }

                th {
                    background: #f9fafb;
                    font-weight: 700;
                }

                .severity {
                    font-weight: 700;
                }

                .hotspot-label {
                    display: inline-block;
                    padding: 4px 9px;
                    border-radius: 999px;
                    background: #ffedd5;
                    color: #9a3412;
                    font-size: 11px;
                    font-weight: 700;
                }

                .alert-label {
                    display: inline-block;
                    padding: 4px 9px;
                    border-radius: 999px;
                    background: #fee2e2;
                    color: #991b1b;
                    font-size: 11px;
                    font-weight: 700;
                }

                .map-card {
                    background: white;
                    border-radius: 12px;
                    padding: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
                }

                .map-card h2 {
                    margin-top: 0;
                }

                .map-container {
                    height: 650px;
                    width: 100%;
                    border-radius: 10px;
                    overflow: hidden;
                }

                .legend {
                    display: flex;
                    gap: 20px;
                    flex-wrap: wrap;
                    margin-bottom: 14px;
                    font-size: 13px;
                }

                .legend-item {
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }

                .legend-box {
                    width: 18px;
                    height: 18px;
                    border-radius: 3px;
                    border: 1px solid #999;
                }

                .legend-normal {
                    background: #22c55e;
                }

                .legend-hotspot {
                    background: #f97316;
                }

                .legend-alert {
                    background: #ef4444;
                }

                .selected-grid {
                    margin-top: 14px;
                    padding: 14px;
                    background: #eff6ff;
                    border-radius: 8px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 12px;
                }

                .grid-button {
                    background: #2563eb;
                    color: white;
                }

                .loading {
                    color: #6b7280;
                    padding: 10px 0;
                }

                @media (max-width: 1000px) {

                    .content-grid {
                        grid-template-columns: 1fr;
                    }

                }

            `}</style>


            {/* -------------------------------------------------
                HEADER
            ------------------------------------------------- */}

            <div className="header">

                <h1>
                    Hotspots & Alerts
                </h1>

                <p>
                    Prioritized network activity and operational
                    attention areas across Milan.
                </p>

            </div>


            {/* -------------------------------------------------
                FILTERS
            ------------------------------------------------- */}

            <div className="filters">

                <div className="filter-group">

                    <label>
                        Hotspot Limit
                    </label>

                    <input
                        type="number"
                        min="1"
                        max="100"
                        value={hotspotLimit}
                        onChange={e =>
                            setHotspotLimit(e.target.value)
                        }
                    />

                </div>


                <div className="filter-group">

                    <label>
                        Alert Limit
                    </label>

                    <input
                        type="number"
                        min="1"
                        max="100"
                        value={alertLimit}
                        onChange={e =>
                            setAlertLimit(e.target.value)
                        }
                    />

                </div>


                <div className="filter-group">

                    <label>
                        Minimum Severity
                    </label>

                    <select
                        value={severity}
                        onChange={e =>
                            setSeverity(e.target.value)
                        }
                    >

                        <option value="">
                            All
                        </option>

                        <option value="MEDIUM">
                            MEDIUM
                        </option>

                        <option value="HIGH">
                            HIGH
                        </option>

                    </select>

                </div>


                <div className="filter-group">

                    <label>
                        As Of
                    </label>

                    <input
                        className="asof"
                        type="text"
                        placeholder="YYYY-MM-DDTHH:MM:SS"
                        value={asOf}
                        onChange={e =>
                            setAsOf(e.target.value)
                        }
                    />

                </div>


                <button
                    className="apply-button"
                    onClick={handleApply}
                >
                    Apply
                </button>


                <button
                    className="clear-button"
                    onClick={handleClear}
                >
                    Clear
                </button>

            </div>


            {/* -------------------------------------------------
                TABLES
            ------------------------------------------------- */}

            <div className="content-grid">


                {/* HOTSPOTS */}

                <div className="card">

                    <h2>
                        🔥 Hotspots
                    </h2>

                    {hotspotError && (
                        <div className="error">
                            {hotspotError}
                        </div>
                    )}

                    {loadingHotspots ? (

                        <div className="loading">
                            Loading hotspots...
                        </div>

                    ) : hotspots.length === 0 ? (

                        <div className="loading">
                            No hotspots found.
                        </div>

                    ) : (

                        <table>

                            <thead>
    
    <tr>
        <th>Rank</th>
        <th>Grid</th>
        <th>Activity</th>
        <th>Risk Score</th>
        <th>Risk Level</th>
        <th>Type</th>
        <th>Severity</th>
        <th>Timestamp</th>
    </tr>
</thead>

                            <tbody>

                                {hotspots.map((item, index) => (

                                    <tr
                                        key={`${item.grid_id}-${item.timestamp}`}
                                    >

                                        <td>
                                            {index + 1}
                                        </td>

                                        <td>
                                            {item.grid_id}
                                        </td>

                                        <td>
                                            {Number(
                                                item.total_activity || 0
                                            ).toLocaleString()}
                                        </td>
                                         <td>
        {item.risk_score != null
            ? Number(item.risk_score).toFixed(3)
            : "-"}
    </td>

    <td>
        {item.risk_level || "-"}
    </td>

                                        <td>

                                            <span className="hotspot-label">
                                                HOTSPOT
                                            </span>

                                        </td>

                                        <td className="severity">
                                            {item.severity}
                                        </td>

                                        <td>
                                            {formatTimestamp(
                                                item.timestamp
                                            )}
                                        </td>

                                    </tr>

                                ))}

                            </tbody>

                        </table>

                    )}

                </div>


                {/* ALERTS */}

                <div className="card">

                    <h2>
                        🚨 Alerts
                    </h2>

                    {alertError && (
                        <div className="error">
                            {alertError}
                        </div>
                    )}

                    {loadingAlerts ? (

                        <div className="loading">
                            Loading alerts...
                        </div>

                    ) : alerts.length === 0 ? (

                        <div className="loading">
                            No alerts found.
                        </div>

                    ) : (

                        <table>

                            <thead>
    <tr>
        <th>Grid</th>
        <th>Alert</th>
        <th>Current</th>
        <th>Baseline</th>
        <th>Risk Score</th>
        <th>Risk Level</th>
        <th>Type</th>
        <th>Severity</th>
        <th>Timestamp</th>
    </tr>
</thead>

                            <tbody>

                                {alerts.map(item => (

                                    <tr
                                        key={`${item.grid_id}-${item.timestamp}-${item.alert_type}`}
                                    >

                                        <td>
                                            {item.grid_id}
                                        </td>

                                        <td>
                                            {item.alert_type}
                                        </td>

                                        <td>
                                            {Number(
                                                item.current_activity || 0
                                            ).toLocaleString()}
                                        </td>

                                        <td>
                                            {Number(
                                                item.baseline_activity || 0
                                            ).toLocaleString()}
                                        </td>
                                        <td>
    {item.risk_score != null
        ? Number(item.risk_score).toFixed(3)
        : "-"}
</td>

<td>
    {item.risk_level || "-"}
</td>

                                        <td>

                                            <span className="alert-label">
                                                ALERT
                                            </span>

                                        </td>

                                        <td className="severity">
                                            {item.severity}
                                        </td>

                                        <td>
                                            {formatTimestamp(
                                                item.timestamp
                                            )}
                                        </td>

                                    </tr>

                                ))}

                            </tbody>

                        </table>

                    )}

                </div>

            </div>


            {/* -------------------------------------------------
                MAP
            ------------------------------------------------- */}

            <div className="map-card">

                <h2>
                    Milan Network Map
                </h2>


                {/* LEGEND */}

                <div className="legend">

                    <div className="legend-item">

                        <div className="legend-box legend-normal" />

                        NORMAL

                    </div>


                    <div className="legend-item">

                        <div className="legend-box legend-hotspot" />

                        HOTSPOT

                    </div>


                    <div className="legend-item">

                        <div className="legend-box legend-alert" />

                        ALERT

                    </div>

                </div>


                {geoJsonError && (

                    <div className="error">
                        {geoJsonError}
                    </div>

                )}


                {geoJsonLoading ? (

                    <div className="loading">
                        Loading Milan grid map...
                    </div>

                ) : geoJson ? (

                    <MapContainer
                        className="map-container"
                        center={[45.4642, 9.1900]}
                        zoom={11}
                        scrollWheelZoom={true}
                    >

                        <TileLayer
                            attribution="&copy; OpenStreetMap contributors"
                            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                        />


                        <GeoJSON
                            key={JSON.stringify(
                                Array.from(
                                    gridStatus.entries()
                                )
                            )}
                            data={geoJson}
                            style={getPolygonStyle}
                            onEachFeature={onEachFeature}
                        />

                    </MapContainer>

                ) : null}


                {/* SELECTED GRID */}

                {selectedGrid && (

                    <div className="selected-grid">

                        <div>

                            <strong>
                                Selected Grid:
                            </strong>{" "}

                            {selectedGrid}

                        </div>


                        <button
                            className="grid-button"
                            onClick={openGridExplorer}
                        >
                            Open Grid Explorer
                        </button>

                    </div>

                )}

            </div>

        </div>

    );

}


export default HotspotsAlerts;