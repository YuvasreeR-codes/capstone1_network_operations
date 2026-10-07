import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer
} from "recharts";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;


function GridExplorer() {

    const [searchParams] = useSearchParams();
    const [gridId, setGridId] = useState("");
    const [date, setDate] = useState("");
    const [hour, setHour] = useState("");
    const [asOf, setAsOf] = useState("");

    const [data, setData] = useState(null);

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    useEffect(() => {

    const selectedGrid = searchParams.get("grid_id");

    if (selectedGrid) {
        setGridId(selectedGrid);
    }

}, [searchParams]);

    async function searchGrid(event) {

        event.preventDefault();

        // -----------------------------
        // Validate Grid ID
        // -----------------------------

        if (!gridId.trim()) {
            setError("Please enter a grid ID.");
            setData(null);
            return;
        }


        // -----------------------------
        // Validate hour on frontend
        // -----------------------------

        if (hour.trim() !== "") {

            const hourNumber = Number(hour);

            if (
                !Number.isInteger(hourNumber) ||
                hourNumber < 0 ||
                hourNumber > 23
            ) {
                setError("Hour must be between 0 and 23.");
                setData(null);
                return;
            }
        }


        try {

            setLoading(true);
            setError(null);
            setData(null);


            // -----------------------------
            // Build endpoint
            // -----------------------------

            let url =
                `${API_BASE_URL}/network/grid/${encodeURIComponent(
                    gridId.trim()
                )}`;


            // -----------------------------
            // Build query parameters
            // -----------------------------

            const params = new URLSearchParams();


            if (date.trim() !== "") {
                params.append("date", date.trim());
            }


            if (hour.trim() !== "") {
                params.append("hour", hour.trim());
            }


            if (asOf.trim() !== "") {
                params.append("as_of", asOf.trim());
            }


            const queryString = params.toString();


            if (queryString) {
                url += `?${queryString}`;
            }


            // -----------------------------
            // API call
            // -----------------------------

            const response = await fetch(url);


            // -----------------------------
            // Handle API errors
            // -----------------------------

            if (!response.ok) {

                let errorMessage =
                    `Request failed with status ${response.status}`;

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


            // -----------------------------
            // Success
            // -----------------------------

            const result = await response.json();

            setData(result);

        } catch (err) {

            setError(err.message);

        } finally {

            setLoading(false);

        }
    }


    function clearSearch() {

        setGridId("");
        setDate("");
        setHour("");
        setAsOf("");

        setData(null);
        setError(null);
    }


    // ---------------------------------------
    // Format timestamp for chart
    // ---------------------------------------

    function formatChartTime(timestamp) {

        const dateObject = new Date(timestamp);

        return dateObject.toLocaleTimeString(
            [],
            {
                hour: "2-digit",
                minute: "2-digit"
            }
        );
    }


    return (

        <div className="grid-explorer-page">

            <style>{`

                .grid-explorer-page {
                    padding: 40px;
                    min-height: 100vh;
                    background: #f5f6f8;
                    font-family: Arial, sans-serif;
                }


                .grid-explorer-page h1 {
                    margin-bottom: 8px;
                }


                .description {
                    color: #666;
                    margin-bottom: 30px;
                }


                .search-form {
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                }


                .form-grid {
                    display: grid;
                    grid-template-columns:
                        repeat(4, minmax(150px, 1fr));
                    gap: 15px;
                }


                .form-field {
                    display: flex;
                    flex-direction: column;
                    gap: 7px;
                }


                .form-field label {
                    font-weight: bold;
                    font-size: 14px;
                }


                .form-field input {
                    padding: 10px;
                    border: 1px solid #ccc;
                    border-radius: 6px;
                    font-size: 14px;
                }


                .help-text {
                    margin-top: 15px;
                    color: #777;
                    font-size: 13px;
                }


                .button-row {
                    display: flex;
                    gap: 10px;
                    margin-top: 20px;
                }


                .button-row button {
                    padding: 10px 20px;
                    border: none;
                    border-radius: 6px;
                    cursor: pointer;
                }


                .search-button {
                    background: #222;
                    color: white;
                }


                .clear-button {
                    background: #ddd;
                    color: #222;
                }


                .status {
                    padding: 20px;
                    background: white;
                    border-radius: 10px;
                }


                .error {
                    padding: 20px;
                    background: #ffe5e5;
                    border-radius: 10px;
                    margin-bottom: 25px;
                }


                .error h3 {
                    margin-top: 0;
                }


                .grid-info {
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    margin-bottom: 25px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                }


                .grid-info h2 {
                    margin-top: 0;
                }


                .info-row {
                    display: flex;
                    gap: 40px;
                    flex-wrap: wrap;
                }


                .info-item {
                    display: flex;
                    flex-direction: column;
                    gap: 5px;
                }


                .info-label {
                    font-size: 13px;
                    color: #777;
                }


                .info-value {
                    font-weight: bold;
                }


                .chart-container {
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    margin-bottom: 25px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                }


                .chart-wrapper {
                    width: 100%;
                    height: 400px;
                }


                .table-container {
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    overflow-x: auto;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                }


                .activity-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                    min-width: 1000px;
                }


                .activity-table th,
                .activity-table td {
                    padding: 10px;
                    border-bottom: 1px solid #ddd;
                    text-align: right;
                }


                .activity-table th {
                    background: #f5f6f8;
                }


                .activity-table th:first-child,
                .activity-table td:first-child {
                    text-align: left;
                }


                @media (max-width: 900px) {

                    .form-grid {
                        grid-template-columns: 1fr 1fr;
                    }

                }


                @media (max-width: 600px) {

                    .grid-explorer-page {
                        padding: 20px;
                    }

                    .form-grid {
                        grid-template-columns: 1fr;
                    }

                }

            `}</style>


            {/* ===================================== */}
            {/* PAGE HEADER */}
            {/* ===================================== */}

            <h1>Grid Explorer</h1>

            <p className="description">
                Enter a grid ID to view its recent network activity.
            </p>


            {/* ===================================== */}
            {/* SEARCH / FILTER FORM */}
            {/* ===================================== */}

            <form
                className="search-form"
                onSubmit={searchGrid}
            >

                <div className="form-grid">


                    {/* GRID ID */}

                    <div className="form-field">

                        <label htmlFor="grid-id">
                            Grid ID *
                        </label>

                        <input
                            id="grid-id"
                            type="text"
                            value={gridId}
                            onChange={(event) =>
                                setGridId(event.target.value)
                            }
                            placeholder="e.g. 5239"
                        />

                    </div>


                    {/* DATE */}

                    <div className="form-field">

                        <label htmlFor="grid-date">
                            Date
                        </label>

                        <input
                            id="grid-date"
                            type="text"
                            value={date}
                            onChange={(event) =>
                                setDate(event.target.value)
                            }
                            placeholder="YYYY-MM-DD"
                        />

                    </div>


                    {/* HOUR */}

                    <div className="form-field">

                        <label htmlFor="grid-hour">
                            Hour
                        </label>

                        <input
                            id="grid-hour"
                            type="text"
                            value={hour}
                            onChange={(event) =>
                                setHour(event.target.value)
                            }
                            placeholder="0-23"
                        />

                    </div>


                    {/* AS OF */}

                    <div className="form-field">

                        <label htmlFor="grid-as-of">
                            As of
                        </label>

                        <input
                            id="grid-as-of"
                            type="text"
                            value={asOf}
                            onChange={(event) =>
                                setAsOf(event.target.value)
                            }
                            placeholder="YYYY-MM-DDTHH:MM:SS"
                        />

                    </div>

                </div>


                <p className="help-text">
                    Leave Date, Hour and As of empty to view the
                    latest available trailing 24-hour activity.
                </p>


                <div className="button-row">

                    <button
                        type="submit"
                        className="search-button"
                    >
                        Search Grid
                    </button>


                    <button
                        type="button"
                        className="clear-button"
                        onClick={clearSearch}
                    >
                        Clear
                    </button>

                </div>

            </form>


            {/* ===================================== */}
            {/* LOADING STATE */}
            {/* ===================================== */}

            {loading && (

                <div className="status">

                    <p>
                        Loading grid activity...
                    </p>

                </div>

            )}


            {/* ===================================== */}
            {/* ERROR STATE */}
            {/* ===================================== */}

            {!loading && error && (

                <div className="error">

                    <h3>
                        Unable to load grid activity
                    </h3>

                    <p>
                        {error}
                    </p>

                </div>

            )}


            {/* ===================================== */}
            {/* SUCCESS STATE */}
            {/* ===================================== */}

            {!loading && !error && data && (

                <>

                    {/* ================================= */}
                    {/* GRID INFORMATION */}
                    {/* ================================= */}

                    <div className="grid-info">

                        <h2>
                            Grid {data.grid_id}
                        </h2>


                        <div className="info-row">


                            <div className="info-item">

                                <span className="info-label">
                                    As of
                                </span>

                                <span className="info-value">
                                    {data.as_of}
                                </span>

                            </div>


                            <div className="info-item">

                                <span className="info-label">
                                    Start Time
                                </span>

                                <span className="info-value">
                                    {data.start_time}
                                </span>

                            </div>


                            <div className="info-item">

                                <span className="info-label">
                                    End Time
                                </span>

                                <span className="info-value">
                                    {data.end_time}
                                </span>

                            </div>


                            <div className="info-item">

                                <span className="info-label">
                                    Data Points
                                </span>

                                <span className="info-value">
                                    {data.data.length}
                                </span>

                            </div>

                        </div>

                    </div>


                    {/* ================================= */}
                    {/* CHART */}
                    {/* ================================= */}

                    <div className="chart-container">

                        <h2>
                            Activity Over Time
                        </h2>


                        <div className="chart-wrapper">

                            <ResponsiveContainer
                                width="100%"
                                height="100%"
                            >

                                <LineChart
                                    data={data.data}
                                    margin={{
                                        top: 10,
                                        right: 30,
                                        left: 20,
                                        bottom: 10
                                    }}
                                >

                                    <CartesianGrid
                                        strokeDasharray="3 3"
                                    />


                                    <XAxis
                                        dataKey="timestamp"
                                        tickFormatter={
                                            formatChartTime
                                        }
                                    />


                                    <YAxis />


                                    <Tooltip
                                        labelFormatter={
                                            (value) =>
                                                new Date(value)
                                                    .toLocaleString()
                                        }
                                    />


                                    <Legend />


                                    <Line
                                        type="monotone"
                                        dataKey="total_calls"
                                        name="Calls"
                                        dot={false}
                                    />


                                    <Line
                                        type="monotone"
                                        dataKey="total_sms"
                                        name="SMS"
                                        dot={false}
                                    />


                                    <Line
                                        type="monotone"
                                        dataKey="internet_activity"
                                        name="Internet"
                                        dot={false}
                                    />


                                    <Line
                                        type="monotone"
                                        dataKey="total_activity"
                                        name="Total Activity"
                                        dot={false}
                                    />

                                </LineChart>

                            </ResponsiveContainer>

                        </div>

                    </div>


                    {/* ================================= */}
                    {/* TABLE */}
                    {/* ================================= */}

                    <div className="table-container">

                        <h2>
                            Activity Details
                        </h2>


                        <table className="activity-table">

                            <thead>

                                <tr>

                                    <th>Timestamp</th>

                                    <th>Hour</th>

                                    <th>SMS</th>

                                    <th>Calls</th>

                                    <th>Internet</th>

                                    <th>Total Activity</th>

                                </tr>

                            </thead>


                            <tbody>

                                {data.data.map(
                                    (point, index) => (

                                        <tr key={index}>

                                            <td>
                                                {point.timestamp}
                                            </td>

                                            <td>
                                                {point.hour}
                                            </td>

                                            <td>
                                                {point.total_sms}
                                            </td>

                                            <td>
                                                {point.total_calls}
                                            </td>

                                            <td>
                                                {point.internet_activity}
                                            </td>

                                            <td>
                                                {point.total_activity}
                                            </td>

                                        </tr>

                                    )
                                )}

                            </tbody>

                        </table>

                    </div>

                </>

            )}

        </div>
    );
}


export default GridExplorer;