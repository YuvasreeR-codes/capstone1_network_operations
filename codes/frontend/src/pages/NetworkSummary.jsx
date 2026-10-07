import { useEffect, useState } from "react";
import { getNetworkSummary } from "../services/api";

function NetworkSummary() {

    const [data, setData] = useState(null);
    const [asOf, setAsOf] = useState("");
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);


    async function fetchSummary(selectedAsOf = "") {

        try {
            setLoading(true);
            setError(null);

            const result = await getNetworkSummary(selectedAsOf);

            setData(result);

        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }


    // Load latest summary when page opens
    useEffect(() => {
        fetchSummary();
    }, []);


    function handleSubmit(event) {

        event.preventDefault();

        fetchSummary(asOf);
    }


    function handleClear() {

        setAsOf("");

        // Fetch latest available summary
        fetchSummary();
    }


    return (
        <div className="network-summary-page">

            <style>{`

                .network-summary-page {
                    padding: 40px;
                    min-height: 100vh;
                    background: #f5f6f8;
                    font-family: Arial, sans-serif;
                }

                .network-summary-page h1 {
                    margin-bottom: 25px;
                }

                .date-filter {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    margin-bottom: 30px;
                }

                .date-filter label {
                    font-weight: bold;
                }

                .date-filter input {
                    width: 250px;
                    padding: 10px;
                    border: 1px solid #ccc;
                    border-radius: 6px;
                    font-size: 14px;
                }

                .date-filter button {
                    padding: 10px 18px;
                    border: none;
                    border-radius: 6px;
                    cursor: pointer;
                    background: #222;
                    color: white;
                }

                .date-filter button:hover {
                    opacity: 0.85;
                }

                .status {
                    padding: 20px;
                    background: white;
                    border-radius: 10px;
                }

                .error {
                    padding: 20px;
                    background: #ffe5e5;
                    border-radius: 8px;
                    margin-top: 20px;
                }

                .summary-grid {
                    display: grid;
                    grid-template-columns: repeat(4, 1fr);
                    gap: 20px;
                }

                .summary-card {
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                }

                .summary-card h3 {
                    margin-top: 0;
                    font-size: 16px;
                }

                .summary-card p {
                    font-size: 28px;
                    font-weight: bold;
                    margin-bottom: 0;
                }

                .summary-meta {
                    margin-top: 25px;
                    color: #666;
                }

            `}</style>


            <h1>Network Summary</h1>


            {/* AS_OF INPUT */}

            <form
                className="date-filter"
                onSubmit={handleSubmit}
            >

                <label htmlFor="as-of">
                    As of:
                </label>

                <input
                    id="as-of"
                    type="text"
                    value={asOf}
                    onChange={(event) => setAsOf(event.target.value)}
                    placeholder="YYYY-MM-DDTHH:MM:SS"
                />

                <button type="submit">
                    Apply
                </button>

                <button
                    type="button"
                    onClick={handleClear}
                >
                    Clear
                </button>

            </form>


            {/* LOADING */}

            {loading && (
                <div className="status">
                    <p>Loading network summary...</p>
                </div>
            )}


            {/* ERROR */}

            {!loading && error && (
                <div className="error">

                    <h3>
                        Unable to load network summary
                    </h3>

                    <p>
                        {error}
                    </p>

                </div>
            )}


            {/* SUCCESS */}

            {!loading && !error && data && (

                <>

                    <div className="summary-grid">

                        <div className="summary-card">
                            <h3>Total Activity</h3>
                            <p>{data.total_activity}</p>
                        </div>


                        <div className="summary-card">
                            <h3>Active Grids</h3>
                            <p>{data.active_grids}</p>
                        </div>


                        <div className="summary-card">
                            <h3>Peak Hour</h3>
                            <p>{data.peak_hour}</p>
                        </div>


                        <div className="summary-card">
                            <h3>Top Grid</h3>
                            <p>{data.top_grid}</p>
                        </div>

                    </div>


                    <div className="summary-meta">

                        <p>
                            As of: {data.as_of}
                        </p>

                    </div>

                </>

            )}

        </div>
    );
}

export default NetworkSummary;