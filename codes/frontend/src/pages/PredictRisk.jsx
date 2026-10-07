import { useState } from "react";
import { predictNetworkRisk } from "../services/api";


function PredictRisk() {

    // -----------------------------------------------------
    // INPUT STATE
    // -----------------------------------------------------

    const [gridId, setGridId] = useState("");
    const [featureTimestamp, setFeatureTimestamp] = useState("");

    // -----------------------------------------------------
    // RESULT STATE
    // -----------------------------------------------------

    const [result, setResult] = useState(null);

    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");


    // -----------------------------------------------------
    // PREDICT
    // -----------------------------------------------------

    async function handlePredict() {

        setError("");
        setResult(null);


        // Validate grid ID

        if (!gridId.trim()) {

            setError("Please enter a grid ID.");

            return;
        }


        // Validate timestamp

        if (!featureTimestamp.trim()) {

            setError(
                "Please enter a feature timestamp."
            );

            return;
        }


        try {

            setLoading(true);

            const data = await predictNetworkRisk(
                gridId.trim(),
                featureTimestamp.trim()
            );

            setResult(data);

        } catch (error) {

            setError(error.message);

        } finally {

            setLoading(false);

        }

    }


    // -----------------------------------------------------
    // CLEAR
    // -----------------------------------------------------

    function handleClear() {

        setGridId("");
        setFeatureTimestamp("");
        setResult(null);
        setError("");

    }


    // -----------------------------------------------------
    // FORMAT SCORE
    // -----------------------------------------------------

    function formatRiskScore(score) {

        if (score === null || score === undefined) {
            return "-";
        }

        return (Number(score) * 100).toFixed(2) + "%";

    }


    // -----------------------------------------------------
    // RISK CLASS
    // -----------------------------------------------------

    function getRiskClass(level) {

        if (
            String(level).toUpperCase() === "HIGH"
        ) {
            return "risk-high";
        }

        return "risk-low";

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


                .input-card {
                    background: white;
                    padding: 24px;
                    border-radius: 12px;
                    margin-bottom: 24px;
                    box-shadow:
                        0 2px 8px rgba(0,0,0,0.06);
                }


                .input-card h2 {
                    margin-top: 0;
                    margin-bottom: 20px;
                    font-size: 20px;
                }


                .form-grid {
                    display: grid;
                    grid-template-columns:
                        repeat(2, minmax(200px, 1fr));
                    gap: 18px;
                }


                .form-group {
                    display: flex;
                    flex-direction: column;
                    gap: 7px;
                }


                .form-group label {
                    font-size: 13px;
                    font-weight: 600;
                }


                .form-group input {
                    padding: 10px 12px;
                    border: 1px solid #d1d5db;
                    border-radius: 7px;
                    font-size: 14px;
                }


                .form-group input:focus {
                    outline: none;
                    border-color: #2563eb;
                }


                .help-text {
                    margin-top: 6px;
                    font-size: 12px;
                    color: #6b7280;
                }


                .actions {
                    display: flex;
                    gap: 10px;
                    margin-top: 20px;
                }


                button {
                    border: none;
                    border-radius: 7px;
                    padding: 10px 18px;
                    cursor: pointer;
                    font-weight: 600;
                }


                .predict-button {
                    background: #2563eb;
                    color: white;
                }


                .predict-button:hover {
                    background: #1d4ed8;
                }


                .clear-button {
                    background: #e5e7eb;
                    color: #111827;
                }


                .predict-button:disabled {
                    background: #93c5fd;
                    cursor: not-allowed;
                }


                .error {
                    background: #fee2e2;
                    color: #991b1b;
                    padding: 12px;
                    border-radius: 7px;
                    margin-bottom: 20px;
                }


                /* -----------------------------------------
                   MODEL OUTPUT
                ----------------------------------------- */


                .result-card {
                    background: white;
                    padding: 24px;
                    border-radius: 12px;
                    box-shadow:
                        0 2px 8px rgba(0,0,0,0.06);
                    margin-bottom: 24px;
                }


                .result-card h2 {
                    margin-top: 0;
                    margin-bottom: 20px;
                    font-size: 20px;
                }


                .prediction-box {
                    border: 1px solid #e5e7eb;
                    border-radius: 10px;
                    padding: 22px;
                    margin-bottom: 20px;
                }


                .prediction-label {
                    font-size: 13px;
                    color: #6b7280;
                    margin-bottom: 8px;
                }


                .risk-score {
                    font-size: 42px;
                    font-weight: 700;
                    margin-bottom: 12px;
                }


                .risk-level {
                    display: inline-block;
                    padding: 7px 14px;
                    border-radius: 999px;
                    font-size: 13px;
                    font-weight: 700;
                }


                .risk-high {
                    background: #fee2e2;
                    color: #991b1b;
                }


                .risk-low {
                    background: #dcfce7;
                    color: #166534;
                }


                .metadata-grid {
                    display: grid;
                    grid-template-columns:
                        repeat(3, 1fr);
                    gap: 14px;
                }


                .metadata-item {
                    background: #f9fafb;
                    padding: 14px;
                    border-radius: 8px;
                }


                .metadata-label {
                    display: block;
                    font-size: 12px;
                    color: #6b7280;
                    margin-bottom: 5px;
                }


                .metadata-value {
                    font-weight: 600;
                    word-break: break-word;
                }


                /* -----------------------------------------
                   ADDITIONAL MODEL OUTPUT
                ----------------------------------------- */


                .additional-output {
                    border-top: 1px solid #e5e7eb;
                    padding-top: 20px;
                }


                .additional-output h3 {
                    margin-top: 0;
                    margin-bottom: 14px;
                    font-size: 16px;
                }


                .output-grid {
                    display: grid;
                    grid-template-columns:
                        repeat(3, 1fr);
                    gap: 12px;
                }


                .output-item {
                    padding: 12px;
                    background: #f9fafb;
                    border-radius: 7px;
                }


                .output-item strong {
                    display: block;
                    font-size: 12px;
                    color: #6b7280;
                    margin-bottom: 5px;
                }


                /* -----------------------------------------
                   AI EXPLANATION
                ----------------------------------------- */


                .explanation-card {
                    background: white;
                    padding: 24px;
                    border-radius: 12px;
                    box-shadow:
                        0 2px 8px rgba(0,0,0,0.06);
                }


                .explanation-card h2 {
                    margin-top: 0;
                    font-size: 20px;
                }


                .explanation-card p {
                    color: #6b7280;
                    line-height: 1.6;
                }


                .ai-button {
                    background: #111827;
                    color: white;
                    margin-top: 8px;
                }


                .ai-button:hover {
                    background: #1f2937;
                }


                @media (max-width: 800px) {

                    .form-grid,
                    .metadata-grid,
                    .output-grid {
                        grid-template-columns: 1fr;
                    }

                }

            `}</style>


            {/* -------------------------------------------------
                HEADER
            ------------------------------------------------- */}

            <div className="header">

                <h1>
                    Predictive Risk
                </h1>

                <p>
                    Request a model-based risk prediction
                    for a selected network grid and timestamp.
                </p>

            </div>


            {/* -------------------------------------------------
                INPUT
            ------------------------------------------------- */}

            <div className="input-card">

                <h2>
                    Prediction Request
                </h2>


                <div className="form-grid">

                    <div className="form-group">

                        <label>
                            Grid ID
                        </label>

                        <input
                            type="text"
                            placeholder="e.g. 5239"
                            value={gridId}
                            onChange={e =>
                                setGridId(e.target.value)
                            }
                        />

                    </div>


                    <div className="form-group">

                        <label>
                            Feature Timestamp
                        </label>

                        <input
                            type="text"
                            placeholder="YYYY-MM-DDTHH:MM:SS"
                            value={featureTimestamp}
                            onChange={e =>
                                setFeatureTimestamp(
                                    e.target.value
                                )
                            }
                        />

                        <div className="help-text">
                            Example: 2013-11-07T23:00:00
                        </div>

                    </div>

                </div>


                <div className="actions">

                    <button
                        className="predict-button"
                        onClick={handlePredict}
                        disabled={loading}
                    >

                        {loading
                            ? "Predicting..."
                            : "Predict Risk"
                        }

                    </button>


                    <button
                        className="clear-button"
                        onClick={handleClear}
                    >
                        Clear
                    </button>

                </div>

            </div>


            {/* -------------------------------------------------
                ERROR
            ------------------------------------------------- */}

            {error && (

                <div className="error">
                    {error}
                </div>

            )}


            {/* -------------------------------------------------
                MODEL OUTPUT
            ------------------------------------------------- */}

            {result && (

                <div className="result-card">

                    <h2>
                        Model Output
                    </h2>


                    <div className="prediction-box">

                        <div className="prediction-label">
                            ML3 Risk Score
                        </div>


                        <div className="risk-score">

                            {formatRiskScore(
                                result.risk_score
                            )}

                        </div>


                        <span
                            className={
                                `risk-level ${
                                    getRiskClass(
                                        result.risk_level
                                    )
                                }`
                            }
                        >

                            {result.risk_level}

                        </span>

                    </div>


                    {/* -----------------------------------------
                        MODEL METADATA
                    ----------------------------------------- */}

                    <div className="metadata-grid">

                        <div className="metadata-item">

                            <span className="metadata-label">
                                Grid ID
                            </span>

                            <span className="metadata-value">
                                {result.grid_id}
                            </span>

                        </div>


                        <div className="metadata-item">

                            <span className="metadata-label">
                                Feature Timestamp
                            </span>

                            <span className="metadata-value">
                                {result.feature_timestamp}
                            </span>

                        </div>


                        <div className="metadata-item">

                            <span className="metadata-label">
                                Model Version
                            </span>

                            <span className="metadata-value">
                                {result.model_version}
                            </span>

                        </div>

                    </div>


                    {/* -----------------------------------------
                        ADDITIONAL OUTPUT
                    ----------------------------------------- */}

                    <div className="additional-output">

                        <h3>
                            Additional Model Results
                        </h3>


                        <div className="output-grid">

                            <div className="output-item">

                                <strong>
                                    Anomaly Score
                                </strong>

                                {result.anomaly_score ?? "-"}

                            </div>


                            <div className="output-item">

                                <strong>
                                    Anomaly Direction
                                </strong>

                                {result.anomaly_direction || "-"}

                            </div>


                            <div className="output-item">

                                <strong>
                                    NP3 Alert
                                </strong>

                                {result.np3_alert === null ||
                                result.np3_alert === undefined
                                    ? "-"
                                    : result.np3_alert
                                }

                            </div>


                            <div className="output-item">

                                <strong>
                                    NP3 Alert Count
                                </strong>

                                {result.np3_alert_count ?? "-"}

                            </div>


                            <div className="output-item">

                                <strong>
                                    NP3 Alert Types
                                </strong>

                                {result.np3_alert_types || "-"}

                            </div>


                            <div className="output-item">

                                <strong>
                                    Comparison
                                </strong>

                                {result.comparison || "-"}

                            </div>

                        </div>

                    </div>

                </div>

            )}


            {/* -------------------------------------------------
                AI EXPLANATION
            ------------------------------------------------- */}

            {result && (

                <div className="explanation-card">

                    <h2>
                        Explain with AI
                    </h2>

                    <p>
                        AI explanation will be added in the
                        later Claude phase. The prediction above
                        is the model output and should not be
                        interpreted as a guaranteed future event.
                    </p>


                    <button
                        className="ai-button"
                        onClick={() =>
                            alert(
                                "AI explanation will be implemented in the Claude phase."
                            )
                        }
                    >
                        Explain with AI
                    </button>

                </div>

            )}

        </div>

    );

}


export default PredictRisk;