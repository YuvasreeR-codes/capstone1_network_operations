import { Link } from "react-router-dom";

function Home() {
    return (
        <div className="home-page">

            <style>{`

                * {
                    box-sizing: border-box;
                }

                body {
                    margin: 0;
                    font-family: Arial, sans-serif;
                }

                .home-page {
                    min-height: 100vh;
                    background: #f3f6fa;
                    color: #172033;
                }

                /* -----------------------------------------
                   HEADER
                ----------------------------------------- */

                .top-header {
                    background: #111827;
                    color: white;
                    padding: 26px 50px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    border-bottom: 4px solid #2563eb;
                }

                .brand {
                    display: flex;
                    align-items: center;
                    gap: 14px;
                }

                .brand-icon {
                    width: 44px;
                    height: 44px;
                    border-radius: 10px;
                    background: #2563eb;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 22px;
                }

                .brand h1 {
                    margin: 0;
                    font-size: 21px;
                    font-weight: 700;
                }

                .brand p {
                    margin: 4px 0 0;
                    color: #9ca3af;
                    font-size: 12px;
                }

                .system-status {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    font-size: 13px;
                    color: #d1d5db;
                }

                .status-dot {
                    width: 9px;
                    height: 9px;
                    border-radius: 50%;
                    background: #22c55e;
                }


                /* -----------------------------------------
                   MAIN
                ----------------------------------------- */

                .main-content {
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 55px 30px;
                }

                .welcome-section {
                    margin-bottom: 40px;
                }

                .welcome-section h2 {
                    margin: 0 0 10px;
                    font-size: 34px;
                    font-weight: 700;
                }

                .welcome-section p {
                    margin: 0;
                    color: #6b7280;
                    font-size: 16px;
                    max-width: 650px;
                    line-height: 1.6;
                }


                /* -----------------------------------------
                   SECTION LABEL
                ----------------------------------------- */

                .section-label {
                    font-size: 12px;
                    font-weight: 700;
                    color: #6b7280;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    margin-bottom: 16px;
                }


                /* -----------------------------------------
                   NAVIGATION GRID
                ----------------------------------------- */

                .navigation-grid {
                    display: grid;
                    grid-template-columns:
                        repeat(2, minmax(0, 1fr));
                    gap: 22px;
                }


                /* -----------------------------------------
                   NAV CARD
                ----------------------------------------- */

                .nav-card {
                    position: relative;
                    background: white;
                    border: 1px solid #e5e7eb;
                    border-radius: 14px;
                    padding: 27px;
                    text-decoration: none;
                    color: inherit;
                    min-height: 190px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;

                    box-shadow:
                        0 3px 10px rgba(0, 0, 0, 0.04);

                    transition:
                        transform 0.2s ease,
                        box-shadow 0.2s ease,
                        border-color 0.2s ease;
                }

                .nav-card:hover {
                    transform: translateY(-4px);

                    box-shadow:
                        0 10px 25px rgba(0, 0, 0, 0.09);

                    border-color: #93c5fd;
                }


                /* -----------------------------------------
                   CARD TOP
                ----------------------------------------- */

                .card-top {
                    display: flex;
                    align-items: flex-start;
                    justify-content: space-between;
                    gap: 15px;
                }

                .card-icon {
                    width: 48px;
                    height: 48px;
                    border-radius: 10px;

                    display: flex;
                    align-items: center;
                    justify-content: center;

                    font-size: 22px;
                }

                .summary-icon {
                    background: #dbeafe;
                }

                .grid-icon {
                    background: #ede9fe;
                }

                .hotspot-icon {
                    background: #ffedd5;
                }

                .risk-icon {
                    background: #fee2e2;
                }


                /* -----------------------------------------
                   ARROW
                ----------------------------------------- */

                .card-arrow {
                    font-size: 22px;
                    color: #9ca3af;
                    transition: transform 0.2s ease;
                }

                .nav-card:hover .card-arrow {
                    transform: translateX(4px);
                    color: #2563eb;
                }


                /* -----------------------------------------
                   CARD TEXT
                ----------------------------------------- */

                .nav-card h2 {
                    margin: 22px 0 8px;
                    font-size: 20px;
                    color: #111827;
                }

                .nav-card p {
                    margin: 0;
                    color: #6b7280;
                    font-size: 14px;
                    line-height: 1.6;
                }


                /* -----------------------------------------
                   CARD FOOTER
                ----------------------------------------- */

                .card-footer {
                    margin-top: 22px;
                    display: flex;
                    align-items: center;
                    gap: 7px;

                    color: #2563eb;
                    font-size: 12px;
                    font-weight: 700;
                }


                /* -----------------------------------------
                   INFO BAR
                ----------------------------------------- */

                .info-bar {
                    margin-top: 35px;
                    background: white;
                    border: 1px solid #e5e7eb;
                    border-radius: 12px;
                    padding: 18px 22px;

                    display: flex;
                    align-items: center;
                    gap: 12px;
                }

                .info-icon {
                    font-size: 18px;
                }

                .info-bar p {
                    margin: 0;
                    color: #6b7280;
                    font-size: 13px;
                    line-height: 1.5;
                }

                .info-bar strong {
                    color: #374151;
                }


                /* -----------------------------------------
                   FOOTER
                ----------------------------------------- */

                .footer {
                    text-align: center;
                    padding: 20px 30px 30px;
                    color: #9ca3af;
                    font-size: 12px;
                }


                /* -----------------------------------------
                   RESPONSIVE
                ----------------------------------------- */

                @media (max-width: 750px) {

                    .top-header {
                        padding: 20px 25px;
                    }

                    .system-status {
                        display: none;
                    }

                    .main-content {
                        padding: 35px 20px;
                    }

                    .welcome-section h2 {
                        font-size: 28px;
                    }

                    .navigation-grid {
                        grid-template-columns: 1fr;
                    }

                }

            `}</style>


            {/* ---------------------------------------------
                TOP HEADER
            --------------------------------------------- */}

            <header className="top-header">

                <div className="brand">

                    <div className="brand-icon">
                        📡
                    </div>

                    <div>

                        <h1>
                            Network Operations Center
                        </h1>

                        <p>
                            Telecom Network Monitoring & Analytics
                        </p>

                    </div>

                </div>


                <div className="system-status">

                    <span className="status-dot"></span>

                    System Operational

                </div>

            </header>


            {/* ---------------------------------------------
                MAIN CONTENT
            --------------------------------------------- */}

            <main className="main-content">

                <section className="welcome-section">

                    <h2>
                        Network Operations Dashboard
                    </h2>

                    <p>
                        Monitor network activity, investigate
                        individual grids, identify operational
                        hotspots and alerts, and evaluate
                        predictive network risk.
                    </p>

                </section>


                <div className="section-label">
                    Operational Areas
                </div>


                {/* -----------------------------------------
                    NAVIGATION CARDS
                ----------------------------------------- */}

                <div className="navigation-grid">


                    {/* NETWORK SUMMARY */}

                    <Link
                        to="/network-summary"
                        className="nav-card"
                    >

                        <div>

                            <div className="card-top">

                                <div className="card-icon summary-icon">
                                    📊
                                </div>

                                <div className="card-arrow">
                                    →
                                </div>

                            </div>


                            <h2>
                                Network Summary
                            </h2>

                            <p>
                                View network-wide activity,
                                active grids, peak hour and
                                top-performing grid metrics.
                            </p>

                        </div>


                        <div className="card-footer">
                            View Network KPIs →
                        </div>

                    </Link>


                    {/* GRID EXPLORER */}

                    <Link
                        to="/grid-explorer"
                        className="nav-card"
                    >

                        <div>

                            <div className="card-top">

                                <div className="card-icon grid-icon">
                                    🔍
                                </div>

                                <div className="card-arrow">
                                    →
                                </div>

                            </div>


                            <h2>
                                Grid Explorer
                            </h2>

                            <p>
                                Drill down into activity
                                metrics for a specific
                                network grid and time period.
                            </p>

                        </div>


                        <div className="card-footer">
                            Explore Grid Activity →
                        </div>

                    </Link>


                    {/* HOTSPOTS & ALERTS */}

                    <Link
                        to="/hotspots-alerts"
                        className="nav-card"
                    >

                        <div>

                            <div className="card-top">

                                <div className="card-icon hotspot-icon">
                                    🚨
                                </div>

                                <div className="card-arrow">
                                    →
                                </div>

                            </div>


                            <h2>
                                Hotspots & Alerts
                            </h2>

                            <p>
                                Identify high-activity grids,
                                network alerts and their
                                geographic locations across Milan.
                            </p>

                        </div>


                        <div className="card-footer">
                            View Operational Issues →
                        </div>

                    </Link>


                    {/* PREDICTIVE RISK */}

                    <Link
                        to="/predict-risk"
                        className="nav-card"
                    >

                        <div>

                            <div className="card-top">

                                <div className="card-icon risk-icon">
                                    ⚠️
                                </div>

                                <div className="card-arrow">
                                    →
                                </div>

                            </div>


                            <h2>
                                Predictive Risk
                            </h2>

                            <p>
                                Request a model-based risk
                                prediction for a selected
                                grid and timestamp.
                            </p>

                        </div>


                        <div className="card-footer">
                            Run Risk Prediction →
                        </div>

                    </Link>

                </div>


                {/* -----------------------------------------
                    INFORMATION BAR
                ----------------------------------------- */}

                <div className="info-bar">

                    <div className="info-icon">
                        ℹ️
                    </div>

                    <p>
                        <strong>Operational note:</strong>{" "}
                        Predictive risk is a model output and
                        should be interpreted as an indication
                        of potential future activity, not a
                        guaranteed event.
                    </p>

                </div>

            </main>


            {/* ---------------------------------------------
                FOOTER
            --------------------------------------------- */}

            <footer className="footer">

                Telecom Network Operations Dashboard

            </footer>

        </div>
    );
}

export default Home;