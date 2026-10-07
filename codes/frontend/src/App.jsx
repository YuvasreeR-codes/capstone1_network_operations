import { BrowserRouter, Routes, Route } from "react-router-dom";

import Home from "./pages/Home";
import NetworkSummary from "./pages/NetworkSummary";
import GridExplorer from "./pages/GridExplorer";
import HotspotsAlerts from "./pages/HotspotsAlerts";
import PredictRisk from "./pages/PredictRisk";
function App() {
    return (
        <BrowserRouter>

            <Routes>

                <Route path="/" element={<Home />} />

                <Route
                    path="/network-summary"
                    element={<NetworkSummary />}
                />
                <Route
    path="/grid-explorer"
    element={<GridExplorer />}
/>
<Route
    path="/hotspots-alerts"
    element={<HotspotsAlerts />}
/>
<Route
    path="/predict-risk"
    element={<PredictRisk />}
/>

               
            </Routes>

        </BrowserRouter>
    );
}

export default App;