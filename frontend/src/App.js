import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import Dashboard from './pages/Dashboard';
import StockDetail from './pages/StockDetail';
import TrendingStocks from './pages/TrendingStocks';

function App() {
    return (
        <Router>
            <div className="App">
                <Navbar />
                <Routes>
                    <Route path="/" element={<Dashboard />} />
                    <Route path="/stock/:symbol" element={<StockDetail />} />
                    <Route path="/trending" element={<TrendingStocks />} />
                </Routes>
            </div>
        </Router>
    );
}

export default App; 