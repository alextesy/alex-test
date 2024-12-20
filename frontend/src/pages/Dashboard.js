import React, { useState, useEffect } from 'react';
import StockCard from '../components/StockCard';
import SentimentChart from '../components/SentimentChart';

function Dashboard() {
    const [topStocks, setTopStocks] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        // TODO: Fetch top stocks data from backend
        setLoading(false);
    }, []);

    return (
        <div className="container mx-auto px-4 py-8">
            <h1 className="text-3xl font-bold mb-6">Social Sentiment Dashboard</h1>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {loading ? (
                    <p>Loading...</p>
                ) : (
                    topStocks.map(stock => (
                        <StockCard key={stock.symbol} stock={stock} />
                    ))
                )}
            </div>

            <div className="mt-8">
                <SentimentChart />
            </div>
        </div>
    );
}

export default Dashboard; 