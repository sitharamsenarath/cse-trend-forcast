CREATE TABLE IF NOT EXISTS dim_stocks (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100) DEFAULT 'Unknown',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS fact_stock_prices (
    fact_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) REFERENCES dim_stocks(symbol),
    price DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    volume BIGINT,
    change_percentage DOUBLE PRECISION,
    extracted_at TIMESTAMP NOT NULL
);


INSERT INTO dim_stocks (symbol, name, sector) VALUES 
('JKH.N0000', 'John Keells Holdings PLC', 'Industrial'),
('COMB.N0000', 'Commercial Bank of Ceylon PLC', 'Banking')
ON CONFLICT (symbol) DO NOTHING;