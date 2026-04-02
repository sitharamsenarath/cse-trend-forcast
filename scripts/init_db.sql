CREATE TABLE IF NOT EXISTS dim_stocks (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100) DEFAULT 'Unknown',
    isin VARCHAR(20),
    beta_value DOUBLE PRECISION,
    market_cap_total DOUBLE PRECISION,
    issued_quantity BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS fact_stock_prices (
    fact_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) REFERENCES dim_stocks(symbol),
    price DOUBLE PRECISION NOT NULL,
    open_price DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    prev_close DOUBLE PRECISION,
    volume BIGINT,
    turnover DOUBLE PRECISION,
    trade_count INTEGER,
    change_percentage DOUBLE PRECISION,
    extracted_at TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS fact_sector_indices (
    sector_id SERIAL PRIMARY KEY,
    index_name VARCHAR(100) NOT NULL,
    index_code VARCHAR(20),      
    index_value DOUBLE PRECISION,     
    sector_turnover DOUBLE PRECISION,
    sector_volume BIGINT,
    change_percentage DOUBLE PRECISION,
    extracted_at TIMESTAMP NOT NULL
);


INSERT INTO dim_stocks (symbol, name, sector) VALUES 
('JKH.N0000', 'John Keells Holdings PLC', 'Industrial'),
('COMB.N0000', 'Commercial Bank of Ceylon PLC', 'Banking'),
('AEL.N0000', 'Access Engineering PLC', 'Construction')
ON CONFLICT (symbol) DO NOTHING;