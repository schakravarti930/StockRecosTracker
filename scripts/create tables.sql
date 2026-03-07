CREATE TABLE recommendations (
    id BIGINT PRIMARY KEY,                     -- '13683300'
    
    organization NVARCHAR(100),                -- ICICI Securities
    analyst_recommendation NVARCHAR(10),       -- BUY / SELL / HOLD
    
    stock_code NVARCHAR(20),                   -- scid (IW)
    stock_name NVARCHAR(100),                  -- Inox Wind
    stock_short_name NVARCHAR(100),
    exchange CHAR(1),                          -- N / B
    
    heading NVARCHAR(300),
    attachment_url NVARCHAR(500),
    stock_url NVARCHAR(200),
    
    recommend_date DATE,                       -- November 17, 2025
    entry_date DATE,                           -- 2025-11-18
    target_price_date DATE,                    -- 2025-11-17
    target_price_date_epoch INT,
    creation_timestamp DATETIME2,              -- parsed from 20251118161842
    
    recommended_price DECIMAL(10,2),           -- 146.1
    target_price DECIMAL(10,2),                -- 180
    cmp DECIMAL(10,2),                         -- 122.85
    
    price_change DECIMAL(10,2),
    percent_change DECIMAL(6,2),
    
    current_returns DECIMAL(6,2),
    potential_returns DECIMAL(6,2),
    
    target_price_flag NVARCHAR(10),             -- green / red
    
    created_at DATETIME2 DEFAULT SYSDATETIME()
);

CREATE TABLE recommendation_history (
    id BIGINT IDENTITY PRIMARY KEY,
    
    recommendation_id BIGINT,
    record_type NVARCHAR(10),              -- 'current' or 'previous'
    
    recommend_flag CHAR(1),                -- B / S / H
    target_price DECIMAL(10,2),
    target_price_date DATE,
    organization NVARCHAR(100),
    
    FOREIGN KEY (recommendation_id)
        REFERENCES recommendations(id)
);
