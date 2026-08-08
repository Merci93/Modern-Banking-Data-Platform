-- Merchant Dimension Table
CREATE TABLE IF NOT EXISTS dim_merchants (
  id                SERIAL       PRIMARY KEY,
  merchantName      VARCHAR(100) UNIQUE NOT NULL, -- e.g., 'Amazon', 'Walmart'
  merchantCategory  VARCHAR(50), -- e.g., 'retail', 'online'
  createdAt        TIMESTAMPTZ  DEFAULT now()
);