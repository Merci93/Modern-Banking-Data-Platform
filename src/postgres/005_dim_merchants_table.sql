-- Merchant Dimension Table
CREATE TABLE IF NOT EXISTS dim_merchants (
  id SERIAL PRIMARY KEY,
  merchant_name VARCHAR(100) NOT NULL, -- e.g., 'Amazon', 'Walmart'
  merchant_category VARCHAR(50), -- e.g., 'retail', 'online'
  created_at TIMESTAMPTZ DEFAULT now()
);