-- Currency Dimension Table
CREATE TABLE IF NOT EXISTS dim_currency (
  currency_code VARCHAR(3) PRIMARY KEY,
  currency_name VARCHAR(50),
  created_at TIMESTAMPTZ DEFAULT now()
);