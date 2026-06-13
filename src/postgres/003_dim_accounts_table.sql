-- Account Dimension Table
CREATE TABLE IF NOT EXISTS dim_accounts (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES dim_customers(id) ON DELETE CASCADE,
  account_type VARCHAR(20) NOT NULL, -- e.g., 'savings', 'checking', 'current'
  account_number VARCHAR(20) UNIQUE NOT NULL,
  currency_code CHAR(3) NOT NULL REFERENCES dim_currency(currency_code),
  status VARCHAR(10) NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ DEFAULT now()
);