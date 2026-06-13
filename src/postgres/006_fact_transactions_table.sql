-- Fact Table for Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
  id BIGSERIAL PRIMARY KEY,
  account_id INT NOT NULL REFERENCES dim_accounts(id) ON DELETE CASCADE,
  customer_id INT NOT NULL REFERENCES dim_customers(id) ON DELETE CASCADE,
  merchant_id INT REFERENCES dim_merchants(id),
  transaction_type VARCHAR(20), -- e.g., 'debit', 'credit'
  amount NUMERIC(12, 2) NOT NULL,
  currency_code CHAR(3) NOT NULL REFERENCES dim_currency(currency_code),
  category_id INT REFERENCES dim_transaction_categories(id),
  transaction_timestamp TIMESTAMPTZ NOT NULL,
  channel VARCHAR(20), -- e.g., 'online', 'in-store', 'mobile', 'ATM', 'POS'
  status VARCHAR(10), -- e.g., 'completed', 'pending', 'failed'
  reference_id VARCHAR(50) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);