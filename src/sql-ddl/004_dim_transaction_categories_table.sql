-- Transaction Categories Dimension Table
CREATE TABLE IF NOT EXISTS dim_transaction_categories (
  id SERIAL PRIMARY KEY,
  category_name VARCHAR(50) NOT NULL, -- e.g., 'groceries', 'utilities', 'entertainment'
  created_at TIMESTAMPTZ DEFAULT now()
);