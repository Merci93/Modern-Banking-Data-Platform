-- Transaction Categories Dimension Table
CREATE TABLE IF NOT EXISTS dim_transaction_categories (
  id           SERIAL      PRIMARY KEY,
  categoryName VARCHAR(50) UNIQUE NOT NULL, -- e.g., 'groceries', 'utilities', 'entertainment'
  createdAt    TIMESTAMPTZ DEFAULT now()
);