-- Currency Dimension Table
CREATE TABLE IF NOT EXISTS dim_currency (
  currencyCode VARCHAR(3) PRIMARY KEY,
  currencyName VARCHAR(50),
  createdAt    TIMESTAMPTZ DEFAULT now()
);