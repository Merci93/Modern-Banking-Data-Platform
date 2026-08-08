-- Account Dimension Table
CREATE TABLE IF NOT EXISTS dim_accounts (
  id            SERIAL      PRIMARY KEY,
  customerId    INT         NOT NULL,
  currencyCode  CHAR(3)     NOT NULL,
  accountType   VARCHAR(20) NOT NULL, -- e.g., 'savings', 'checking', 'current'
  accountNumber VARCHAR(20) UNIQUE NOT NULL,
  status        VARCHAR(10) NOT NULL DEFAULT 'active',
  createdAt     TIMESTAMPTZ DEFAULT now(),

  CONSTRAINT fk_customer_accounts
    FOREIGN KEY (customerId)
    REFERENCES dim_customers(id)
    ON DELETE CASCADE,

  CONSTRAINT fk_customer_currency_coode
    FOREIGN KEY (currencyCode)
    REFERENCES dim_currency(currencyCode)
    ON DELETE RESTRICT
);