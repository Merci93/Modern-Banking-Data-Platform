-- Fact Table for Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
  id                   BIGSERIAL      PRIMARY KEY,
  accountId            INT            NOT NULL,
  customerId           INT            NOT NULL,
  merchantId           INT,
  categoryId           INT,
  currencyCode         CHAR(3)        NOT NULL,
  transactionType      VARCHAR(20),
  amount               NUMERIC(12, 2) NOT NULL,
  transactionTimestamp TIMESTAMPTZ    NOT NULL,
  channel              VARCHAR(20),
  status               VARCHAR(10),
  referenceId          VARCHAR(50)    UNIQUE NOT NULL,
  createdAt            TIMESTAMPTZ    DEFAULT now(),

  CONSTRAINT fk_account_transactions
    FOREIGN KEY (accountId)
    REFERENCES dim_accounts(id)
    ON DELETE RESTRICT,

  CONSTRAINT fk_customer_transactions
    FOREIGN KEY (customerId)
    REFERENCES dim_customers(id)
    ON DELETE RESTRICT,

  CONSTRAINT fk_merchant_transactions
    FOREIGN KEY (merchantId)
    REFERENCES dim_merchants(id)
    ON DELETE RESTRICT,

  CONSTRAINT fk_catergory_transactions
    FOREIGN KEY (categoryId)
    REFERENCES dim_transaction_categories(id)
    ON DELETE RESTRICT,

  CONSTRAINT fk_currency_transactions
    FOREIGN KEY (currencyCode)
    REFERENCES dim_currency(currency_code)
    ON DELETE RESTRICT
);