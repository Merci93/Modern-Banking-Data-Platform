-- Customer Dimension Table
CREATE TABLE IF NOT EXISTS dim_customers (
  id         SERIAL PRIMARY KEY,
  firstName  VARCHAR(100) NOT NULL,
  middleName VARCHAR(100),
  lastName   VARCHAR(100) NOT NULL,
  phone      VARCHAR(20)  UNIQUE NOT NULL,
  email      VARCHAR(100) UNIQUE NOT NULL,
  country    VARCHAR(100) NOT NULL,
  address    VARCHAR(100) NOT NULL,
  createdAt  TIMESTAMPTZ DEFAULT now(),
  updatedAt  TIMESTAMPTZ
); 
