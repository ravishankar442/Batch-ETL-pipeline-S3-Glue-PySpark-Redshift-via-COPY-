CREATE TABLE IF NOT EXISTS public.orders (
  order_id INTEGER PRIMARY KEY,
  product VARCHAR(256),
  quantity INTEGER,
  price DECIMAL(12,2),
  order_date DATE,
  total_price DECIMAL(12,2)
);
