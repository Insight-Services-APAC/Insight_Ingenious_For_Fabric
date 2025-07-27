-- Sample dataset configurations
-- Insert predefined dataset templates

INSERT INTO config_synthetic_data_datasets (
    dataset_id, dataset_name, dataset_type, schema_pattern, domain, 
    max_recommended_rows, description, config_json, is_active
) VALUES
-- Retail Transactional (OLTP)
('retail_oltp_small', 'Retail OLTP - Small', 'transactional', 'oltp', 'retail', 
 1000000, 'Small retail transactional system with customers, orders, products',
 '{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized"}', 1),

('retail_oltp_large', 'Retail OLTP - Large', 'transactional', 'oltp', 'retail', 
 1000000000, 'Large-scale retail transactional system',
 '{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized", "partitioning": "date"}', 1),

-- Retail Analytics (Star Schema)
('retail_star_small', 'Retail Star Schema - Small', 'analytical', 'star_schema', 'retail', 
 10000000, 'Small retail data warehouse with fact_sales and dimensions',
 '{"fact_tables": ["fact_sales"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store"]}', 1),

('retail_star_large', 'Retail Star Schema - Large', 'analytical', 'star_schema', 'retail', 
 5000000000, 'Large retail data warehouse with multiple fact tables',
 '{"fact_tables": ["fact_sales", "fact_inventory"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"]}', 1),

-- Financial Transactional
('finance_oltp_small', 'Financial OLTP - Small', 'transactional', 'oltp', 'finance', 
 500000, 'Small financial system with accounts, transactions, customers',
 '{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss"}', 1),

('finance_oltp_large', 'Financial OLTP - Large', 'transactional', 'oltp', 'finance', 
 2000000000, 'Large financial system with high transaction volume',
 '{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss", "high_frequency": true}', 1),

-- E-commerce Analytics
('ecommerce_star_small', 'E-commerce Star Schema - Small', 'analytical', 'star_schema', 'ecommerce', 
 5000000, 'E-commerce analytics with web events and sales',
 '{"fact_tables": ["fact_web_events", "fact_orders"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date"]}', 1),

('ecommerce_star_large', 'E-commerce Star Schema - Large', 'analytical', 'star_schema', 'ecommerce', 
 10000000000, 'Large e-commerce analytics with clickstream data',
 '{"fact_tables": ["fact_web_events", "fact_orders", "fact_page_views"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date", "dim_geography"]}', 1);