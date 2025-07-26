IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'config')
    exec('CREATE SCHEMA config;')

