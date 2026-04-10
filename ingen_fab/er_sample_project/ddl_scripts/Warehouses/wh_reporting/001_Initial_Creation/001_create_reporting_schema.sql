IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Reporting')
BEGIN
    EXEC('CREATE SCHEMA [Reporting]')
END
