"""
Extract Generation Package

This module provides functionality to compile and generate extract report generation
notebooks and DDL scripts for automated file extracts from Fabric warehouse and lakehouse.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from ingen_fab.notebook_utils.base_notebook_compiler import BaseNotebookCompiler
from ingen_fab.utils.path_utils import PathUtils


class ExtractGenerationCompiler(BaseNotebookCompiler):
    """Compiler for extract generation templates"""
    
    def __init__(self, fabric_workspace_repo_dir: str = None):
        try:
            # Use path utilities for package resource discovery
            package_base = PathUtils.get_package_resource_path("packages/extract_generation")
            self.package_dir = package_base
            self.templates_dir = package_base / "templates"
            self.ddl_scripts_dir = package_base / "ddl_scripts"
        except FileNotFoundError:
            # Fallback for development environment
            self.package_dir = Path(__file__).parent
            self.templates_dir = self.package_dir / "templates"
            self.ddl_scripts_dir = self.package_dir / "ddl_scripts"
        
        # Set up template directories - include package templates and unified templates
        root_dir = Path.cwd()
        unified_templates_dir = root_dir / "ingen_fab" / "templates"
        template_search_paths = [self.templates_dir, unified_templates_dir]
        
        super().__init__(
            templates_dir=template_search_paths,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            package_name="extract_generation"
        )
        
        if self.console:
            self.console.print(f"[bold blue]Package Directory:[/bold blue] {self.package_dir}")
            self.console.print(f"[bold blue]Templates Directory:[/bold blue] {self.templates_dir}")
            self.console.print(f"[bold blue]DDL Scripts Directory:[/bold blue] {self.ddl_scripts_dir}")
    
    def compile_notebook(self, template_vars: Dict[str, Any] = None, target_datastore: str = "warehouse") -> Path:
        """Compile the extract generation notebook template"""
        
        # Select template based on target datastore
        if target_datastore == "warehouse":
            template_name = "extract_generation_warehouse_notebook.py.jinja"
        elif target_datastore == "lakehouse":
            template_name = "extract_generation_lakehouse_notebook.py.jinja"
        else:
            # Fallback to warehouse template for unknown datastores
            template_name = "extract_generation_warehouse_notebook.py.jinja"
        
        # Merge template variables with defaults
        default_vars = {
            "notebook_name": "extract_generation",
            "author": "Extract Generation Package",
            "description": "Automated extract report generation from warehouse/lakehouse tables",
            "target_datastore": target_datastore,
            "config_schema": "config",
            "log_schema": "log",
            "package_name": "extract_generation"
        }
        
        if template_vars:
            default_vars.update(template_vars)
        
        return self.compile_notebook_from_template(
            template_name=template_name,
            output_notebook_name="extract_generation_notebook",
            template_vars=default_vars,
            display_name="Extract Generation",
            description="Automated extract report generation from warehouse/lakehouse tables"
        )
    
    def get_ddl_scripts(self, generation_mode: str = "warehouse") -> List[Path]:
        """Get DDL scripts for the specified generation mode"""
        
        mode_dir = self.ddl_scripts_dir / generation_mode.lower()
        
        if not mode_dir.exists():
            raise ValueError(f"DDL scripts directory not found for mode: {generation_mode}")
        
        # For lakehouse, look for Python files; for warehouse, look for SQL files
        if generation_mode.lower() == "lakehouse":
            script_pattern = "*.py"
        else:
            script_pattern = "*.sql"
        
        # Order by name
        script_order = sorted([f.name for f in mode_dir.glob(script_pattern)])

        scripts = []
        for script_name in script_order:
            script_path = mode_dir / script_name
            if script_path.exists():
                scripts.append(script_path)
        
        return scripts
    
    def compile_ddl_scripts(self, template_vars: Dict[str, Any] = None, generation_mode: str = "warehouse") -> List[Path]:
        """Compile DDL scripts and place them in the project structure"""
        
        # Default template variables for DDL scripts
        default_vars = {
            "config_schema": "config",
            "log_schema": "log",
            "package_name": "extract_generation"
        }
        
        if template_vars:
            default_vars.update(template_vars)
        
        # Create DDL scripts directory structure based on generation mode
        if generation_mode.lower() == "lakehouse":
            # Following the pattern: ddl_scripts/Lakehouses/Config/001_Initial_Creation_ExtractGeneration/
            ddl_base_dir = Path(self.fabric_workspace_repo_dir) / "ddl_scripts" / "Lakehouses" / "Config" / "001_Initial_Creation_ExtractGeneration"
        else:
            # Following the pattern: ddl_scripts/Warehouses/Config_WH/001_Initial_Creation_ExtractGeneration/
            ddl_base_dir = Path(self.fabric_workspace_repo_dir) / "ddl_scripts" / "Warehouses" / "Config_WH" / "001_Initial_Creation_ExtractGeneration"
        
        ddl_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Get DDL scripts and copy/compile them
        scripts = self.get_ddl_scripts(generation_mode)
        compiled_scripts = []
        
        for script in scripts:
            # Generate target filename (remove any numbering prefix for cleaner names)
            target_filename = script.name
            
            target_path = ddl_base_dir / target_filename
            
            if script.suffix == ".jinja":
                # Compile Jinja template
                if generation_mode.lower() == "lakehouse":
                    output_ext = '.py'  # Lakehouse templates become Python files
                else:
                    output_ext = '.sql'  # Warehouse templates become SQL files
                
                self._compile_template(
                    template_name=script.name,
                    output_path=target_path.with_suffix(output_ext),
                    template_vars=default_vars,
                    search_paths=[script.parent]
                )
                compiled_scripts.append(target_path.with_suffix(output_ext))
            else:
                # Copy file directly (preserving original extension)
                target_path.write_text(script.read_text())
                compiled_scripts.append(target_path)
                if self.console:
                    self.console.print(f"[green]✓ DDL script copied:[/green] {target_path}")
        
        return compiled_scripts
    
    def get_sample_configurations(self) -> Dict[str, Any]:
        """Return sample extract configurations for testing"""
        
        return {
            "simple_table_extract": {
                "extract_name": "customers_daily",
                "extract_table_name": "customers",
                "extract_table_schema": "dbo",
                "is_full_load": True,
                "is_active": True,
                "file_properties_header": True,
                "extract_file_name": "customers",
                "extract_file_name_extension": "csv",
                "file_properties_column_delimiter": ",",
                "file_properties_row_delimiter": "\\n",
                "file_properties_encoding": "UTF-8",
                "output_format": "csv"
            },
            "view_extract_compressed": {
                "extract_name": "sales_summary_monthly",
                "extract_view_name": "v_sales_summary",
                "extract_view_schema": "reporting",
                "is_full_load": True,
                "is_active": True,
                "is_compressed": True,
                "compressed_type": "ZIP",
                "compressed_level": "NORMAL",
                "compressed_file_name": "sales_summary",
                "compressed_extension": ".zip",
                "output_format": "csv"
            },
            "stored_procedure_extract": {
                "extract_name": "financial_report",
                "extract_sp_name": "sp_generate_financial_report",
                "extract_sp_schema": "finance",
                "is_full_load": False,
                "is_active": True,
                "is_trigger_file": True,
                "trigger_file_extension": ".done",
                "output_format": "parquet"
            }
        }
    
    def compile_all(self, template_vars: Dict[str, Any] = None, include_samples: bool = False, target_datastore: str = "warehouse") -> Dict[str, Any]:
        """Compile all templates and DDL scripts"""
        
        results = {
            "notebook_file": None,
            "ddl_files": [],
            "sample_files": [],
            "success": True,
            "errors": []
        }
        
        try:
            # Compile notebook
            notebook_path = self.compile_notebook(template_vars, target_datastore)
            results["notebook_file"] = notebook_path
            
            # Compile DDL scripts (use target_datastore as generation_mode)
            ddl_scripts = self.compile_ddl_scripts(template_vars, target_datastore)
            results["ddl_files"] = ddl_scripts
            
            # Create sample source tables DDL if requested
            if include_samples:
                sample_ddl_path = self._create_sample_source_tables_ddl()
                if sample_ddl_path:
                    results["ddl_files"].append(sample_ddl_path)
            
            if self.console:
                self.console.print(f"[green]✓ Successfully compiled extract generation package[/green]")
                self.console.print(f"  Notebook: {notebook_path}")
                self.console.print(f"  DDL Scripts: {len(results['ddl_files'])} files")
            
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            if self.console:
                self.console.print(f"[red]✗ Error compiling package: {e}[/red]")
        
        return results
    
    def _create_sample_source_tables_ddl(self) -> Optional[Path]:
        """Create DDL for sample source tables used in extract configurations"""
        
        sample_ddl = """-- Sample source tables for Extract Generation testing

-- Create sample customers table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'customers')
BEGIN
    CREATE TABLE [dbo].[customers] (
        customer_id INT IDENTITY(1,1) PRIMARY KEY,
        customer_name NVARCHAR(100),
        email NVARCHAR(100),
        phone NVARCHAR(20),
        address NVARCHAR(200),
        city NVARCHAR(50),
        country NVARCHAR(50),
        created_date DATETIME2(7) DEFAULT GETUTCDATE()
    );
    
    -- Insert sample data
    INSERT INTO [dbo].[customers] (customer_name, email, phone, address, city, country) VALUES
    ('Acme Corp', 'contact@acme.com', '555-0100', '123 Main St', 'New York', 'USA'),
    ('Global Industries', 'info@global.com', '555-0200', '456 Oak Ave', 'London', 'UK'),
    ('Tech Solutions', 'sales@techsol.com', '555-0300', '789 Pine Rd', 'Tokyo', 'Japan'),
    ('Innovation Ltd', 'hello@innovate.com', '555-0400', '321 Elm St', 'Berlin', 'Germany'),
    ('Future Systems', 'support@future.com', '555-0500', '654 Maple Dr', 'Sydney', 'Australia');
END;

-- Create sample transactions table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'transactions')
BEGIN
    CREATE TABLE [dbo].[transactions] (
        transaction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT,
        product_id INT,
        amount DECIMAL(10,2),
        quantity INT,
        transaction_date DATETIME2(7),
        status VARCHAR(20)
    );
    
    -- Generate sample transactions (this would be larger in production)
    DECLARE @i INT = 1;
    WHILE @i <= 100
    BEGIN
        INSERT INTO [dbo].[transactions] (customer_id, product_id, amount, quantity, transaction_date, status)
        VALUES (
            ABS(CHECKSUM(NEWID())) % 5 + 1,
            ABS(CHECKSUM(NEWID())) % 10 + 1,
            ROUND(RAND() * 1000, 2),
            ABS(CHECKSUM(NEWID())) % 10 + 1,
            DATEADD(day, -ABS(CHECKSUM(NEWID())) % 365, GETUTCDATE()),
            CASE ABS(CHECKSUM(NEWID())) % 3 WHEN 0 THEN 'pending' WHEN 1 THEN 'completed' ELSE 'cancelled' END
        );
        SET @i = @i + 1;
    END;
END;

-- Create sample orders table
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'orders')
BEGIN
    CREATE TABLE [dbo].[orders] (
        order_id INT IDENTITY(1,1) PRIMARY KEY,
        customer_id INT,
        order_date DATETIME2(7),
        total_amount DECIMAL(10,2),
        status VARCHAR(20),
        modified_date DATETIME2(7) DEFAULT GETUTCDATE()
    );
END;

-- Create sales summary view
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[reporting].[v_sales_summary]'))
    DROP VIEW [reporting].[v_sales_summary];
GO

-- Create reporting schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting')
BEGIN
    EXEC('CREATE SCHEMA reporting')
END;
GO

CREATE VIEW [reporting].[v_sales_summary] AS
SELECT 
    YEAR(t.transaction_date) as year,
    MONTH(t.transaction_date) as month,
    c.country,
    COUNT(DISTINCT t.customer_id) as unique_customers,
    COUNT(t.transaction_id) as total_transactions,
    SUM(t.amount) as total_revenue,
    AVG(t.amount) as avg_transaction_value
FROM [dbo].[transactions] t
INNER JOIN [dbo].[customers] c ON t.customer_id = c.customer_id
WHERE t.status = 'completed'
GROUP BY YEAR(t.transaction_date), MONTH(t.transaction_date), c.country;
GO

-- Create finance schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'finance')
BEGIN
    EXEC('CREATE SCHEMA finance')
END;
GO

-- Create sample stored procedure
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[finance].[sp_generate_financial_report]'))
    DROP PROCEDURE [finance].[sp_generate_financial_report];
GO

CREATE PROCEDURE [finance].[sp_generate_financial_report]
AS
BEGIN
    SELECT 
        'Q' + CAST(DATEPART(QUARTER, t.transaction_date) AS VARCHAR(1)) + ' ' + 
        CAST(YEAR(t.transaction_date) AS VARCHAR(4)) as reporting_period,
        c.country as region,
        COUNT(DISTINCT t.customer_id) as active_customers,
        SUM(t.amount) as gross_revenue,
        SUM(CASE WHEN t.status = 'completed' THEN t.amount ELSE 0 END) as net_revenue,
        SUM(CASE WHEN t.status = 'cancelled' THEN t.amount ELSE 0 END) as cancelled_revenue,
        COUNT(t.transaction_id) as total_transactions,
        AVG(t.amount) as avg_transaction_size
    FROM [dbo].[transactions] t
    INNER JOIN [dbo].[customers] c ON t.customer_id = c.customer_id
    WHERE t.transaction_date >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY 
        DATEPART(QUARTER, t.transaction_date),
        YEAR(t.transaction_date),
        c.country
    ORDER BY 
        YEAR(t.transaction_date) DESC,
        DATEPART(QUARTER, t.transaction_date) DESC,
        c.country;
END;
GO

-- Create validation stored procedure
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'[dbo].[sp_validate_orders_extract]'))
    DROP PROCEDURE [dbo].[sp_validate_orders_extract];
GO

CREATE PROCEDURE [dbo].[sp_validate_orders_extract]
AS
BEGIN
    -- Simple validation: ensure we have recent orders
    DECLARE @recent_count INT;
    SELECT @recent_count = COUNT(*) 
    FROM [dbo].[orders] 
    WHERE order_date >= DATEADD(DAY, -7, GETDATE());
    
    IF @recent_count = 0
    BEGIN
        RAISERROR('No recent orders found for extraction', 16, 1);
    END;
END;
GO
"""
        
        # Write to DDL output directory
        output_path = self.output_dir / "ddl_scripts" / "999_sample_source_tables.sql"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sample_ddl)
        
        if self.console:
            self.console.print(f"[green]✓ Created sample source tables DDL:[/green] {output_path}")
        
        return output_path


def compile_extract_generation_package(fabric_workspace_repo_dir: str = None, 
                                     template_vars: Dict[str, Any] = None,
                                     include_samples: bool = False,
                                     target_datastore: str = "warehouse") -> Dict[str, Any]:
    """Main function to compile the extract generation package"""
    
    compiler = ExtractGenerationCompiler(fabric_workspace_repo_dir)
    return compiler.compile_all(template_vars, include_samples=include_samples, target_datastore=target_datastore)