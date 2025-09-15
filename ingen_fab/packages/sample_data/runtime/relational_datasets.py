from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class TableInfo:
    """Information about a table in a relational dataset."""
    
    name: str
    url: str
    description: str
    primary_key: Optional[str] = None
    foreign_keys: Dict[str, str] = field(default_factory=dict)  # column -> referenced_table
    columns: Optional[List[str]] = None
    read_options: Optional[Dict] = None
    

@dataclass 
class RelationalDatasetInfo:
    """Information about a relational dataset with multiple related tables."""
    
    name: str
    description: str
    source: str
    tables: Dict[str, TableInfo]
    load_order: List[str]  # Order in which tables should be loaded
    relationships: Optional[List[Tuple[str, str, str, str]]] = None  # (from_table, from_col, to_table, to_col)
    size_estimate: Optional[str] = None


class RelationalDatasetRegistry:
    """Registry of relational datasets with multiple related tables."""
    
    RELATIONAL_DATASETS = {
        "northwind": RelationalDatasetInfo(
            name="northwind",
            description="Classic Northwind database with customers, orders, and products",
            source="GitHub - graphql-compose",
            tables={
                "categories": TableInfo(
                    name="categories",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/categories.csv",
                    description="Product categories",
                    primary_key="categoryID"
                ),
                "customers": TableInfo(
                    name="customers",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/customers.csv",
                    description="Customer information",
                    primary_key="customerID"
                ),
                "employees": TableInfo(
                    name="employees",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/employees.csv",
                    description="Employee records",
                    primary_key="employeeID",
                    foreign_keys={"reportsTo": "employees"}
                ),
                "orders": TableInfo(
                    name="orders",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/orders.csv",
                    description="Customer orders",
                    primary_key="orderID",
                    foreign_keys={
                        "customerID": "customers",
                        "employeeID": "employees",
                        "shipVia": "shippers"
                    }
                ),
                "order_details": TableInfo(
                    name="order_details",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/order_details.csv",
                    description="Order line items",
                    foreign_keys={
                        "orderID": "orders",
                        "productID": "products"
                    }
                ),
                "products": TableInfo(
                    name="products",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/products.csv",
                    description="Product catalog",
                    primary_key="productID",
                    foreign_keys={
                        "supplierID": "suppliers",
                        "categoryID": "categories"
                    }
                ),
                "regions": TableInfo(
                    name="regions",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/regions.csv",
                    description="Geographic regions",
                    primary_key="regionID"
                ),
                "shippers": TableInfo(
                    name="shippers",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/shippers.csv",
                    description="Shipping companies",
                    primary_key="shipperID"
                ),
                "suppliers": TableInfo(
                    name="suppliers",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/suppliers.csv",
                    description="Product suppliers",
                    primary_key="supplierID"
                ),
                "territories": TableInfo(
                    name="territories",
                    url="https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/territories.csv",
                    description="Sales territories",
                    primary_key="territoryID",
                    foreign_keys={"regionID": "regions"}
                )
            },
            load_order=[
                "regions", "territories", "categories", "suppliers", 
                "shippers", "employees", "customers", "products", 
                "orders", "order_details"
            ],
            size_estimate="500KB"
        ),
        
        "chinook": RelationalDatasetInfo(
            name="chinook",
            description="Digital media store database with music tracks, albums, and sales",
            source="GitHub - lerocha/chinook-database",
            tables={
                "Album": TableInfo(
                    name="Album",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Album.csv",
                    description="Music albums",
                    primary_key="AlbumId",
                    foreign_keys={"ArtistId": "Artist"}
                ),
                "Artist": TableInfo(
                    name="Artist",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Artist.csv",
                    description="Music artists",
                    primary_key="ArtistId"
                ),
                "Customer": TableInfo(
                    name="Customer",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Customer.csv",
                    description="Store customers",
                    primary_key="CustomerId",
                    foreign_keys={"SupportRepId": "Employee"}
                ),
                "Employee": TableInfo(
                    name="Employee",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Employee.csv",
                    description="Store employees",
                    primary_key="EmployeeId",
                    foreign_keys={"ReportsTo": "Employee"}
                ),
                "Genre": TableInfo(
                    name="Genre",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Genre.csv",
                    description="Music genres",
                    primary_key="GenreId"
                ),
                "Invoice": TableInfo(
                    name="Invoice",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Invoice.csv",
                    description="Customer invoices",
                    primary_key="InvoiceId",
                    foreign_keys={"CustomerId": "Customer"}
                ),
                "InvoiceLine": TableInfo(
                    name="InvoiceLine",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/InvoiceLine.csv",
                    description="Invoice line items",
                    primary_key="InvoiceLineId",
                    foreign_keys={
                        "InvoiceId": "Invoice",
                        "TrackId": "Track"
                    }
                ),
                "MediaType": TableInfo(
                    name="MediaType",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/MediaType.csv",
                    description="Media formats",
                    primary_key="MediaTypeId"
                ),
                "Playlist": TableInfo(
                    name="Playlist",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Playlist.csv",
                    description="Music playlists",
                    primary_key="PlaylistId"
                ),
                "PlaylistTrack": TableInfo(
                    name="PlaylistTrack",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/PlaylistTrack.csv",
                    description="Playlist to track mapping",
                    foreign_keys={
                        "PlaylistId": "Playlist",
                        "TrackId": "Track"
                    }
                ),
                "Track": TableInfo(
                    name="Track",
                    url="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Track.csv",
                    description="Music tracks",
                    primary_key="TrackId",
                    foreign_keys={
                        "AlbumId": "Album",
                        "MediaTypeId": "MediaType",
                        "GenreId": "Genre"
                    }
                )
            },
            load_order=[
                "Artist", "Genre", "MediaType", "Album", "Track",
                "Playlist", "PlaylistTrack", "Employee", "Customer",
                "Invoice", "InvoiceLine"
            ],
            size_estimate="1MB"
        ),
        
        "adventureworks": RelationalDatasetInfo(
            name="adventureworks",
            description="Microsoft AdventureWorks sample database - bicycle manufacturer",
            source="GitHub - olafusimichael/AdventureWorksCSV",
            tables={
                "Product": TableInfo(
                    name="Product",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Production%20Product.csv",
                    description="Product catalog",
                    primary_key="ProductID",
                    foreign_keys={
                        "ProductSubcategoryID": "ProductSubcategory",
                        "ProductModelID": "ProductModel"
                    }
                ),
                "ProductCategory": TableInfo(
                    name="ProductCategory",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Production%20ProductCategory.csv",
                    description="Product categories",
                    primary_key="ProductCategoryID"
                ),
                "ProductSubcategory": TableInfo(
                    name="ProductSubcategory",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Production%20ProductSubcategory.csv",
                    description="Product subcategories",
                    primary_key="ProductSubcategoryID",
                    foreign_keys={"ProductCategoryID": "ProductCategory"}
                ),
                "ProductModel": TableInfo(
                    name="ProductModel",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Production%20ProductModel.csv",
                    description="Product models",
                    primary_key="ProductModelID"
                ),
                "ProductDescription": TableInfo(
                    name="ProductDescription",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Production%20ProductDescription.csv",
                    description="Product descriptions",
                    primary_key="ProductDescriptionID"
                ),
                "ProductModelProductDescriptionCulture": TableInfo(
                    name="ProductModelProductDescriptionCulture",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Production%20ProductModelProductDescriptionCulture.csv",
                    description="Product model to description mapping",
                    foreign_keys={
                        "ProductModelID": "ProductModel",
                        "ProductDescriptionID": "ProductDescription"
                    }
                ),
                "Customer": TableInfo(
                    name="Customer",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Sales%20Customer.csv",
                    description="Customer records",
                    primary_key="CustomerID"
                ),
                "Person": TableInfo(
                    name="Person",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Person%20Person.csv",
                    description="Person records",
                    primary_key="BusinessEntityID"
                ),
                "Address": TableInfo(
                    name="Address",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Person%20Address.csv",
                    description="Address information",
                    primary_key="AddressID"
                ),
                "SalesOrderHeader": TableInfo(
                    name="SalesOrderHeader",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Sales%20SalesOrderHeader.csv",
                    description="Sales order headers",
                    primary_key="SalesOrderID",
                    foreign_keys={
                        "CustomerID": "Customer",
                        "ShipToAddressID": "Address",
                        "BillToAddressID": "Address"
                    }
                ),
                "SalesOrderDetail": TableInfo(
                    name="SalesOrderDetail",
                    url="https://raw.githubusercontent.com/olafusimichael/AdventureWorksCSV/main/Sales%20SalesOrderDetail.csv",
                    description="Sales order line items",
                    primary_key="SalesOrderDetailID",
                    foreign_keys={
                        "SalesOrderID": "SalesOrderHeader",
                        "ProductID": "Product"
                    }
                )
            },
            load_order=[
                "ProductCategory", "ProductSubcategory", "ProductModel",
                "ProductDescription", "ProductModelProductDescriptionCulture", 
                "Product", "Person", "Address", "Customer",
                "SalesOrderHeader", "SalesOrderDetail"
            ],
            size_estimate="5MB"
        )
    }
    
    @classmethod
    def get_dataset(cls, name: str) -> RelationalDatasetInfo:
        """Get relational dataset information by name.
        
        Args:
            name: Name of the relational dataset
            
        Returns:
            RelationalDatasetInfo for the requested dataset
            
        Raises:
            KeyError: If dataset not found
        """
        if name not in cls.RELATIONAL_DATASETS:
            raise KeyError(f"Relational dataset '{name}' not found. Available: {', '.join(cls.list_datasets())}")
        return cls.RELATIONAL_DATASETS[name]
    
    @classmethod
    def list_datasets(cls) -> List[str]:
        """List all available relational dataset names.
        
        Returns:
            List of relational dataset names
        """
        return list(cls.RELATIONAL_DATASETS.keys())
    
    @classmethod
    def get_dataset_info_table(cls) -> List[Dict]:
        """Get relational dataset information as a list of dictionaries.
        
        Returns:
            List of dataset information dictionaries
        """
        return [
            {
                "name": name,
                "description": info.description,
                "source": info.source,
                "tables": len(info.tables),
                "size_estimate": info.size_estimate or "Unknown"
            }
            for name, info in cls.RELATIONAL_DATASETS.items()
        ]