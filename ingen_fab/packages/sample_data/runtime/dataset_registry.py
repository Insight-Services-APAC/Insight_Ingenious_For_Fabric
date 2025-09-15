from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DatasetInfo:
    """Information about a sample dataset."""
    
    name: str
    description: str
    url: str
    file_type: str
    source: str
    size_estimate: str
    columns: Optional[List[str]] = None
    read_options: Optional[Dict] = None


class DatasetRegistry:
    """Registry of available sample datasets from public repositories."""
    
    DATASETS = {
        # UCI Machine Learning Repository Datasets
        "iris": DatasetInfo(
            name="iris",
            description="Classic Iris flower dataset with 150 samples",
            url="https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data",
            file_type="csv",
            source="UCI ML Repository",
            size_estimate="5KB",
            columns=["sepal_length", "sepal_width", "petal_length", "petal_width", "species"],
            read_options={"header": None, "names": ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]}
        ),
        
        "wine": DatasetInfo(
            name="wine",
            description="Wine dataset with chemical analysis of wines",
            url="https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data",
            file_type="csv",
            source="UCI ML Repository", 
            size_estimate="11KB",
            read_options={"header": None}
        ),
        
        # Seaborn Sample Datasets
        "titanic": DatasetInfo(
            name="titanic",
            description="Titanic passenger survival dataset",
            url="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv",
            file_type="csv",
            source="Seaborn Data",
            size_estimate="60KB"
        ),
        
        "tips": DatasetInfo(
            name="tips",
            description="Restaurant tips dataset",
            url="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv",
            file_type="csv",
            source="Seaborn Data",
            size_estimate="7KB"
        ),
        
        "flights": DatasetInfo(
            name="flights",
            description="Monthly airline passenger numbers 1949-1960",
            url="https://raw.githubusercontent.com/mwaskom/seaborn-data/master/flights.csv",
            file_type="csv",
            source="Seaborn Data",
            size_estimate="3KB"
        ),
        
        # COVID-19 Data
        "covid_countries": DatasetInfo(
            name="covid_countries",
            description="COVID-19 data by country",
            url="https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv",
            file_type="csv",
            source="Our World in Data",
            size_estimate="500KB"
        ),
        
        # NYC Open Data
        "nyc_taxi_zones": DatasetInfo(
            name="nyc_taxi_zones",
            description="NYC Taxi Zone lookup table",
            url="https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD",
            file_type="csv",
            source="NYC Open Data",
            size_estimate="15KB"
        ),
        
        # Sample Sales Data
        "sample_sales": DatasetInfo(
            name="sample_sales",
            description="Sample superstore sales data",
            url="https://raw.githubusercontent.com/plotly/datasets/master/superstore.csv",
            file_type="csv",
            source="Plotly Datasets",
            size_estimate="2MB"
        ),
        
        # Financial Data
        "sp500_companies": DatasetInfo(
            name="sp500_companies",
            description="S&P 500 companies list",
            url="https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
            file_type="csv",
            source="DataHub",
            size_estimate="50KB"
        ),
        
        # Weather Data Sample
        "weather_sample": DatasetInfo(
            name="weather_sample",
            description="Sample weather data",
            url="https://raw.githubusercontent.com/vega/vega-datasets/main/data/weather.csv",
            file_type="csv",
            source="Vega Datasets",
            size_estimate="5KB"
        ),
        
        # Population Data
        "world_population": DatasetInfo(
            name="world_population", 
            description="World population by country",
            url="https://raw.githubusercontent.com/datasets/population/main/data/population.csv",
            file_type="csv",
            source="DataHub",
            size_estimate="100KB"
        ),
        
        # GitHub Events Sample (JSON)
        "github_events": DatasetInfo(
            name="github_events",
            description="Sample of recent GitHub events",
            url="https://api.github.com/events",
            file_type="json",
            source="GitHub API",
            size_estimate="100KB"
        )
    }
    
    @classmethod
    def get_dataset(cls, name: str) -> DatasetInfo:
        """Get dataset information by name.
        
        Args:
            name: Name of the dataset
            
        Returns:
            DatasetInfo for the requested dataset
            
        Raises:
            KeyError: If dataset not found
        """
        if name not in cls.DATASETS:
            raise KeyError(f"Dataset '{name}' not found. Available datasets: {', '.join(cls.list_datasets())}")
        return cls.DATASETS[name]
    
    @classmethod
    def list_datasets(cls) -> List[str]:
        """List all available dataset names.
        
        Returns:
            List of dataset names
        """
        return list(cls.DATASETS.keys())
    
    @classmethod
    def get_datasets_by_source(cls, source: str) -> List[str]:
        """Get all datasets from a specific source.
        
        Args:
            source: Source name to filter by
            
        Returns:
            List of dataset names from that source
        """
        return [
            name for name, info in cls.DATASETS.items()
            if info.source == source
        ]
    
    @classmethod
    def get_dataset_info_table(cls) -> List[Dict]:
        """Get dataset information as a list of dictionaries for tabular display.
        
        Returns:
            List of dataset information dictionaries
        """
        return [
            {
                "name": name,
                "description": info.description,
                "source": info.source,
                "file_type": info.file_type,
                "size_estimate": info.size_estimate
            }
            for name, info in cls.DATASETS.items()
        ]