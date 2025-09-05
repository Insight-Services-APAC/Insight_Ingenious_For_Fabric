#!/usr/bin/env python3
"""
Custom Command: analyze_table

Usage: analyze_table <table_name> [--schema <schema_name>] [--format <output_format>]

This command provides a streamlined interface for comprehensive table analysis,
executing all necessary SQL queries and providing structured output for documentation.
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fabric_sql_query_tool import FabricWarehouseQueryTool


class TableAnalyzer:
    """Comprehensive table analysis with multiple output formats."""
    
    def __init__(self):
        self.tool = FabricWarehouseQueryTool()
        self.analysis_results = {}
        
    def analyze(self, table_name: str, schema_name: str = "dbo") -> dict:
        """Execute comprehensive table analysis."""
        
        print(f"🚀 Starting comprehensive analysis of {schema_name}.{table_name}")
        print(f"📅 Analysis started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Initialize results structure
        self.analysis_results = {
            'table_name': table_name,
            'schema_name': schema_name,
            'analysis_timestamp': datetime.now().isoformat(),
            'sql_queries_executed': [],
            'analysis_steps': {}
        }
        
        # Execute analysis steps
        self._step1_basic_info(table_name, schema_name)
        self._step2_sample_data(table_name, schema_name)
        self._step3_data_quality(table_name, schema_name)
        self._step4_state_analysis(table_name, schema_name)
        self._step5_freshness_analysis(table_name, schema_name)
        self._step6_relationship_analysis(table_name, schema_name)
        
        # Generate summary
        self._generate_summary()
        
        return self.analysis_results
    
    def _execute_query_with_logging(self, query: str, description: str, return_dict: bool = False):
        """Execute query and log it for documentation."""
        print(f"\n📝 {description}")
        print("SQL Query:")
        print("```sql")
        print(query)
        print("```")
        
        # Store query for documentation
        self.analysis_results['sql_queries_executed'].append({
            'description': description,
            'query': query,
            'timestamp': datetime.now().isoformat()
        })
        
        try:
            result = self.tool.execute_query(query, return_dict=return_dict)
            print(f"✅ Query executed successfully")
            return result
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return None
    
    def _step1_basic_info(self, table_name: str, schema_name: str):
        """Step 1: Basic table information and column analysis."""
        print(f"\n🔍 STEP 1: BASIC TABLE INFORMATION")
        print("=" * 50)
        
        # Get column structure
        columns = self.tool.get_table_columns(table_name, schema_name)
        
        # Log the conceptual query (actual implementation uses tool method)
        conceptual_query = f"""SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION,
       CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
ORDER BY ORDINAL_POSITION"""
        
        self.analysis_results['sql_queries_executed'].append({
            'description': 'Get table column structure',
            'query': conceptual_query,
            'timestamp': datetime.now().isoformat()
        })
        
        print(f"📊 Found {len(columns)} columns")
        
        # Categorize columns
        categories = self._categorize_columns(columns)
        
        self.analysis_results['analysis_steps']['basic_info'] = {
            'total_columns': len(columns),
            'column_categories': categories,
            'columns': columns
        }
        
        print(f"Column categorization:")
        for category, count in categories.items():
            print(f"  📋 {category.replace('_', ' ').title()}: {count}")
        
        # Get row count
        row_count_query = f"SELECT COUNT(*) as [row_count] FROM [{schema_name}].[{table_name}]"
        row_result = self._execute_query_with_logging(
            row_count_query, 
            "Get total row count"
        )
        
        if row_result:
            row_count = row_result[0][0]
            self.analysis_results['analysis_steps']['basic_info']['row_count'] = row_count
            print(f"📈 Total records: {row_count:,}")
        
    def _step2_sample_data(self, table_name: str, schema_name: str):
        """Step 2: Sample data analysis."""
        print(f"\n🔍 STEP 2: SAMPLE DATA ANALYSIS")
        print("=" * 50)
        
        # Get available columns
        columns = self.analysis_results['analysis_steps']['basic_info']['columns']
        available_fields = [col['column_name'] for col in columns]
        
        # Select key fields for sampling
        key_fields = ['Id', 'SinkCreatedOn', 'SinkModifiedOn', 'statecode', 'statuscode']
        for field in ['name', 'firstname', 'lastname', 'accountnumber', 'telephone1', 'emailaddress1']:
            if field in available_fields:
                key_fields.append(field)
        
        # Limit to available fields and reasonable number
        sample_fields = [f for f in key_fields if f in available_fields][:8]
        fields_str = ', '.join(sample_fields)
        
        sample_query = f"""SELECT TOP 10 {fields_str}
FROM [{schema_name}].[{table_name}]
ORDER BY SinkCreatedOn DESC"""
        
        sample_data = self._execute_query_with_logging(
            sample_query,
            "Get representative sample data",
            return_dict=True
        )
        
        if sample_data:
            self.analysis_results['analysis_steps']['sample_data'] = {
                'sample_records': sample_data,
                'fields_analyzed': sample_fields
            }
            
            print(f"📋 Sample records ({len(sample_data)} retrieved):")
            for i, record in enumerate(sample_data[:3], 1):
                print(f"  Record {i}:")
                for key, value in record.items():
                    display_value = str(value)[:50] + '...' if value and len(str(value)) > 50 else str(value)
                    print(f"    {key}: {display_value}")
                print()
    
    def _step3_data_quality(self, table_name: str, schema_name: str):
        """Step 3: Data quality analysis."""
        print(f"\n🔍 STEP 3: DATA QUALITY ANALYSIS")
        print("=" * 50)
        
        # Get available columns
        columns = self.analysis_results['analysis_steps']['basic_info']['columns']
        available_fields = [col['column_name'] for col in columns]
        
        # Select fields for quality analysis
        quality_fields = []
        for field in ['name', 'firstname', 'lastname', 'accountnumber', 'telephone1', 'emailaddress1', 'websiteurl']:
            if field in available_fields:
                quality_fields.append(field)
        
        if quality_fields:
            count_selects = [f"COUNT({field}) as {field}_non_null" for field in quality_fields]
            
            quality_query = f"""SELECT 
    COUNT(*) as total_rows,
    {', '.join(count_selects)}
FROM [{schema_name}].[{table_name}]"""
            
            quality_data = self._execute_query_with_logging(
                quality_query,
                "Analyze data completeness for key fields",
                return_dict=True
            )
            
            if quality_data:
                quality_result = quality_data[0]
                total = quality_result['total_rows']
                
                completeness_analysis = {}
                print(f"📊 Data completeness analysis:")
                print(f"  Total records: {total:,}")
                
                for field in quality_fields:
                    non_null = quality_result[f'{field}_non_null']
                    completeness = (non_null / total * 100) if total > 0 else 0
                    
                    if completeness >= 95:
                        indicator = "✅"
                        status = "Excellent"
                    elif completeness >= 50:
                        indicator = "⚠️"
                        status = "Moderate"
                    else:
                        indicator = "❌"
                        status = "Poor"
                    
                    completeness_analysis[field] = {
                        'non_null_count': non_null,
                        'total_count': total,
                        'completeness_percentage': round(completeness, 1),
                        'status': status
                    }
                    
                    print(f"  {indicator} {field}: {non_null:,}/{total:,} ({completeness:.1f}% - {status})")
                
                self.analysis_results['analysis_steps']['data_quality'] = completeness_analysis
    
    def _step4_state_analysis(self, table_name: str, schema_name: str):
        """Step 4: State/status distribution analysis (D365 tables)."""
        columns = self.analysis_results['analysis_steps']['basic_info']['columns']
        available_fields = [col['column_name'] for col in columns]
        
        if 'statecode' in available_fields and 'statuscode' in available_fields:
            print(f"\n🔍 STEP 4: STATE/STATUS DISTRIBUTION")
            print("=" * 50)
            
            state_query = f"""SELECT statecode, statuscode, COUNT(*) as record_count,
       CAST((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [{schema_name}].[{table_name}])) as DECIMAL(5,2)) as percentage
FROM [{schema_name}].[{table_name}]
GROUP BY statecode, statuscode
ORDER BY record_count DESC"""
            
            state_data = self._execute_query_with_logging(
                state_query,
                "Analyze state/status distribution",
                return_dict=True
            )
            
            if state_data:
                self.analysis_results['analysis_steps']['state_distribution'] = state_data
                
                print(f"📊 State/Status distribution:")
                for row in state_data:
                    state_desc = 'Active' if row['statecode'] == 0 else 'Inactive'
                    print(f"  📈 State {row['statecode']} ({state_desc}), Status {row['statuscode']}: {row['record_count']:,} records ({row['percentage']}%)")
    
    def _step5_freshness_analysis(self, table_name: str, schema_name: str):
        """Step 5: Data freshness analysis."""
        columns = self.analysis_results['analysis_steps']['basic_info']['columns']
        available_fields = [col['column_name'] for col in columns]
        
        if 'SinkCreatedOn' in available_fields:
            print(f"\n🔍 STEP 5: DATA FRESHNESS ANALYSIS")
            print("=" * 50)
            
            freshness_query = f"""SELECT 
    MIN(SinkCreatedOn) as earliest_record,
    MAX(SinkCreatedOn) as latest_record,
    COUNT(DISTINCT CAST(SinkCreatedOn as DATE)) as distinct_days,
    COUNT(DISTINCT CAST(SinkModifiedOn as DATE)) as distinct_modified_days
FROM [{schema_name}].[{table_name}]
WHERE SinkCreatedOn IS NOT NULL"""
            
            freshness_data = self._execute_query_with_logging(
                freshness_query,
                "Analyze data loading patterns and freshness",
                return_dict=True
            )
            
            if freshness_data:
                freshness_result = freshness_data[0]
                self.analysis_results['analysis_steps']['data_freshness'] = freshness_result
                
                print(f"📅 Data freshness analysis:")
                print(f"  📅 Earliest record: {freshness_result['earliest_record']}")
                print(f"  📅 Latest record: {freshness_result['latest_record']}")
                print(f"  📊 Loading days span: {freshness_result['distinct_days']} distinct days")
                print(f"  📊 Modification days: {freshness_result['distinct_modified_days']} distinct days")
    
    def _step6_relationship_analysis(self, table_name: str, schema_name: str):
        """Step 6: Relationship field analysis."""
        print(f"\n🔍 STEP 6: RELATIONSHIP ANALYSIS")
        print("=" * 50)
        
        columns = self.analysis_results['analysis_steps']['basic_info']['columns']
        
        # Find relationship fields
        relationship_fields = []
        for col in columns:
            name = col['column_name'].lower()
            if (name.endswith('id') and name != 'id') or 'customerid' in name or 'parentid' in name:
                relationship_fields.append(col['column_name'])
        
        self.analysis_results['analysis_steps']['relationships'] = {
            'relationship_fields': relationship_fields,
            'relationship_count': len(relationship_fields)
        }
        
        print(f"🔗 Found {len(relationship_fields)} potential relationship fields:")
        for field in relationship_fields[:10]:  # Show first 10
            print(f"  - {field}")
        if len(relationship_fields) > 10:
            print(f"  ... and {len(relationship_fields) - 10} more")
    
    def _categorize_columns(self, columns):
        """Categorize columns into business, system, relationship, and custom."""
        categories = {
            'core_business': 0,
            'system_fields': 0,
            'relationship_fields': 0,
            'custom_extended': 0
        }
        
        for col in columns:
            name = col['column_name'].lower()
            
            if col['column_name'] in ['Id', 'name', 'firstname', 'lastname', 'accountnumber', 'telephone1', 'emailaddress1']:
                categories['core_business'] += 1
            elif any(pattern in name for pattern in ['statecode', 'statuscode', 'sink', 'createdon', 'modifiedon', 'ownerid']):
                categories['system_fields'] += 1
            elif name.endswith('id') and name != 'id':
                categories['relationship_fields'] += 1
            else:
                categories['custom_extended'] += 1
        
        return categories
    
    def _generate_summary(self):
        """Generate analysis summary."""
        print(f"\n" + "=" * 80)
        print(f"📋 COMPREHENSIVE ANALYSIS SUMMARY")
        print(f"=" * 80)
        
        basic_info = self.analysis_results['analysis_steps'].get('basic_info', {})
        
        print(f"🎯 Table: {self.analysis_results['schema_name']}.{self.analysis_results['table_name']}")
        print(f"📊 Columns: {basic_info.get('total_columns', 'Unknown')}")
        
        if 'row_count' in basic_info:
            print(f"📈 Records: {basic_info['row_count']:,}")
        
        print(f"🔧 Column Categories: {basic_info.get('column_categories', {})}")
        print(f"🔍 SQL Queries Executed: {len(self.analysis_results['sql_queries_executed'])}")
        
        # Data quality summary
        if 'data_quality' in self.analysis_results['analysis_steps']:
            quality_data = self.analysis_results['analysis_steps']['data_quality']
            print(f"📊 Data Quality Issues Identified:")
            for field, metrics in quality_data.items():
                if metrics['completeness_percentage'] < 95:
                    print(f"  ⚠️  {field}: {metrics['completeness_percentage']}% complete")
        
        print(f"\n💾 Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def save_results(self, output_format='json', output_file=None):
        """Save analysis results to file."""
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            table_name = self.analysis_results['table_name']
            schema_name = self.analysis_results['schema_name']
            output_file = f"table_analysis_{schema_name}_{table_name}_{timestamp}.{output_format}"
        
        if output_format == 'json':
            with open(output_file, 'w') as f:
                json.dump(self.analysis_results, f, indent=2, default=str)
        
        print(f"💾 Results saved to: {output_file}")
        return output_file


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Comprehensive table analysis with SQL execution logging",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  analyze_table account
  analyze_table contact --schema dbo
  analyze_table custtable --format json
        """
    )
    
    parser.add_argument("table_name", help="Name of the table to analyze")
    parser.add_argument("--schema", "-s", default="dbo", help="Schema name (default: dbo)")
    parser.add_argument("--format", "-f", choices=['json'], default='json', help="Output format (default: json)")
    parser.add_argument("--output", "-o", help="Output file name (auto-generated if not specified)")
    parser.add_argument("--save", action="store_true", help="Save results to file")
    
    args = parser.parse_args()
    
    # Execute analysis
    analyzer = TableAnalyzer()
    results = analyzer.analyze(args.table_name, args.schema)
    
    # Save results if requested
    if args.save or args.output:
        analyzer.save_results(args.format, args.output)
    
    return results


if __name__ == "__main__":
    main()