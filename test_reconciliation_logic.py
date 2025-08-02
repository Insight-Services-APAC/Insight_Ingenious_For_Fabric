#!/usr/bin/env python3
"""
Test script to verify row count reconciliation logic for different write modes
"""
import sys
sys.path.insert(0, '/workspaces/ingen_fab')

from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import ProcessingMetrics
from ingen_fab.python_libs.common.flat_file_ingestion_utils import ProcessingMetricsUtils

def test_reconciliation_logic():
    """Test reconciliation logic for different write modes"""
    print("🧪 Testing Row Count Reconciliation Logic")
    print("=" * 60)
    
    # Test case 1: Overwrite mode - SUCCESS case
    print("\n📋 Test 1: Overwrite Mode - Success Case")
    metrics1 = ProcessingMetrics()
    metrics1.source_row_count = 158780000
    metrics1.target_row_count_before = 158480000
    metrics1.target_row_count_after = 158780000
    
    result1 = ProcessingMetricsUtils.calculate_performance_metrics(metrics1, "overwrite")
    print(f"  Source: {metrics1.source_row_count:,}")
    print(f"  Target Before: {metrics1.target_row_count_before:,}")
    print(f"  Target After: {metrics1.target_row_count_after:,}")
    print(f"  Expected: matched")
    print(f"  Actual: {result1.row_count_reconciliation_status}")
    print(f"  ✅ {'PASS' if result1.row_count_reconciliation_status == 'matched' else 'FAIL'}")
    
    # Test case 2: Overwrite mode - FAILURE case
    print("\n📋 Test 2: Overwrite Mode - Failure Case")
    metrics2 = ProcessingMetrics()
    metrics2.source_row_count = 100000
    metrics2.target_row_count_before = 50000
    metrics2.target_row_count_after = 90000  # Should be 100000 for success
    
    result2 = ProcessingMetricsUtils.calculate_performance_metrics(metrics2, "overwrite")
    print(f"  Source: {metrics2.source_row_count:,}")
    print(f"  Target Before: {metrics2.target_row_count_before:,}")
    print(f"  Target After: {metrics2.target_row_count_after:,}")
    print(f"  Expected: mismatched")
    print(f"  Actual: {result2.row_count_reconciliation_status}")
    print(f"  ✅ {'PASS' if result2.row_count_reconciliation_status == 'mismatched' else 'FAIL'}")
    
    # Test case 3: Append mode - SUCCESS case
    print("\n📋 Test 3: Append Mode - Success Case")
    metrics3 = ProcessingMetrics()
    metrics3.source_row_count = 50000
    metrics3.target_row_count_before = 100000
    metrics3.target_row_count_after = 150000  # 100000 + 50000
    
    result3 = ProcessingMetricsUtils.calculate_performance_metrics(metrics3, "append")
    print(f"  Source: {metrics3.source_row_count:,}")
    print(f"  Target Before: {metrics3.target_row_count_before:,}")
    print(f"  Target After: {metrics3.target_row_count_after:,}")
    print(f"  Expected: matched")
    print(f"  Actual: {result3.row_count_reconciliation_status}")
    print(f"  ✅ {'PASS' if result3.row_count_reconciliation_status == 'matched' else 'FAIL'}")
    
    # Test case 4: Append mode - FAILURE case
    print("\n📋 Test 4: Append Mode - Failure Case")
    metrics4 = ProcessingMetrics()
    metrics4.source_row_count = 50000
    metrics4.target_row_count_before = 100000
    metrics4.target_row_count_after = 140000  # Should be 150000 for success
    
    result4 = ProcessingMetricsUtils.calculate_performance_metrics(metrics4, "append")
    print(f"  Source: {metrics4.source_row_count:,}")
    print(f"  Target Before: {metrics4.target_row_count_before:,}")
    print(f"  Target After: {metrics4.target_row_count_after:,}")
    print(f"  Expected: mismatched")
    print(f"  Actual: {result4.row_count_reconciliation_status}")
    print(f"  ✅ {'PASS' if result4.row_count_reconciliation_status == 'mismatched' else 'FAIL'}")
    
    # Test case 5: Merge mode - SUCCESS case
    print("\n📋 Test 5: Merge Mode - Success Case")
    metrics5 = ProcessingMetrics()
    metrics5.source_row_count = 50000
    metrics5.target_row_count_before = 100000
    metrics5.target_row_count_after = 120000  # Could be anything >= 100000
    
    result5 = ProcessingMetricsUtils.calculate_performance_metrics(metrics5, "merge")
    print(f"  Source: {metrics5.source_row_count:,}")
    print(f"  Target Before: {metrics5.target_row_count_before:,}")
    print(f"  Target After: {metrics5.target_row_count_after:,}")
    print(f"  Expected: verified")
    print(f"  Actual: {result5.row_count_reconciliation_status}")
    print(f"  ✅ {'PASS' if result5.row_count_reconciliation_status == 'verified' else 'FAIL'}")
    
    print("\n" + "=" * 60)
    print("📊 Reconciliation Logic Summary")
    print("=" * 60)
    print("✅ OVERWRITE Mode: source_count == target_after")
    print("✅ APPEND Mode: source_count == (target_after - target_before)")
    print("✅ MERGE Mode: target_after >= target_before (verified)")
    print("\n🎉 All reconciliation logic tests completed!")

if __name__ == "__main__":
    test_reconciliation_logic()