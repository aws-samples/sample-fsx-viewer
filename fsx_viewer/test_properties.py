"""Property-based tests for FSx Viewer.

These tests validate correctness properties from the design document.
Each property test runs minimum 100 iterations using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
from datetime import datetime, timezone
from io import StringIO

from rich.console import Console

from .model import (
    FileSystem,
    FileSystemType,
    Metrics,
    Store,
    Stats,
    PricingBreakdown,
)
from .ui import UI, Style, make_sorter


# =============================================================================
# Strategies (Generators)
# =============================================================================

def make_file_system(fs_id: str, name: str, fs_type: FileSystemType,
                     storage_capacity: int, used_capacity: int,
                     read_iops: float = 0.0, write_iops: float = 0.0,
                     read_throughput: float = 0.0, write_throughput: float = 0.0,
                     hourly_price: float = 0.0) -> FileSystem:
    """Helper to create FileSystem objects."""
    return FileSystem(
        id=fs_id,
        name=name,
        type=fs_type,
        storage_capacity=storage_capacity,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
        used_capacity=used_capacity,
        read_iops=read_iops,
        write_iops=write_iops,
        read_throughput=read_throughput,
        write_throughput=write_throughput,
        hourly_price=hourly_price,
    )


@st.composite
def file_system_strategy(draw):
    """Generate random FileSystem objects with valid data."""
    # Use simpler ID generation
    fs_id = f"fs-{draw(st.integers(min_value=10000000, max_value=99999999))}"
    
    name = draw(st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789-_"))
    fs_type = draw(st.sampled_from(list(FileSystemType)))
    storage_capacity = draw(st.integers(min_value=1, max_value=100000))
    used_capacity = draw(st.integers(min_value=0, max_value=storage_capacity))
    
    read_iops = draw(st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False))
    write_iops = draw(st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False))
    read_throughput = draw(st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False))
    write_throughput = draw(st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False))
    hourly_price = draw(st.floats(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False))
    
    return make_file_system(
        fs_id=fs_id,
        name=name,
        fs_type=fs_type,
        storage_capacity=storage_capacity,
        used_capacity=used_capacity,
        read_iops=read_iops,
        write_iops=write_iops,
        read_throughput=read_throughput,
        write_throughput=write_throughput,
        hourly_price=hourly_price,
    )


@st.composite
def file_system_list_strategy(draw, min_size=0, max_size=10):
    """Generate a list of unique FileSystem objects."""
    count = draw(st.integers(min_value=min_size, max_value=max_size))
    file_systems = []
    
    for i in range(count):
        fs_id = f"fs-{10000000 + i}"
        name = f"fs-name-{i}"
        fs_type = draw(st.sampled_from(list(FileSystemType)))
        storage_capacity = draw(st.integers(min_value=1, max_value=100000))
        used_capacity = draw(st.integers(min_value=0, max_value=storage_capacity))
        hourly_price = draw(st.floats(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False))
        
        fs = make_file_system(
            fs_id=fs_id,
            name=name,
            fs_type=fs_type,
            storage_capacity=storage_capacity,
            used_capacity=used_capacity,
            hourly_price=hourly_price,
        )
        file_systems.append(fs)
    
    return file_systems


# =============================================================================
# Property 3: Utilization Calculation
# =============================================================================

@settings(max_examples=100)
@given(
    storage_capacity=st.integers(min_value=1, max_value=1000000),
    used_capacity=st.integers(min_value=0, max_value=1000000),
)
def test_utilization_calculation(storage_capacity, used_capacity):
    """Property 3: utilization() = used_capacity / storage_capacity, clamped to [0, 1]."""
    # Ensure used <= storage for valid test
    used_capacity = min(used_capacity, storage_capacity)
    
    fs = FileSystem(
        id="fs-test",
        name="test",
        type=FileSystemType.ONTAP,
        storage_capacity=storage_capacity,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
        used_capacity=used_capacity,
    )
    
    expected = used_capacity / storage_capacity
    actual = fs.utilization()
    
    assert abs(actual - expected) < 1e-9, f"Expected {expected}, got {actual}"
    assert 0.0 <= actual <= 1.0, f"Utilization {actual} out of bounds [0, 1]"


@settings(max_examples=100)
@given(storage_capacity=st.integers(min_value=0, max_value=0))
def test_utilization_zero_capacity(storage_capacity):
    """Property 3b: utilization() returns 0 when storage_capacity is 0."""
    fs = FileSystem(
        id="fs-test",
        name="test",
        type=FileSystemType.ONTAP,
        storage_capacity=0,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
        used_capacity=0,
    )
    
    assert fs.utilization() == 0.0


# =============================================================================
# Property 5: IOPS Calculation
# =============================================================================

@settings(max_examples=100)
@given(
    read_iops=st.floats(min_value=0, max_value=1000000, allow_nan=False, allow_infinity=False),
    write_iops=st.floats(min_value=0, max_value=1000000, allow_nan=False, allow_infinity=False),
)
def test_iops_calculation(read_iops, write_iops):
    """Property 5: total_iops() = read_iops + write_iops."""
    fs = FileSystem(
        id="fs-test",
        name="test",
        type=FileSystemType.ONTAP,
        storage_capacity=1000,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
        read_iops=read_iops,
        write_iops=write_iops,
    )
    
    expected = read_iops + write_iops
    actual = fs.total_iops()
    
    assert abs(actual - expected) < 1e-9, f"Expected {expected}, got {actual}"


# =============================================================================
# Property 6: Monthly Price Calculation
# =============================================================================

@settings(max_examples=100)
@given(hourly_price=st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False))
def test_monthly_price_calculation(hourly_price):
    """Property 6: monthly_price() = hourly_price * 730."""
    fs = FileSystem(
        id="fs-test",
        name="test",
        type=FileSystemType.ONTAP,
        storage_capacity=1000,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
        hourly_price=hourly_price,
    )
    
    expected = hourly_price * 730
    actual = fs.monthly_price()
    
    assert abs(actual - expected) < 1e-6, f"Expected {expected}, got {actual}"


# =============================================================================
# Property 1: Store Consistency
# =============================================================================

@settings(max_examples=100)
@given(file_systems=file_system_list_strategy(min_size=0, max_size=20))
def test_store_consistency(file_systems):
    """Property 1: stats reflects current state after add/delete operations."""
    store = Store()
    
    # Add all file systems
    for fs in file_systems:
        store.add(fs)
    
    stats = store.stats()
    assert stats.total_file_systems == len(file_systems)
    
    # Delete half
    to_delete = file_systems[:len(file_systems) // 2]
    for fs in to_delete:
        store.delete(fs.id)
    
    stats = store.stats()
    expected_count = len(file_systems) - len(to_delete)
    assert stats.total_file_systems == expected_count


# =============================================================================
# Property 2: Stats Aggregation
# =============================================================================

@settings(max_examples=100)
@given(file_systems=file_system_list_strategy(min_size=1, max_size=20))
def test_stats_aggregation(file_systems):
    """Property 2: Totals equal sum of individual values."""
    store = Store()
    
    for fs in file_systems:
        store.add(fs)
    
    stats = store.stats()
    
    # Verify capacity totals
    expected_capacity = sum(fs.storage_capacity for fs in file_systems)
    assert stats.total_capacity == expected_capacity
    
    expected_used = sum(fs.used_capacity for fs in file_systems)
    assert stats.total_used_capacity == expected_used
    
    # Verify cost total
    expected_cost = sum(fs.hourly_price for fs in file_systems)
    assert abs(stats.total_hourly_cost - expected_cost) < 1e-6
    
    # Verify count by type
    for fs_type in FileSystemType:
        expected_type_count = sum(1 for fs in file_systems if fs.type == fs_type)
        actual_type_count = stats.count_by_type.get(fs_type, 0)
        assert actual_type_count == expected_type_count


# =============================================================================
# Property 4: Utilization Styling
# =============================================================================

@settings(max_examples=100)
@given(utilization=st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False))
def test_utilization_styling(utilization):
    """Property 4: Correct color for utilization ranges."""
    style = Style(good="green", ok="yellow", bad="red")
    
    color = style.color_for_utilization(utilization)
    
    if utilization < 0.8:
        assert color == "green", f"Expected green for {utilization}, got {color}"
    elif utilization < 0.9:
        assert color == "yellow", f"Expected yellow for {utilization}, got {color}"
    else:
        assert color == "red", f"Expected red for {utilization}, got {color}"


# =============================================================================
# Property 7: Type Filtering
# =============================================================================

@settings(max_examples=100)
@given(
    file_systems=file_system_list_strategy(min_size=1, max_size=20),
    filter_type=st.sampled_from(list(FileSystemType)),
)
def test_type_filtering(file_systems, filter_type):
    """Property 7: Only matching types visible after filtering."""
    store = Store()
    
    for fs in file_systems:
        store.add(fs)
    
    # Apply type filter by hiding non-matching
    def apply_filter(fs):
        if fs.type != filter_type:
            fs.hide()
        else:
            fs.show()
    
    store.for_each(apply_filter)
    
    stats = store.stats()
    
    # All visible file systems should match the filter type
    for fs in stats.file_systems:
        assert fs.type == filter_type, f"Expected {filter_type}, got {fs.type}"


# =============================================================================
# Property 8: Sorting Correctness
# =============================================================================

@settings(max_examples=100, suppress_health_check=[HealthCheck.large_base_example])
@given(file_systems=file_system_list_strategy(min_size=2, max_size=10))
def test_sorting_by_capacity_asc(file_systems):
    """Property 8a: Output ordered by capacity ascending."""
    sort_key, reverse = make_sorter("capacity=asc")
    sorted_fs = sorted(file_systems, key=sort_key, reverse=reverse)
    
    for i in range(len(sorted_fs) - 1):
        assert sorted_fs[i].storage_capacity <= sorted_fs[i + 1].storage_capacity


@settings(max_examples=100, suppress_health_check=[HealthCheck.large_base_example])
@given(file_systems=file_system_list_strategy(min_size=2, max_size=10))
def test_sorting_by_capacity_dsc(file_systems):
    """Property 8b: Output ordered by capacity descending."""
    sort_key, reverse = make_sorter("capacity=dsc")
    sorted_fs = sorted(file_systems, key=sort_key, reverse=reverse)
    
    for i in range(len(sorted_fs) - 1):
        assert sorted_fs[i].storage_capacity >= sorted_fs[i + 1].storage_capacity


@settings(max_examples=100, suppress_health_check=[HealthCheck.large_base_example])
@given(file_systems=file_system_list_strategy(min_size=2, max_size=10))
def test_sorting_by_name_asc(file_systems):
    """Property 8c: Output ordered by name ascending."""
    sort_key, reverse = make_sorter("name=asc")
    sorted_fs = sorted(file_systems, key=sort_key, reverse=reverse)
    
    for i in range(len(sorted_fs) - 1):
        assert sorted_fs[i].name.lower() <= sorted_fs[i + 1].name.lower()


# =============================================================================
# Property 9: Name Filtering
# =============================================================================

@settings(max_examples=100)
@given(
    file_systems=file_system_list_strategy(min_size=1, max_size=20),
    filter_pattern=st.text(min_size=1, max_size=5, alphabet="abcdefghijklmnopqrstuvwxyz"),
)
def test_name_filtering(file_systems, filter_pattern):
    """Property 9: Only matching names visible after filtering."""
    store = Store()
    
    for fs in file_systems:
        store.add(fs)
    
    # Apply name filter by hiding non-matching
    def apply_filter(fs):
        if filter_pattern.lower() not in fs.name.lower():
            fs.hide()
        else:
            fs.show()
    
    store.for_each(apply_filter)
    
    stats = store.stats()
    
    # All visible file systems should contain the filter pattern
    for fs in stats.file_systems:
        assert filter_pattern.lower() in fs.name.lower()


# =============================================================================
# Property 10: Config Precedence
# =============================================================================

def test_config_precedence_cli_over_env(monkeypatch):
    """Property 10a: CLI args override environment variables."""
    import os
    from .cli import parse_args
    
    # Set env var
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    
    # CLI should override
    config = parse_args(["--region", "eu-west-1"])
    assert config.region == "eu-west-1"


def test_config_precedence_env_over_default(monkeypatch):
    """Property 10b: Environment variables override defaults."""
    import os
    from .cli import parse_args
    
    # Set env var
    monkeypatch.setenv("AWS_REGION", "ap-southeast-1")
    
    # Should use env var
    config = parse_args([])
    assert config.region == "ap-southeast-1"


# =============================================================================
# Property 11: Pagination Bounds
# =============================================================================

@settings(max_examples=100)
@given(
    total_items=st.integers(min_value=0, max_value=100),
    page_size=st.integers(min_value=1, max_value=20),
)
def test_pagination_page_count(total_items, page_size):
    """Property 11a: Correct page count calculation."""
    import math
    
    store = Store()
    ui = UI(store=store, page_size=page_size)
    
    expected_pages = max(1, math.ceil(total_items / page_size))
    actual_pages = ui._get_page_count(total_items)
    
    assert actual_pages == expected_pages


@settings(max_examples=100)
@given(
    file_systems=file_system_list_strategy(min_size=1, max_size=50),
    page_size=st.integers(min_value=1, max_value=10),
)
def test_pagination_items_per_page(file_systems, page_size):
    """Property 11b: Correct items per page."""
    store = Store()
    for fs in file_systems:
        store.add(fs)
    
    ui = UI(store=store, page_size=page_size)
    
    # Get items for first page
    stats = store.stats()
    sorted_fs = ui._get_sorted_file_systems(stats)
    page_items = ui._get_page_items(sorted_fs)
    
    expected_count = min(page_size, len(file_systems))
    assert len(page_items) == expected_count


@settings(max_examples=100)
@given(
    file_systems=file_system_list_strategy(min_size=1, max_size=50),
    page_size=st.integers(min_value=1, max_value=10),
)
def test_pagination_all_items_covered(file_systems, page_size):
    """Property 11c: All items are covered across all pages."""
    store = Store()
    for fs in file_systems:
        store.add(fs)
    
    ui = UI(store=store, page_size=page_size)
    
    stats = store.stats()
    sorted_fs = ui._get_sorted_file_systems(stats)
    total_pages = ui._get_page_count(len(sorted_fs))
    
    all_items = []
    for page in range(total_pages):
        ui._current_page = page
        page_items = ui._get_page_items(sorted_fs)
        all_items.extend(page_items)
    
    # All items should be covered exactly once
    assert len(all_items) == len(file_systems)
    assert set(fs.id for fs in all_items) == set(fs.id for fs in file_systems)


# =============================================================================
# Pricing Breakdown Properties
# =============================================================================

@given(
    storage=st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False),
    throughput=st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False),
    iops=st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False),
    capacity_pool=st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False),
)
def test_pricing_breakdown_total_equals_sum(storage, throughput, iops, capacity_pool):
    """Property: PricingBreakdown.total always equals sum of components."""
    b = PricingBreakdown(storage=storage, throughput=throughput, iops=iops, capacity_pool=capacity_pool)
    assert abs(b.total - (storage + throughput + iops + capacity_pool)) < 1e-6


@given(
    storage=st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False),
    throughput=st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False),
)
def test_pricing_breakdown_set_price_backward_compat(storage, throughput):
    """Property: set_price(PricingBreakdown) derives hourly_price for backward compat."""
    b = PricingBreakdown(storage=storage, throughput=throughput)
    fs = make_file_system("fs-99999999", "test", FileSystemType.ONTAP, 100, 50)
    fs.set_price(b)
    
    assert fs.pricing_breakdown is b
    if b.total > 0:
        assert abs(fs.hourly_price - b.total / 730) < 1e-6
        assert abs(fs.monthly_price() - b.total) < 1e-6
        assert fs.has_price()
    else:
        assert fs.hourly_price == 0.0


@given(hourly_price=st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False))
def test_pricing_set_price_float_still_works(hourly_price):
    """Property: set_price(float) still works for legacy callers."""
    fs = make_file_system("fs-99999999", "test", FileSystemType.ONTAP, 100, 50)
    fs.set_price(hourly_price)
    assert fs.hourly_price == hourly_price
    assert fs.pricing_breakdown is None


@given(
    storage_capacity=st.integers(min_value=1, max_value=100000),
    throughput_capacity=st.integers(min_value=0, max_value=10000),
    provisioned_iops=st.integers(min_value=0, max_value=500000),
)
def test_pricing_ontap_components_non_negative(storage_capacity, throughput_capacity, provisioned_iops):
    """Property: All ONTAP pricing components are non-negative."""
    from .aws_client import StaticPricingProvider
    provider = StaticPricingProvider('us-east-1')
    fs = make_file_system("fs-99999999", "test", FileSystemType.ONTAP, storage_capacity, 0)
    fs.deployment_type = 'SINGLE_AZ_1'
    fs.throughput_capacity = throughput_capacity
    fs.provisioned_iops = provisioned_iops
    
    result = provider.file_system_price(fs)
    assert result is not None
    assert result.storage >= 0
    assert result.throughput >= 0
    assert result.iops >= 0
    assert result.capacity_pool >= 0
    assert result.total >= 0


@given(
    storage_capacity=st.integers(min_value=1, max_value=100000),
    provisioned_iops=st.integers(min_value=0, max_value=500000),
)
def test_pricing_iops_zero_within_baseline(storage_capacity, provisioned_iops):
    """Property: IOPS cost is 0 when provisioned IOPS <= baseline (3 per GB)."""
    from .aws_client import StaticPricingProvider
    provider = StaticPricingProvider('us-east-1')
    
    assume(provisioned_iops <= storage_capacity * 3)
    
    fs = make_file_system("fs-99999999", "test", FileSystemType.ONTAP, storage_capacity, 0)
    fs.deployment_type = 'SINGLE_AZ_1'
    fs.provisioned_iops = provisioned_iops
    
    result = provider.file_system_price(fs)
    assert result is not None
    assert result.iops == 0.0


def test_pricing_unknown_region_returns_none():
    """Property: Provider returns None for unknown regions."""
    from .aws_client import StaticPricingProvider
    provider = StaticPricingProvider('xx-nowhere-99')
    fs = make_file_system("fs-99999999", "test", FileSystemType.ONTAP, 1024, 0)
    assert provider.file_system_price(fs) is None


@given(capacity_pool_gb=st.floats(min_value=0.1, max_value=100000, allow_nan=False, allow_infinity=False))
def test_pricing_ontap_capacity_pool_from_cloudwatch(capacity_pool_gb):
    """Property: ONTAP capacity pool cost > 0 when usage > 0."""
    from .aws_client import StaticPricingProvider
    provider = StaticPricingProvider('us-east-1')
    fs = make_file_system("fs-99999999", "test", FileSystemType.ONTAP, 1024, 0)
    fs.deployment_type = 'SINGLE_AZ_1'
    fs.capacity_pool_used_gb = capacity_pool_gb
    
    result = provider.file_system_price(fs)
    assert result is not None
    assert result.capacity_pool > 0
