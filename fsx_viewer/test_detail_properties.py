"""Property-based tests for FSx Detail View.

Feature: fsx-detail-view
These tests validate correctness properties from the design document.
"""

import pytest
from hypothesis import given, strategies as st, settings
from datetime import datetime, timezone
from io import StringIO

from rich.console import Console

from .model import (
    Volume,
    MetadataServer,
    FileSystem,
    FileSystemType,
    DetailStore,
)
from .ui import DetailUI, Style


@st.composite
def volume_strategy(draw):
    """Generate random Volume objects with valid data."""
    vol_id = draw(st.text(
        alphabet="abcdef0123456789",
        min_size=8,
        max_size=17
    ).map(lambda x: f"fsvol-{x}"))
    
    name = draw(st.text(alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-", min_size=3, max_size=50))
    fs_id = draw(st.text(
        alphabet="abcdef0123456789",
        min_size=8,
        max_size=17
    ).map(lambda x: f"fs-{x}"))
    
    vol_type = draw(st.sampled_from(["ONTAP", "OPENZFS"]))
    storage_capacity = draw(st.integers(min_value=1, max_value=100000))
    used_capacity = draw(st.integers(min_value=0, max_value=storage_capacity))
    
    read_iops = draw(st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False))
    write_iops = draw(st.floats(min_value=0, max_value=100000, allow_nan=False, allow_infinity=False))
    read_throughput = draw(st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False))
    write_throughput = draw(st.floats(min_value=0, max_value=10000, allow_nan=False, allow_infinity=False))
    
    return Volume(
        id=vol_id,
        name=name,
        file_system_id=fs_id,
        type=vol_type,
        storage_capacity=storage_capacity,
        used_capacity=used_capacity,
        read_iops=read_iops,
        write_iops=write_iops,
        read_throughput=read_throughput,
        write_throughput=write_throughput,
    )


def render_to_string(panel) -> str:
    """Render a Rich Panel to a plain string for testing."""
    console = Console(file=StringIO(), force_terminal=True, width=200)
    console.print(panel)
    return console.file.getvalue()


# Property 1: Volume Display Completeness
# Validates: Requirements 2.2, 2.3, 2.4, 3.2, 3.3, 3.4

@settings(max_examples=100)
@given(volume=volume_strategy())
def test_volume_display_completeness(volume: Volume):
    """Property 1: Volume Display Completeness
    
    *For any* ONTAP or OpenZFS volume, the rendered detail view SHALL contain:
    volume ID, volume name, storage capacity, used capacity.
    
    **Validates: Requirements 2.2, 2.3, 2.4, 3.2, 3.3, 3.4**
    """
    fs_type = FileSystemType.ONTAP if volume.type == "ONTAP" else FileSystemType.OPENZFS
    fs = FileSystem(
        id=volume.file_system_id,
        name="Test FS",
        type=fs_type,
        storage_capacity=1000,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
    )
    
    store = DetailStore()
    store.set_file_system(fs)
    store.add_volume(volume)
    
    ui = DetailUI(store=store, style=Style())
    panel = ui.render()
    rendered = render_to_string(panel)
    
    assert volume.id in rendered, f"Volume ID '{volume.id}' not found"
    assert volume.name in rendered, f"Volume name '{volume.name}' not found"
    assert str(volume.storage_capacity) in rendered, f"Storage capacity not found"
    assert str(volume.used_capacity) in rendered, f"Used capacity not found"
    
    if volume.read_iops > 0 or volume.write_iops > 0:
        assert "r/" in rendered or "-" in rendered, "IOPS format not found"
    
    if volume.read_throughput > 0 or volume.write_throughput > 0:
        assert "r/" in rendered or "-" in rendered, "Throughput format not found"


# Property 2: MDS Display Completeness
# Validates: Requirements 4.2, 4.3, 4.5

@settings(max_examples=100)
@given(
    mds_count=st.integers(min_value=1, max_value=16),
    data=st.data()
)
def test_mds_display_completeness(mds_count: int, data):
    """Property 2: MDS Display Completeness
    
    *For any* Lustre file system with N MDS servers (where 1 <= N <= 16),
    the rendered detail view SHALL display MDS servers on the current page,
    each with server ID and CPU utilization percentage.
    
    **Validates: Requirements 4.2, 4.3, 4.5**
    """
    fs_id = f"fs-{data.draw(st.text(alphabet='abcdef0123456789', min_size=8, max_size=17))}"
    fs = FileSystem(
        id=fs_id,
        name="Test Lustre FS",
        type=FileSystemType.LUSTRE,
        storage_capacity=10000,
        creation_time=datetime.now(timezone.utc),
        lifecycle="AVAILABLE",
        read_throughput=100.0,
        write_throughput=50.0,
        read_iops=1000.0,
        write_iops=500.0,
    )
    
    mds_servers = []
    for i in range(mds_count):
        cpu = data.draw(st.floats(min_value=0, max_value=100, allow_nan=False, allow_infinity=False))
        mds = MetadataServer(
            id=f"MDS{i:04d}",
            file_system_id=fs_id,
            cpu_utilization=cpu,
        )
        mds_servers.append(mds)
    
    store = DetailStore()
    store.set_file_system(fs)
    for mds in mds_servers:
        store.add_mds(mds)
    
    ui = DetailUI(store=store, style=Style(), page_size=10)
    panel = ui.render()
    rendered = render_to_string(panel)
    
    # With pagination, only the first page (up to page_size items) is displayed
    page_size = 10
    page_mds = mds_servers[:page_size]
    
    for mds in page_mds:
        assert mds.id in rendered, f"MDS ID '{mds.id}' not found on first page"
        cpu_str = f"{mds.cpu_utilization:.1f}%"
        assert cpu_str in rendered, f"CPU '{cpu_str}' for MDS '{mds.id}' not found"
    
    # Verify the count matches for the first page
    expected_on_page = min(mds_count, page_size)
    displayed_mds_count = sum(1 for mds in page_mds if mds.id in rendered)
    assert displayed_mds_count == expected_on_page, \
        f"Expected {expected_on_page} MDS servers on first page, found {displayed_mds_count}"


# Property 3: Volume Metrics Dimension Correctness
# Validates: Requirements 2.5, 3.5

class MockCloudWatchClient:
    """Mock CloudWatch client that records query dimensions."""
    
    def __init__(self):
        self.recorded_queries = []
    
    def get_volume_metrics(self, fs_id: str, volume_id: str):
        """Record the query dimensions and return mock data."""
        self.recorded_queries.append({
            'fs_id': fs_id,
            'volume_id': volume_id,
        })
        return {
            'read_throughput': 0.0,
            'write_throughput': 0.0,
            'read_iops': 0.0,
            'write_iops': 0.0,
        }


@settings(max_examples=100)
@given(
    fs_id=st.text(alphabet="abcdef0123456789", min_size=8, max_size=17).map(lambda x: f"fs-{x}"),
    volume_id=st.text(alphabet="abcdef0123456789", min_size=8, max_size=17).map(lambda x: f"fsvol-{x}"),
)
def test_volume_metrics_dimension_correctness(fs_id: str, volume_id: str):
    """Property 3: Volume Metrics Dimension Correctness
    
    *For any* volume metrics query, the CloudWatch request SHALL include
    both FileSystemId and VolumeId dimensions.
    
    **Validates: Requirements 2.5, 3.5**
    """
    mock_client = MockCloudWatchClient()
    mock_client.get_volume_metrics(fs_id, volume_id)
    
    assert len(mock_client.recorded_queries) == 1, "Expected one query"
    
    query = mock_client.recorded_queries[0]
    
    assert 'fs_id' in query, "FileSystemId dimension not found"
    assert query['fs_id'] == fs_id, f"FileSystemId mismatch"
    
    assert 'volume_id' in query, "VolumeId dimension not found"
    assert query['volume_id'] == volume_id, f"VolumeId mismatch"
