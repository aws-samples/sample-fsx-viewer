"""Data models for FSx file systems."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import RLock
from typing import Dict, List, Optional, Callable


class FileSystemType(str, Enum):
    """FSx file system types."""
    LUSTRE = "LUSTRE"
    WINDOWS = "WINDOWS"
    ONTAP = "ONTAP"
    OPENZFS = "OPENZFS"


@dataclass
class PricingBreakdown:
    """Itemized monthly cost breakdown for a file system."""
    storage: float = 0.0
    throughput: float = 0.0
    iops: float = 0.0
    capacity_pool: float = 0.0

    @property
    def total(self) -> float:
        """Total monthly cost."""
        return self.storage + self.throughput + self.iops + self.capacity_pool


@dataclass
class Metrics:
    """CloudWatch metrics for a file system."""
    used_capacity: int = 0  # GiB
    read_throughput: float = 0.0  # MiB/s
    write_throughput: float = 0.0  # MiB/s
    read_iops: float = 0.0
    write_iops: float = 0.0
    cpu_utilization: float = 0.0  # percentage (0-100)
    capacity_pool_used_gb: Optional[float] = None  # ONTAP capacity pool usage in GB


@dataclass
class FileSystem:
    """Represents an FSx file system with its metrics."""
    id: str
    name: str
    type: FileSystemType
    storage_capacity: int  # GiB
    creation_time: datetime
    lifecycle: str
    
    # Configuration for pricing
    deployment_type: str = "SINGLE_AZ"  # SINGLE_AZ, MULTI_AZ, etc.
    storage_type: str = "SSD"  # SSD, HDD
    throughput_capacity: int = 0  # MBps (provisioned throughput)
    provisioned_iops: int = 0  # Provisioned IOPS (if applicable)
    
    # Metrics
    used_capacity: int = 0
    read_throughput: float = 0.0
    write_throughput: float = 0.0
    read_iops: float = 0.0
    write_iops: float = 0.0
    cpu_utilization: float = 0.0  # percentage (0-100)
    
    # Pricing
    hourly_price: float = 0.0
    pricing_breakdown: Optional['PricingBreakdown'] = None
    capacity_pool_used_gb: Optional[float] = None
    
    # Display state
    visible: bool = True
    
    def utilization(self) -> float:
        """Return storage utilization as a percentage (0.0 to 1.0)."""
        if self.storage_capacity <= 0:
            return 0.0
        util = self.used_capacity / self.storage_capacity
        return max(0.0, min(1.0, util))
    
    def total_iops(self) -> float:
        """Return combined read + write IOPS."""
        return self.read_iops + self.write_iops
    
    def total_throughput(self) -> float:
        """Return combined read + write throughput in MiB/s."""
        return self.read_throughput + self.write_throughput
    
    def monthly_price(self) -> float:
        """Return estimated monthly cost."""
        if self.pricing_breakdown is not None:
            return self.pricing_breakdown.total
        return self.hourly_price * 730
    
    def has_price(self) -> bool:
        """Return True if pricing data is available."""
        if self.pricing_breakdown is not None:
            return self.pricing_breakdown.total > 0
        return self.hourly_price > 0
    
    def update_metrics(self, metrics: Metrics) -> None:
        """Update the performance metrics."""
        self.used_capacity = metrics.used_capacity
        self.read_throughput = metrics.read_throughput
        self.write_throughput = metrics.write_throughput
        self.read_iops = metrics.read_iops
        self.write_iops = metrics.write_iops
        self.cpu_utilization = metrics.cpu_utilization
        if metrics.capacity_pool_used_gb is not None:
            self.capacity_pool_used_gb = metrics.capacity_pool_used_gb
    
    def set_price(self, price) -> None:
        """Set pricing. Accepts float (hourly) or PricingBreakdown (monthly)."""
        if isinstance(price, PricingBreakdown):
            self.pricing_breakdown = price
            self.hourly_price = price.total / 730 if price.total > 0 else 0.0
        else:
            self.hourly_price = price
    
    def show(self) -> None:
        """Mark the file system as visible."""
        self.visible = True
    
    def hide(self) -> None:
        """Mark the file system as hidden."""
        self.visible = False


@dataclass
class Stats:
    """Aggregate statistics for all visible file systems."""
    total_file_systems: int = 0
    total_capacity: int = 0  # GiB
    total_used_capacity: int = 0  # GiB
    total_hourly_cost: float = 0.0
    count_by_type: Dict[FileSystemType, int] = field(default_factory=dict)
    file_systems: List[FileSystem] = field(default_factory=list)


@dataclass
class Volume:
    """ONTAP or OpenZFS volume with metrics."""
    id: str                    # vol-xxx
    name: str                  # Volume name
    file_system_id: str        # Parent fs-xxx
    type: str                  # "ONTAP" or "OPENZFS"
    storage_capacity: int      # GiB
    used_capacity: int = 0     # GiB
    read_iops: float = 0.0
    write_iops: float = 0.0
    read_throughput: float = 0.0   # MiB/s
    write_throughput: float = 0.0  # MiB/s
    
    def utilization(self) -> float:
        """Return storage utilization as 0.0-1.0."""
        if self.storage_capacity <= 0:
            return 0.0
        return min(1.0, self.used_capacity / self.storage_capacity)
    
    def total_iops(self) -> float:
        """Return combined read + write IOPS."""
        return self.read_iops + self.write_iops
    
    def total_throughput(self) -> float:
        """Return combined read + write throughput in MiB/s."""
        return self.read_throughput + self.write_throughput


@dataclass
class MetadataServer:
    """Lustre MDS/MDT server with CPU metrics."""
    id: str                    # e.g., "MDS0000", "MDS0001"
    file_system_id: str        # Parent fs-xxx
    cpu_utilization: float = 0.0  # 0-100 percentage


class DetailStore:
    """Thread-safe store for detail view data."""
    
    def __init__(self):
        self._lock = RLock()
        self._file_system: Optional[FileSystem] = None
        self._volumes: Dict[str, Volume] = {}
        self._mds_servers: Dict[str, MetadataServer] = {}
    
    def set_file_system(self, fs: FileSystem) -> None:
        """Set the file system for detail view."""
        with self._lock:
            self._file_system = fs
    
    def get_file_system(self) -> Optional[FileSystem]:
        """Get the file system for detail view."""
        with self._lock:
            return self._file_system
    
    def add_volume(self, vol: Volume) -> None:
        """Add or update a volume in the store."""
        with self._lock:
            self._volumes[vol.id] = vol
    
    def get_volumes(self) -> List[Volume]:
        """Get all volumes sorted by ID."""
        with self._lock:
            return sorted(self._volumes.values(), key=lambda v: v.id)
    
    def add_mds(self, mds: MetadataServer) -> None:
        """Add or update an MDS server in the store."""
        with self._lock:
            self._mds_servers[mds.id] = mds
    
    def get_mds_servers(self) -> List[MetadataServer]:
        """Get all MDS servers sorted by ID."""
        with self._lock:
            return sorted(self._mds_servers.values(), key=lambda m: m.id)


class Store:
    """Thread-safe store for file systems."""
    
    def __init__(self):
        self._lock = RLock()
        self._file_systems: Dict[str, FileSystem] = {}
    
    def add(self, fs: FileSystem) -> FileSystem:
        """Add or update a file system in the store."""
        with self._lock:
            if fs.id in self._file_systems:
                existing = self._file_systems[fs.id]
                existing.name = fs.name
                existing.type = fs.type
                existing.storage_capacity = fs.storage_capacity
                existing.creation_time = fs.creation_time
                existing.lifecycle = fs.lifecycle
                # Update pricing configuration
                existing.deployment_type = fs.deployment_type
                existing.storage_type = fs.storage_type
                existing.throughput_capacity = fs.throughput_capacity
                existing.provisioned_iops = fs.provisioned_iops
                return existing
            self._file_systems[fs.id] = fs
            return fs
    
    def delete(self, fs_id: str) -> None:
        """Remove a file system by ID."""
        with self._lock:
            self._file_systems.pop(fs_id, None)
    
    def get(self, fs_id: str) -> Optional[FileSystem]:
        """Retrieve a file system by ID."""
        with self._lock:
            return self._file_systems.get(fs_id)
    
    def for_each(self, fn: Callable[[FileSystem], None]) -> None:
        """Iterate over all file systems."""
        with self._lock:
            for fs in self._file_systems.values():
                fn(fs)
    
    def ids(self) -> List[str]:
        """Return all file system IDs."""
        with self._lock:
            return list(self._file_systems.keys())
    
    def count(self) -> int:
        """Return total number of file systems (including hidden)."""
        with self._lock:
            return len(self._file_systems)
    
    def stats(self) -> Stats:
        """Return aggregate statistics for all visible file systems."""
        with self._lock:
            stats = Stats()
            stats.count_by_type = {}
            stats.file_systems = []
            
            for fs in self._file_systems.values():
                if not fs.visible:
                    continue
                
                stats.total_file_systems += 1
                stats.total_capacity += fs.storage_capacity
                stats.total_used_capacity += fs.used_capacity
                stats.total_hourly_cost += fs.hourly_price
                
                if fs.type not in stats.count_by_type:
                    stats.count_by_type[fs.type] = 0
                stats.count_by_type[fs.type] += 1
                
                stats.file_systems.append(fs)
            
            return stats
