"""Controller for orchestrating data fetching and model updates."""

import logging
import threading
import time
from typing import Optional, Callable, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from .model import Store, FileSystem, FileSystemType, DetailStore, Volume, MetadataServer
from .aws_client import FSxClient, CloudWatchClient, StaticPricingProvider

logger = logging.getLogger(__name__)


class Config:
    """Controller configuration."""
    
    def __init__(
        self,
        file_system_type: Optional[str] = None,
        name_filter: Optional[str] = None,
        refresh_interval: int = 300,
        metric_interval: int = 60,
    ):
        self.file_system_type = file_system_type
        self.name_filter = name_filter
        self.refresh_interval = refresh_interval
        self.metric_interval = metric_interval


class Controller:
    """Orchestrates data fetching and model updates."""
    
    def __init__(
        self,
        fsx_client: FSxClient,
        cw_client: CloudWatchClient,
        store: Store,
        pricing: StaticPricingProvider,
        config: Config,
    ):
        self._fsx_client = fsx_client
        self._cw_client = cw_client
        self._store = store
        self._pricing = pricing
        self._config = config
        self._running = False
        self._stop_event = threading.Event()
        self._threads: list = []
        self._on_update: Optional[Callable[[], None]] = None
        self._executor = ThreadPoolExecutor(max_workers=20)
    
    def on_update(self, callback: Callable[[], None]) -> None:
        """Register a callback for when data is updated."""
        self._on_update = callback
    
    def _notify_update(self) -> None:
        """Notify listeners of data update."""
        if self._on_update:
            self._on_update()
    
    def start(self) -> None:
        """Start the polling loops in background threads."""
        if self._running:
            return
        
        self._running = True
        self._stop_event.clear()
        
        # Initial fetch: file systems first (fast), then metrics in parallel
        self.refresh_file_systems()
        self.refresh_prices()
        self._notify_update()  # Show UI immediately with basic data
        
        # Fetch metrics in background (don't block UI)
        self._executor.submit(self._initial_metrics_fetch)
        
        # Start polling threads
        fs_thread = threading.Thread(target=self._poll_file_systems, daemon=True)
        metrics_thread = threading.Thread(target=self._poll_metrics, daemon=True)
        
        fs_thread.start()
        metrics_thread.start()
        
        self._threads = [fs_thread, metrics_thread]
    
    def _initial_metrics_fetch(self) -> None:
        """Fetch initial metrics in background."""
        self.refresh_metrics()
    
    def stop(self) -> None:
        """Stop the polling loops."""
        self._running = False
        self._stop_event.set()
        
        for thread in self._threads:
            thread.join(timeout=2.0)
        
        self._threads = []
        self._executor.shutdown(wait=False)
    
    def _poll_file_systems(self) -> None:
        """Polling loop for file systems."""
        while not self._stop_event.wait(self._config.refresh_interval):
            self.refresh_file_systems()
            self.refresh_prices()
    
    def _poll_metrics(self) -> None:
        """Polling loop for CloudWatch metrics."""
        while not self._stop_event.wait(self._config.metric_interval):
            self.refresh_metrics()
    
    def refresh_file_systems(self) -> None:
        """Fetch current file systems from AWS FSx API."""
        try:
            file_systems = self._fsx_client.list_file_systems(
                fs_type=self._config.file_system_type
            )
            
            # Track current IDs
            current_ids = set()
            
            for fs in file_systems:
                # Apply name filter if specified
                if self._config.name_filter:
                    if self._config.name_filter.lower() not in fs.name.lower():
                        continue
                
                current_ids.add(fs.id)
                self._store.add(fs)
            
            # Remove file systems that no longer exist
            for fs_id in self._store.ids():
                if fs_id not in current_ids:
                    self._store.delete(fs_id)
            
            self._notify_update()
        except Exception as e:
            logger.error(f"Failed to refresh file systems: {e}")
    
    def refresh_metrics(self) -> None:
        """Fetch CloudWatch metrics for all file systems in a single batched API call."""
        fs_ids = self._store.ids()
        if not fs_ids:
            return
        
        # Build list of (fs_id, fs_type, storage_capacity) for batch query
        file_systems_info = []
        lustre_fs_ids = []  # Track Lustre FS for separate CPU fetch
        
        for fs_id in fs_ids:
            fs = self._store.get(fs_id)
            if fs is not None:
                file_systems_info.append((fs.id, fs.type, fs.storage_capacity))
                if fs.type == FileSystemType.LUSTRE:
                    lustre_fs_ids.append(fs.id)
        
        if not file_systems_info:
            return
        
        try:
            # Fetch all metrics in one batched API call
            metrics_batch = self._cw_client.get_file_system_metrics_batch(file_systems_info)
            
            # Update each file system with its metrics
            for fs_id, metrics in metrics_batch.items():
                fs = self._store.get(fs_id)
                if fs is not None:
                    fs.update_metrics(metrics)
            
            # Recalculate pricing (capacity pool usage may have changed)
            self.refresh_prices()
            self._notify_update()
            
            # For Lustre file systems, fetch CPU separately (requires FileServer dimension)
            # This is done in parallel for efficiency
            if lustre_fs_ids:
                self._fetch_lustre_cpu_batch(lustre_fs_ids)
                
        except Exception as e:
            logger.warning(f"Failed to refresh metrics: {e}")
    
    def _fetch_lustre_cpu_batch(self, lustre_fs_ids: List[str]) -> None:
        """Fetch CPU metrics for Lustre file systems in parallel."""
        def fetch_cpu(fs_id: str) -> None:
            fs = self._store.get(fs_id)
            if fs is None:
                return
            try:
                metrics = self._cw_client.get_file_system_metrics(fs_id, fs.type)
                if metrics.cpu_utilization > 0:
                    fs.cpu_utilization = metrics.cpu_utilization
                    self._notify_update()
            except Exception as e:
                logger.warning(f"Failed to fetch Lustre CPU for {fs_id}: {e}")
        
        # Fetch all Lustre CPU metrics concurrently
        futures = [self._executor.submit(fetch_cpu, fs_id) for fs_id in lustre_fs_ids]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.warning(f"Lustre CPU fetch task failed: {e}")
    
    def refresh_prices(self) -> None:
        """Update pricing for all file systems."""
        def update_price(fs: FileSystem) -> None:
            price = self._pricing.file_system_price(fs)
            if price is not None:
                fs.set_price(price)
        
        self._store.for_each(update_price)
        self._notify_update()


class FileSystemNotFoundError(Exception):
    """Raised when the specified file system ID does not exist."""
    pass


class DetailController:
    """Controller for detail view mode.
    
    Orchestrates data fetching for a single file system's detailed metrics,
    including volume-level metrics for ONTAP/OpenZFS and MDS-level CPU
    metrics for Lustre.
    """
    
    def __init__(
        self,
        fsx_client: FSxClient,
        cw_client: CloudWatchClient,
        store: DetailStore,
        pricing: StaticPricingProvider,
        file_system_id: str,
        config: Config,
    ):
        """Initialize the DetailController.
        
        Args:
            fsx_client: FSx API client
            cw_client: CloudWatch API client
            store: DetailStore for thread-safe data storage
            pricing: Pricing provider for cost calculations
            file_system_id: The file system ID to monitor
            config: Controller configuration
        """
        self._fsx_client = fsx_client
        self._cw_client = cw_client
        self._store = store
        self._pricing = pricing
        self._file_system_id = file_system_id
        self._config = config
        self._running = False
        self._stop_event = threading.Event()
        self._threads: list = []
        self._on_update: Optional[Callable[[], None]] = None
        self._executor = ThreadPoolExecutor(max_workers=20)
        self._mds_cache: Optional[List[str]] = None  # Cache MDS list
    
    def on_update(self, callback: Callable[[], None]) -> None:
        """Register a callback for when data is updated."""
        self._on_update = callback
    
    def _notify_update(self) -> None:
        """Notify listeners of data update."""
        if self._on_update:
            self._on_update()
    
    def start(self) -> None:
        """Start polling for detail view data.
        
        Raises:
            FileSystemNotFoundError: If the specified file system does not exist
        """
        if self._running:
            return
        
        # Validate file system exists and fetch initial data
        fs = self._fetch_file_system()
        if fs is None:
            raise FileSystemNotFoundError(
                f"File system '{self._file_system_id}' not found"
            )
        
        self._store.set_file_system(fs)
        
        # Update pricing
        price = self._pricing.file_system_price(fs)
        if price is not None:
            fs.set_price(price)
        
        self._running = True
        self._stop_event.clear()
        
        # Show UI immediately with basic data
        self._notify_update()
        
        # Fetch detailed data in background (parallel)
        self._executor.submit(self._initial_fetch_async, fs)
        
        # Start polling threads
        fs_thread = threading.Thread(target=self._poll_file_system, daemon=True)
        metrics_thread = threading.Thread(target=self._poll_metrics, daemon=True)
        
        fs_thread.start()
        metrics_thread.start()
        
        self._threads = [fs_thread, metrics_thread]
    
    def _initial_fetch_async(self, fs: FileSystem) -> None:
        """Perform initial data fetch in parallel based on file system type."""
        futures = []
        
        # Always fetch file system metrics
        futures.append(self._executor.submit(self._refresh_file_system_metrics))
        
        # Fetch type-specific data in parallel
        if fs.type in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
            futures.append(self._executor.submit(self._fetch_volumes_and_metrics))
        elif fs.type == FileSystemType.LUSTRE:
            futures.append(self._executor.submit(self.refresh_mds_metrics))
        
        # Wait for all to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.warning(f"Initial fetch task failed: {e}")
        
        self._notify_update()
    
    def _fetch_volumes_and_metrics(self) -> None:
        """Fetch volumes and their metrics in one operation."""
        self.refresh_volumes()
        self._notify_update()  # Show volumes immediately
        self.refresh_volume_metrics()
    
    def stop(self) -> None:
        """Stop the polling loops."""
        self._running = False
        self._stop_event.set()
        
        for thread in self._threads:
            thread.join(timeout=2.0)
        
        self._threads = []
        self._executor.shutdown(wait=False)
    
    def _fetch_file_system(self) -> Optional[FileSystem]:
        """Fetch the file system by ID using direct lookup (more efficient).
        
        Returns:
            FileSystem if found, None otherwise
        """
        try:
            return self._fsx_client.get_file_system(self._file_system_id)
        except Exception as e:
            logger.warning(f"Failed to fetch file system {self._file_system_id}: {e}")
        return None
    
    def _poll_file_system(self) -> None:
        """Polling loop for file system metadata."""
        while not self._stop_event.wait(self._config.refresh_interval):
            fs = self._fetch_file_system()
            if fs is not None:
                # Preserve existing metrics
                existing = self._store.get_file_system()
                if existing:
                    fs.used_capacity = existing.used_capacity
                    fs.read_throughput = existing.read_throughput
                    fs.write_throughput = existing.write_throughput
                    fs.read_iops = existing.read_iops
                    fs.write_iops = existing.write_iops
                    fs.cpu_utilization = existing.cpu_utilization
                    fs.hourly_price = existing.hourly_price
                    fs.pricing_breakdown = existing.pricing_breakdown
                
                self._store.set_file_system(fs)
                
                # Update pricing (recalculate based on current config)
                price = self._pricing.file_system_price(fs)
                if price is not None:
                    fs.set_price(price)
                
                self._notify_update()
    
    def _poll_metrics(self) -> None:
        """Polling loop for CloudWatch metrics."""
        while not self._stop_event.wait(self._config.metric_interval):
            fs = self._store.get_file_system()
            if fs is None:
                continue
            
            # Refresh all metrics in parallel
            futures = [self._executor.submit(self._refresh_file_system_metrics)]
            
            if fs.type in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
                futures.append(self._executor.submit(self._fetch_volumes_and_metrics))
            elif fs.type == FileSystemType.LUSTRE:
                futures.append(self._executor.submit(self.refresh_mds_metrics))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.warning(f"Metrics polling task failed: {e}")
            
            self._notify_update()
    
    def _refresh_file_system_metrics(self) -> None:
        """Fetch CloudWatch metrics for the file system."""
        fs = self._store.get_file_system()
        if fs is None:
            return
        
        try:
            metrics = self._cw_client.get_file_system_metrics(fs.id, fs.type)
            # Handle free_capacity (negative value means we need to calculate used)
            if metrics.used_capacity < 0:
                free_gib = -metrics.used_capacity
                metrics.used_capacity = max(0, fs.storage_capacity - free_gib)
            fs.update_metrics(metrics)
            
            # Recalculate pricing (capacity pool usage may have changed)
            price = self._pricing.file_system_price(fs)
            if price is not None:
                fs.set_price(price)
            
            self._notify_update()
        except Exception as e:
            logger.warning(f"Failed to refresh file system metrics: {e}")
    
    def refresh_volumes(self) -> None:
        """Fetch volumes for ONTAP/OpenZFS file systems."""
        fs = self._store.get_file_system()
        if fs is None:
            return
        
        if fs.type not in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
            return
        
        try:
            volumes = self._fsx_client.describe_volumes(self._file_system_id)
            for vol in volumes:
                self._store.add_volume(vol)
        except Exception as e:
            logger.warning(f"Failed to refresh volumes: {e}")
    
    def refresh_volume_metrics(self) -> None:
        """Fetch CloudWatch metrics for all volumes in a single batched API call."""
        volumes = self._store.get_volumes()
        if not volumes:
            return
        
        # Get all volume IDs
        volume_ids = [vol.id for vol in volumes]
        fs = self._store.get_file_system()
        if fs is None:
            return
        
        # Fetch all metrics in one batched API call
        metrics_batch = self._cw_client.get_volume_metrics_batch(
            fs.id, volume_ids
        )
        
        # Update each volume with its metrics
        for vol in volumes:
            if vol.id in metrics_batch:
                metrics = metrics_batch[vol.id]
                vol.read_throughput = metrics.get('read_throughput', 0.0)
                vol.write_throughput = metrics.get('write_throughput', 0.0)
                vol.read_iops = metrics.get('read_iops', 0.0)
                vol.write_iops = metrics.get('write_iops', 0.0)
                vol.used_capacity = metrics.get('used_capacity', 0)
                # Use CloudWatch capacity if available, otherwise keep API capacity
                cw_capacity = metrics.get('storage_capacity', 0)
                if cw_capacity > 0:
                    vol.storage_capacity = cw_capacity
                # If both CW and API capacity are 0, volume might be newly created
                self._store.add_volume(vol)
        
        self._notify_update()
    
    def refresh_mds_metrics(self) -> None:
        """Discover MDS servers and fetch CPU for each."""
        fs = self._store.get_file_system()
        if fs is None:
            return
        
        if fs.type != FileSystemType.LUSTRE:
            return
        
        try:
            # Use cached MDS list if available, otherwise discover
            if self._mds_cache is None:
                self._mds_cache = self._cw_client.get_lustre_mds_list(self._file_system_id)
            
            mds_ids = self._mds_cache
            if not mds_ids:
                return
            
            # Fetch all MDS CPU metrics in a single batched API call
            cpu_metrics = self._cw_client.get_lustre_mds_cpu_batch(
                self._file_system_id, mds_ids
            )
            
            # Update store with results
            for mds_id, cpu in cpu_metrics.items():
                mds = MetadataServer(
                    id=mds_id,
                    file_system_id=self._file_system_id,
                    cpu_utilization=cpu,
                )
                self._store.add_mds(mds)
            
            self._notify_update()
        except Exception as e:
            logger.warning(f"Failed to refresh MDS metrics: {e}")
