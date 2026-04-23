"""AWS client wrappers for FSx, CloudWatch, and Pricing."""

import boto3
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Tuple

from .model import FileSystem, FileSystemType, Metrics, Volume, MetadataServer, PricingBreakdown, AccessPoint, PerfMetrics, LatencyMetrics

logger = logging.getLogger(__name__)


def create_session(region: str, profile: Optional[str] = None) -> boto3.Session:
    """Create a shared boto3 session for all clients."""
    return boto3.Session(profile_name=profile, region_name=region)


class FSxClient:
    """Wrapper for AWS FSx API."""
    
    def __init__(self, region: str, profile: Optional[str] = None, session: Optional[boto3.Session] = None):
        if session is None:
            session = boto3.Session(profile_name=profile, region_name=region)
        self._client = session.client('fsx')
        self._session = session
        self._ec2_client = None
        self._az_cache: Dict[str, str] = {}  # subnet_id -> AZ name

    def resolve_azs(self, subnet_ids: List[str]) -> List[str]:
        """Resolve subnet IDs to availability-zone names, cached per client.

        Returns an empty list (and logs a warning once) when the caller lacks
        ``ec2:DescribeSubnets`` permission.
        """
        missing = [s for s in subnet_ids if s and s not in self._az_cache]
        if missing:
            try:
                if self._ec2_client is None:
                    self._ec2_client = self._session.client('ec2')
                resp = self._ec2_client.describe_subnets(SubnetIds=missing)
                for s in resp.get('Subnets', []):
                    self._az_cache[s['SubnetId']] = s.get('AvailabilityZone', '')
            except Exception as e:
                logger.warning(f"Failed to resolve AZs for subnets {missing}: {e}")
                # Cache empty to avoid retrying every cycle
                for s in missing:
                    self._az_cache.setdefault(s, '')
        # Preserve positional alignment with subnet_ids (empty string when
        # resolution failed) so callers can zip(subnet_ids, availability_zones).
        return [self._az_cache.get(s, '') for s in subnet_ids]
    
    def list_file_systems(self, fs_type: Optional[str] = None) -> List[FileSystem]:
        """List all FSx file systems, optionally filtered by type."""
        file_systems = []
        paginator = self._client.get_paginator('describe_file_systems')
        
        for page in paginator.paginate():
            for fs in page.get('FileSystems', []):
                # Filter by type if specified
                if fs_type and fs.get('FileSystemType') != fs_type:
                    continue
                
                file_system = self._parse_file_system(fs)
                file_systems.append(file_system)
        
        return file_systems
    
    def get_file_system(self, file_system_id: str) -> Optional[FileSystem]:
        """Get a specific file system by ID (more efficient than list_file_systems).
        
        Args:
            file_system_id: The FSx file system ID (fs-xxx)
            
        Returns:
            FileSystem if found, None otherwise
        """
        try:
            response = self._client.describe_file_systems(
                FileSystemIds=[file_system_id]
            )
            file_systems = response.get('FileSystems', [])
            if file_systems:
                return self._parse_file_system(file_systems[0])
        except Exception as e:
            logger.warning(f"Failed to describe file system {file_system_id}: {e}")
        return None
    
    def _parse_file_system(self, fs: dict) -> FileSystem:
        """Parse a file system response dict into a FileSystem object."""
        # Parse name from tags
        name = self._parse_name_tag(fs.get('Tags', []))
        fs_id = fs.get('FileSystemId', '')
        fs_type_enum = FileSystemType(fs.get('FileSystemType', 'LUSTRE'))
        
        # Extract type-specific configuration for pricing
        deployment_type, storage_type, throughput_capacity, provisioned_iops = \
            self._extract_pricing_config(fs, fs_type_enum)

        # HA pairs (ONTAP only) and subnet placement (all types)
        ha_pairs = 1
        management_ip = None
        if fs_type_enum == FileSystemType.ONTAP:
            ontap_config = fs.get('OntapConfiguration', {})
            ha_pairs = ontap_config.get('HAPairs', 1) or 1
            # Management endpoint IP for SSH (first IP if multiple).
            mgmt = (ontap_config.get('Endpoints') or {}).get('Management') or {}
            ips = mgmt.get('IpAddresses') or []
            if ips:
                management_ip = ips[0]

        return FileSystem(
            id=fs_id,
            name=name if name else fs_id,
            type=fs_type_enum,
            storage_capacity=fs.get('StorageCapacity', 0),
            creation_time=fs.get('CreationTime', datetime.now(timezone.utc)),
            lifecycle=fs.get('Lifecycle', 'UNKNOWN'),
            deployment_type=deployment_type,
            storage_type=storage_type,
            throughput_capacity=throughput_capacity,
            provisioned_iops=provisioned_iops,
            ha_pairs=ha_pairs,
            subnet_ids=list(fs.get('SubnetIds', []) or []),
            preferred_subnet_id=fs.get('PreferredSubnetId') or None,
            management_ip=management_ip,
        )
    
    def _extract_pricing_config(self, fs: dict, fs_type: FileSystemType) -> tuple:
        """Extract pricing-relevant configuration from file system response.
        
        Returns:
            Tuple of (deployment_type, storage_type, throughput_capacity, provisioned_iops)
        """
        deployment_type = "SINGLE_AZ"
        storage_type = "SSD"
        throughput_capacity = 0
        provisioned_iops = 0
        
        if fs_type == FileSystemType.ONTAP:
            ontap_config = fs.get('OntapConfiguration', {})
            deployment_type = ontap_config.get('DeploymentType', 'SINGLE_AZ_1')
            throughput_capacity = ontap_config.get('ThroughputCapacity', 0)
            # ONTAP uses ThroughputCapacityPerHAPair for newer deployments
            if throughput_capacity == 0:
                throughput_capacity = ontap_config.get('ThroughputCapacityPerHAPair', 0)
            # ONTAP disk IOPS configuration
            disk_iops = ontap_config.get('DiskIopsConfiguration', {})
            provisioned_iops = disk_iops.get('Iops', 0)
            storage_type = "SSD"  # ONTAP primary storage is always SSD
            
        elif fs_type == FileSystemType.OPENZFS:
            zfs_config = fs.get('OpenZFSConfiguration', {})
            deployment_type = zfs_config.get('DeploymentType', 'SINGLE_AZ_1')
            throughput_capacity = zfs_config.get('ThroughputCapacity', 0)
            # OpenZFS disk IOPS configuration
            disk_iops = zfs_config.get('DiskIopsConfiguration', {})
            provisioned_iops = disk_iops.get('Iops', 0)
            storage_type = "SSD"  # OpenZFS uses SSD
            
        elif fs_type == FileSystemType.WINDOWS:
            win_config = fs.get('WindowsConfiguration', {})
            deployment_type = win_config.get('DeploymentType', 'SINGLE_AZ_1')
            throughput_capacity = win_config.get('ThroughputCapacity', 0)
            # Windows disk IOPS configuration
            disk_iops = win_config.get('DiskIopsConfiguration', {})
            provisioned_iops = disk_iops.get('Iops', 0)
            # Windows can be SSD or HDD
            storage_type = fs.get('StorageType', 'SSD')
            
        elif fs_type == FileSystemType.LUSTRE:
            lustre_config = fs.get('LustreConfiguration', {})
            deployment_type = lustre_config.get('DeploymentType', 'SCRATCH_1')
            # Lustre throughput is per unit of storage
            throughput_capacity = lustre_config.get('PerUnitStorageThroughput', 0)
            # Lustre metadata IOPS
            metadata_config = lustre_config.get('MetadataConfiguration', {})
            provisioned_iops = metadata_config.get('Iops', 0)
            # Lustre can be SSD or HDD based on deployment type
            if 'HDD' in deployment_type or deployment_type in ('PERSISTENT_HDD',):
                storage_type = "HDD"
            else:
                storage_type = "SSD"
        
        return deployment_type, storage_type, throughput_capacity, provisioned_iops
    
    def _parse_name_tag(self, tags: List[Dict[str, str]]) -> str:
        """Extract the Name tag value from a list of tags."""
        for tag in tags:
            if tag.get('Key') == 'Name':
                return tag.get('Value', '')
        return ''
    
    def describe_volumes(self, file_system_id: str) -> List[Volume]:
        """List all volumes for an ONTAP or OpenZFS file system.
        
        Args:
            file_system_id: The FSx file system ID (fs-xxx)
            
        Returns:
            List of Volume objects with basic metadata (metrics populated separately)
        """
        volumes = []
        paginator = self._client.get_paginator('describe_volumes')
        
        try:
            for page in paginator.paginate(
                Filters=[{'Name': 'file-system-id', 'Values': [file_system_id]}]
            ):
                for vol in page.get('Volumes', []):
                    volume = Volume(
                        id=vol['VolumeId'],
                        name=vol.get('Name', vol['VolumeId']),
                        file_system_id=file_system_id,
                        type=vol['VolumeType'],
                        storage_capacity=self._get_volume_capacity(vol),
                        used_capacity=0,  # Populated from CloudWatch
                        read_iops=0.0,
                        write_iops=0.0,
                        read_throughput=0.0,
                        write_throughput=0.0,
                    )
                    volumes.append(volume)
        except Exception as e:
            logger.warning(f"Failed to describe volumes for file system {file_system_id}: {e}")
        
        return volumes
    
    def _get_volume_capacity(self, vol: dict) -> int:
        """Extract capacity from ONTAP or OpenZFS volume config.
        
        Args:
            vol: Volume response dict from describe_volumes
            
        Returns:
            Storage capacity in GiB
        """
        vol_type = vol.get('VolumeType', '')
        
        if vol_type == 'ONTAP':
            # ONTAP stores size in megabytes
            size_mb = vol.get('OntapConfiguration', {}).get('SizeInMegabytes', 0)
            return size_mb // 1024 if size_mb else 0
        elif vol_type == 'OPENZFS':
            # OpenZFS: prefer quota, fall back to reservation, then 0 (unlimited)
            zfs_config = vol.get('OpenZFSConfiguration', {})
            quota = zfs_config.get('StorageCapacityQuotaGiB')
            if quota is not None and quota > 0:
                return quota
            # Try reservation as fallback
            reservation = zfs_config.get('StorageCapacityReservationGiB')
            if reservation is not None and reservation > 0:
                return reservation
            # No quota/reservation means unlimited (will show as 0)
            return 0
        
        return 0


    def describe_s3_access_points(self, file_system_id: str) -> Dict[str, List[AccessPoint]]:
        """List S3 access points for all volumes of a file system.

        Returns a mapping of volume_id -> list of AccessPoint. Volumes with no
        attachments are simply absent from the mapping.

        Raises the underlying boto3 exception on failure (caller decides to
        log/degrade). Returns {} if the FSx API does not support this operation
        in the installed boto3 (shouldn't happen post-June-2025 models).
        """
        result: Dict[str, List[AccessPoint]] = {}
        if not hasattr(self._client, 'describe_s3_access_point_attachments'):
            return result

        paginator = self._client.get_paginator('describe_s3_access_point_attachments')
        pages = paginator.paginate(
            Filters=[{'Name': 'file-system-id', 'Values': [file_system_id]}]
        )
        for page in pages:
            for att in page.get('S3AccessPointAttachments', []):
                ap_info = att.get('S3AccessPoint', {})
                vol_id = (
                    att.get('OntapConfiguration', {}).get('VolumeId')
                    or att.get('OpenZFSConfiguration', {}).get('VolumeId')
                )
                if not vol_id:
                    continue
                ap = AccessPoint(
                    name=att.get('Name', ''),
                    alias=ap_info.get('Alias', ''),
                    lifecycle=att.get('Lifecycle', ''),
                    vpc_id=(ap_info.get('VpcConfiguration') or {}).get('VpcId'),
                )
                result.setdefault(vol_id, []).append(ap)
        return result


class CloudWatchClient:
    """Wrapper for AWS CloudWatch API."""
    
    def __init__(self, region: str, profile: Optional[str] = None, session: Optional[boto3.Session] = None):
        if session is None:
            session = boto3.Session(profile_name=profile, region_name=region)
        self._client = session.client('cloudwatch')
    
    def get_file_system_metrics(self, fs_id: str, fs_type: FileSystemType) -> Metrics:
        """Retrieve CloudWatch metrics for a specific file system.

        For the detail view, this single call also pulls the file-server
        performance utilization metrics and attaches them to
        ``Metrics.perf_metrics`` so callers don't need a second GetMetricData.
        """
        metrics = Metrics()
        end_time = datetime.now(timezone.utc)
        # 10-minute window covers the 300s-Period perf burst-balance queries
        # (FileServerDiskThroughputBalance / FileServerDiskIopsBalance) that
        # are folded into this request. ScanBy=TimestampDescending ensures the
        # 60s-Period metrics still return their most-recent datapoint first.
        start_time = end_time - timedelta(minutes=10)
        
        # Define metrics based on file system type
        metric_queries = self._build_metric_queries(fs_id, fs_type)
        # Append perf utilization queries so we can issue a single GetMetricData.
        perf_queries, perf_attr_map = self._build_perf_queries(fs_id, fs_type)
        metric_queries.extend(perf_queries)
        # Append latency math queries (read/write/metadata ms per op).
        latency_queries, latency_attr_map = self._build_latency_queries(fs_id, fs_type)
        metric_queries.extend(latency_queries)

        if not metric_queries:
            return metrics

        try:
            response = self._client.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time,
                ScanBy='TimestampDescending',
            )
            
            for result in response.get('MetricDataResults', []):
                values = result.get('Values', [])
                if not values:
                    continue
                
                value = values[0]
                metric_id = result.get('Id', '')
                
                if metric_id == 'read_bytes':
                    # CloudWatch returns bytes per period, convert to MiB/s
                    metrics.read_throughput = value / (1024 * 1024) / 60  # bytes -> MiB/s
                elif metric_id == 'write_bytes':
                    metrics.write_throughput = value / (1024 * 1024) / 60  # bytes -> MiB/s
                elif metric_id == 'read_ops':
                    metrics.read_iops = value / 60  # ops per period -> ops per second
                elif metric_id == 'write_ops':
                    metrics.write_iops = value / 60  # ops per period -> ops per second
                elif metric_id == 'free_capacity':
                    # Free capacity in bytes, store as negative to signal it needs adjustment
                    metrics.used_capacity = -int(value / (1024 * 1024 * 1024))  # bytes -> GiB (negative)
                elif metric_id == 'used_capacity':
                    metrics.used_capacity = int(value / (1024 * 1024 * 1024))  # bytes -> GiB
                elif metric_id == 'cpu_util':
                    metrics.cpu_utilization = value  # Already a percentage
                elif metric_id == 'cpu_util_avg':
                    metrics.cpu_utilization = value  # Average from SEARCH expression
                elif metric_id == 'capacity_pool_used':
                    metrics.capacity_pool_used_gb = value / (1024 * 1024 * 1024)  # bytes -> GB
                elif metric_id in perf_attr_map:
                    if metrics.perf_metrics is None:
                        metrics.perf_metrics = PerfMetrics()
                    setattr(metrics.perf_metrics, perf_attr_map[metric_id], float(value))
                elif metric_id in latency_attr_map:
                    if metrics.latency_metrics is None:
                        metrics.latency_metrics = LatencyMetrics()
                    setattr(metrics.latency_metrics, latency_attr_map[metric_id], float(value))
                    logger.debug(
                        "latency %s=%.3f ms for %s", latency_attr_map[metric_id], float(value), fs_id,
                    )
            # For Lustre, fetch CPU separately with FileServer dimension
            if fs_type == FileSystemType.LUSTRE and metrics.cpu_utilization == 0:
                cpu = self._get_lustre_cpu(fs_id, start_time, end_time)
                if cpu is not None:
                    metrics.cpu_utilization = cpu
                    
        except Exception as e:
            logger.warning(f"Failed to get metrics for file system {fs_id}: {e}")
        
        return metrics
    
    def _get_lustre_cpu(self, fs_id: str, start_time: datetime, end_time: datetime) -> Optional[float]:
        """Get CPU utilization for Lustre file system (requires FileServer dimension)."""
        try:
            # First, find the FileServer values for this file system
            list_response = self._client.list_metrics(
                Namespace='AWS/FSx',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'FileSystemId', 'Value': fs_id}]
            )
            
            if not list_response.get('Metrics'):
                return None
            
            # Get CPU for each FileServer and average them
            cpu_values = []
            for metric in list_response['Metrics']:
                dims = {d['Name']: d['Value'] for d in metric['Dimensions']}
                file_server = dims.get('FileServer')
                if not file_server:
                    continue
                
                response = self._client.get_metric_data(
                    MetricDataQueries=[{
                        'Id': 'cpu',
                        'MetricStat': {
                            'Metric': {
                                'Namespace': 'AWS/FSx',
                                'MetricName': 'CPUUtilization',
                                'Dimensions': [
                                    {'Name': 'FileSystemId', 'Value': fs_id},
                                    {'Name': 'FileServer', 'Value': file_server},
                                ],
                            },
                            'Period': 60,
                            'Stat': 'Average',
                        },
                    }],
                    StartTime=start_time,
                    EndTime=end_time,
                )
                
                for result in response.get('MetricDataResults', []):
                    values = result.get('Values', [])
                    if values:
                        cpu_values.append(values[0])
            
            if cpu_values:
                return sum(cpu_values) / len(cpu_values)
        except Exception as e:
            logger.warning(f"Failed to get Lustre CPU for file system {fs_id}: {e}")
        return None
    
    def get_file_system_metrics_batch(
        self, 
        file_systems: List[Tuple[str, FileSystemType, int]]
    ) -> Dict[str, Metrics]:
        """Get CloudWatch metrics for multiple file systems in a single API call.
        
        This is much more efficient than calling get_file_system_metrics for each FS.
        CloudWatch allows up to 500 metric queries per request.
        
        Args:
            file_systems: List of tuples (fs_id, fs_type, storage_capacity)
            
        Returns:
            Dict mapping fs_id to Metrics object
        """
        # Initialize results
        results = {fs_id: Metrics() for fs_id, _, _ in file_systems}
        
        if not file_systems:
            return results
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        namespace = 'AWS/FSx'
        
        # Build queries for all file systems
        # Each FS needs ~6 metrics, max 500 queries total = ~83 file systems
        queries = []
        fs_info = {}  # Map fs_id to (type, capacity) for processing
        
        max_fs = min(len(file_systems), 70)  # Leave room for ~7 metrics per FS
        
        for i, (fs_id, fs_type, storage_capacity) in enumerate(file_systems[:max_fs]):
            fs_info[fs_id] = (fs_type, storage_capacity)
            dimension = [{'Name': 'FileSystemId', 'Value': fs_id}]
            
            # Common metrics for all types
            queries.extend([
                {
                    'Id': f'rb_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataReadBytes', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{fs_id}|read_bytes',
                },
                {
                    'Id': f'wb_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteBytes', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{fs_id}|write_bytes',
                },
                {
                    'Id': f'ro_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataReadOperations', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{fs_id}|read_ops',
                },
                {
                    'Id': f'wo_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteOperations', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{fs_id}|write_ops',
                },
            ])
            
            # Type-specific capacity and CPU metrics
            if fs_type == FileSystemType.LUSTRE:
                queries.append({
                    'Id': f'fc_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'FreeDataStorageCapacity', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{fs_id}|free_capacity',
                })
            elif fs_type == FileSystemType.WINDOWS:
                queries.extend([
                    {
                        'Id': f'fc_{i}',
                        'MetricStat': {
                            'Metric': {'Namespace': namespace, 'MetricName': 'FreeStorageCapacity', 'Dimensions': dimension},
                            'Period': 60, 'Stat': 'Average',
                        },
                        'Label': f'{fs_id}|free_capacity',
                    },
                    {
                        'Id': f'cpu_{i}',
                        'MetricStat': {
                            'Metric': {'Namespace': namespace, 'MetricName': 'CPUUtilization', 'Dimensions': dimension},
                            'Period': 60, 'Stat': 'Average',
                        },
                        'Label': f'{fs_id}|cpu',
                    },
                ])
            elif fs_type == FileSystemType.ONTAP:
                queries.extend([
                    {
                        'Id': f'uc_{i}',
                        'MetricStat': {
                            'Metric': {'Namespace': namespace, 'MetricName': 'StorageUsed', 'Dimensions': dimension},
                            'Period': 60, 'Stat': 'Average',
                        },
                        'Label': f'{fs_id}|used_capacity',
                    },
                    {
                        'Id': f'cpu_{i}',
                        'MetricStat': {
                            'Metric': {'Namespace': namespace, 'MetricName': 'CPUUtilization', 'Dimensions': dimension},
                            'Period': 60, 'Stat': 'Average',
                        },
                        'Label': f'{fs_id}|cpu',
                    },
                ])
            elif fs_type == FileSystemType.OPENZFS:
                queries.extend([
                    {
                        'Id': f'uc_{i}',
                        'MetricStat': {
                            'Metric': {'Namespace': namespace, 'MetricName': 'UsedStorageCapacity', 'Dimensions': dimension},
                            'Period': 60, 'Stat': 'Average',
                        },
                        'Label': f'{fs_id}|used_capacity',
                    },
                    {
                        'Id': f'cpu_{i}',
                        'MetricStat': {
                            'Metric': {'Namespace': namespace, 'MetricName': 'CPUUtilization', 'Dimensions': dimension},
                            'Period': 60, 'Stat': 'Average',
                        },
                        'Label': f'{fs_id}|cpu',
                    },
                ])
        
        if not queries:
            return results
        
        try:
            response = self._client.get_metric_data(
                MetricDataQueries=queries,
                StartTime=start_time,
                EndTime=end_time,
            )
            
            for metric_result in response.get('MetricDataResults', []):
                label = metric_result.get('Label', '')
                values = metric_result.get('Values', [])
                
                if not label or not values or '|' not in label:
                    continue
                
                fs_id, metric_type = label.split('|', 1)
                value = values[0]
                
                if fs_id not in results:
                    continue
                
                metrics = results[fs_id]
                fs_type, storage_capacity = fs_info.get(fs_id, (None, 0))
                
                if metric_type == 'read_bytes':
                    metrics.read_throughput = value / (1024 * 1024) / 60
                elif metric_type == 'write_bytes':
                    metrics.write_throughput = value / (1024 * 1024) / 60
                elif metric_type == 'read_ops':
                    metrics.read_iops = value / 60
                elif metric_type == 'write_ops':
                    metrics.write_iops = value / 60
                elif metric_type == 'free_capacity':
                    # Convert free to used
                    free_gib = int(value / (1024 * 1024 * 1024))
                    metrics.used_capacity = max(0, storage_capacity - free_gib)
                elif metric_type == 'used_capacity':
                    metrics.used_capacity = int(value / (1024 * 1024 * 1024))
                elif metric_type == 'cpu':
                    metrics.cpu_utilization = value
                    
        except Exception as e:
            logger.warning(f"Failed to get batch metrics for file systems: {e}")
        
        # For Lustre file systems, we need to fetch CPU separately (requires FileServer dimension)
        # This is done in parallel by the controller
        
        return results
    
    def get_volume_metrics(self, fs_id: str, volume_id: str) -> Dict[str, float]:
        """Get CloudWatch metrics for a specific volume.
        
        Queries DataReadBytes, DataWriteBytes, DataReadOperations, DataWriteOperations,
        and StorageUsed using FileSystemId + VolumeId dimensions.
        
        Args:
            fs_id: The FSx file system ID (fs-xxx)
            volume_id: The volume ID (fsvol-xxx)
            
        Returns:
            Dict with keys: read_throughput, write_throughput, read_iops, write_iops, used_capacity (all floats)
        """
        result = {
            'read_throughput': 0.0,
            'write_throughput': 0.0,
            'read_iops': 0.0,
            'write_iops': 0.0,
            'used_capacity': 0,
        }
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        
        namespace = 'AWS/FSx'
        dimensions = [
            {'Name': 'FileSystemId', 'Value': fs_id},
            {'Name': 'VolumeId', 'Value': volume_id},
        ]
        
        queries = [
            {
                'Id': 'read_bytes',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataReadBytes',
                        'Dimensions': dimensions,
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'write_bytes',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataWriteBytes',
                        'Dimensions': dimensions,
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'read_ops',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataReadOperations',
                        'Dimensions': dimensions,
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'write_ops',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataWriteOperations',
                        'Dimensions': dimensions,
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'storage_used_ontap',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'StorageUsed',  # ONTAP uses StorageUsed
                        'Dimensions': dimensions,
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            },
            {
                'Id': 'storage_used_openzfs',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'UsedStorageCapacity',  # OpenZFS uses UsedStorageCapacity
                        'Dimensions': dimensions,
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            },
        ]
        
        try:
            response = self._client.get_metric_data(
                MetricDataQueries=queries,
                StartTime=start_time,
                EndTime=end_time,
            )
            
            for metric_result in response.get('MetricDataResults', []):
                values = metric_result.get('Values', [])
                if not values:
                    continue
                
                value = values[0]
                metric_id = metric_result.get('Id', '')
                
                if metric_id == 'read_bytes':
                    # Convert bytes per period to MiB/s
                    result['read_throughput'] = value / (1024 * 1024) / 60
                elif metric_id == 'write_bytes':
                    result['write_throughput'] = value / (1024 * 1024) / 60
                elif metric_id == 'read_ops':
                    # Convert ops per period to ops per second
                    result['read_iops'] = value / 60
                elif metric_id == 'write_ops':
                    result['write_iops'] = value / 60
                elif metric_id in ('storage_used_ontap', 'storage_used_openzfs'):
                    # Convert bytes to GiB (round to nearest integer)
                    result['used_capacity'] = round(value / (1024 * 1024 * 1024))
                    
        except Exception as e:
            logger.warning(f"Failed to get metrics for volume {volume_id}: {e}")
        
        return result
    
    def get_volume_metrics_batch(self, fs_id: str, volume_ids: List[str],
                                 volume_types: Optional[Dict[str, str]] = None
                                 ) -> Dict[str, Dict[str, float]]:
        """Get CloudWatch metrics for multiple volumes.

        Issues one or more GetMetricData calls (chunked to stay under
        CloudWatch's 500-queries-per-request limit). ONTAP volumes receive an
        additional set of queries (latency math expressions, capacity-pool
        IOPS, inode counts); OpenZFS volumes use the original 7-query set.

        Args:
            fs_id: The FSx file system ID (fs-xxx)
            volume_ids: List of volume IDs (fsvol-xxx)
            volume_types: Optional map of volume_id -> "ONTAP" | "OPENZFS". When
                omitted, all volumes are treated as OpenZFS (7-query path) to
                preserve prior behaviour.

        Returns:
            Dict mapping volume_id to metrics dict. OpenZFS entries populate
            read_throughput, write_throughput, read_iops, write_iops,
            used_capacity, storage_capacity. ONTAP entries additionally
            populate metadata_iops, capacity_pool_read_iops,
            capacity_pool_write_iops, files_used, files_capacity, and a
            nested 'latency' dict with read_ms, write_ms, metadata_ms.
        """
        volume_types = volume_types or {}

        def _empty(vol_id: str) -> Dict[str, Any]:
            entry: Dict[str, Any] = {
                'read_throughput': 0.0,
                'write_throughput': 0.0,
                'read_iops': 0.0,
                'write_iops': 0.0,
                'used_capacity': 0,
                'storage_capacity': 0,
            }
            if volume_types.get(vol_id) == 'ONTAP':
                entry.update({
                    'metadata_iops': 0.0,
                    'capacity_pool_read_iops': 0.0,
                    'capacity_pool_write_iops': 0.0,
                    'files_used': 0,
                    'files_capacity': 0,
                    'latency': {'read_ms': None, 'write_ms': None, 'metadata_ms': None},
                })
            return entry

        results: Dict[str, Dict[str, Any]] = {vol_id: _empty(vol_id) for vol_id in volume_ids}
        if not volume_ids:
            return results

        # Query budget: OpenZFS=7, ONTAP=18 (7 base + 8 extra metrics + 3 math).
        # Chunk volumes so each call stays under 500 queries.
        def _queries_for_volume(index: int, vol_id: str) -> List[Dict[str, Any]]:
            dimensions = [
                {'Name': 'FileSystemId', 'Value': fs_id},
                {'Name': 'VolumeId', 'Value': vol_id},
            ]
            namespace = 'AWS/FSx'
            # Base 7 metrics (existing behaviour).
            qs = [
                {'Id': f'rb_{index}', 'Label': f'{vol_id}|read_bytes',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'DataReadBytes', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Sum'}},
                {'Id': f'wb_{index}', 'Label': f'{vol_id}|write_bytes',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteBytes', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Sum'}},
                {'Id': f'ro_{index}', 'Label': f'{vol_id}|read_ops',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'DataReadOperations', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Sum'}},
                {'Id': f'wo_{index}', 'Label': f'{vol_id}|write_ops',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteOperations', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Sum'}},
                {'Id': f'su_{index}', 'Label': f'{vol_id}|storage_used',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'StorageUsed', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Average'}},
                {'Id': f'sc_{index}', 'Label': f'{vol_id}|storage_capacity',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'StorageCapacity', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Average'}},
                {'Id': f'usc_{index}', 'Label': f'{vol_id}|used_storage_openzfs',
                 'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'UsedStorageCapacity', 'Dimensions': dimensions},
                                'Period': 60, 'Stat': 'Average'}},
            ]
            # ONTAP-only extras: metadata ops, latency math, inodes, capacity pool.
            if volume_types.get(vol_id) == 'ONTAP':
                def _ms(qid: str, name: str, stat: str = 'Sum') -> Dict[str, Any]:
                    return {
                        'Id': qid,
                        'Label': f'{vol_id}|{qid.split("_", 1)[0]}',  # we set Label explicitly below where needed
                        'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': name, 'Dimensions': dimensions},
                                       'Period': 60, 'Stat': stat},
                        'ReturnData': False,
                    }
                # Denominator/numerator pairs for latency (ReturnData=False).
                # Labels aren't strictly needed on ReturnData=False queries, but
                # we keep them consistent for easier debugging.
                qs.extend([
                    _ms(f'mo_{index}', 'MetadataOperations'),
                    _ms(f'rot_{index}', 'DataReadOperationTime'),
                    _ms(f'wot_{index}', 'DataWriteOperationTime'),
                    _ms(f'mot_{index}', 'MetadataOperationTime'),
                ])
                # Metadata ops returned separately for total-IOPS & stored as rate.
                qs.append({
                    'Id': f'moi_{index}', 'Label': f'{vol_id}|metadata_ops',
                    'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'MetadataOperations', 'Dimensions': dimensions},
                                   'Period': 60, 'Stat': 'Sum'},
                })
                # Latency math expressions (ms/op), server-side division.
                qs.append({
                    'Id': f'lr_{index}', 'Label': f'{vol_id}|lat_read',
                    'Expression': f'(rot_{index} * 1000) / ro_{index}', 'Period': 60, 'ReturnData': True,
                })
                qs.append({
                    'Id': f'lw_{index}', 'Label': f'{vol_id}|lat_write',
                    'Expression': f'(wot_{index} * 1000) / wo_{index}', 'Period': 60, 'ReturnData': True,
                })
                qs.append({
                    'Id': f'lm_{index}', 'Label': f'{vol_id}|lat_meta',
                    'Expression': f'(mot_{index} * 1000) / mo_{index}', 'Period': 60, 'ReturnData': True,
                })
                # Capacity-pool tiering ops and inode counters.
                qs.extend([
                    {'Id': f'cpr_{index}', 'Label': f'{vol_id}|cp_read_ops',
                     'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'CapacityPoolReadOperations', 'Dimensions': dimensions},
                                    'Period': 60, 'Stat': 'Sum'}},
                    {'Id': f'cpw_{index}', 'Label': f'{vol_id}|cp_write_ops',
                     'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'CapacityPoolWriteOperations', 'Dimensions': dimensions},
                                    'Period': 60, 'Stat': 'Sum'}},
                    {'Id': f'fu_{index}', 'Label': f'{vol_id}|files_used',
                     'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'FilesUsed', 'Dimensions': dimensions},
                                    'Period': 60, 'Stat': 'Average'}},
                    {'Id': f'fc_{index}', 'Label': f'{vol_id}|files_capacity',
                     'MetricStat': {'Metric': {'Namespace': namespace, 'MetricName': 'FilesCapacity', 'Dimensions': dimensions},
                                    'Period': 60, 'Stat': 'Average'}},
                ])
            return qs

        # Build all queries and chunk into GetMetricData calls <=500 each.
        per_vol_queries: List[List[Dict[str, Any]]] = []
        for idx, vol_id in enumerate(volume_ids):
            per_vol_queries.append(_queries_for_volume(idx, vol_id))

        chunks: List[List[Dict[str, Any]]] = []
        current: List[Dict[str, Any]] = []
        for qs in per_vol_queries:
            if len(current) + len(qs) > 500 and current:
                chunks.append(current)
                current = []
            current.extend(qs)
        if current:
            chunks.append(current)

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)

        for chunk in chunks:
            try:
                response = self._client.get_metric_data(
                    MetricDataQueries=chunk,
                    StartTime=start_time,
                    EndTime=end_time,
                    ScanBy='TimestampDescending',
                )
            except Exception as e:
                logger.warning(f"Failed to get batch metrics for volumes: {e}")
                continue

            for metric_result in response.get('MetricDataResults', []):
                label = metric_result.get('Label', '')
                values = metric_result.get('Values', [])
                if not label or not values or '|' not in label:
                    continue
                vol_id, metric_type = label.split('|', 1)
                value = values[0]
                if vol_id not in results:
                    continue
                entry = results[vol_id]
                if metric_type == 'read_bytes':
                    entry['read_throughput'] = value / (1024 * 1024) / 60
                elif metric_type == 'write_bytes':
                    entry['write_throughput'] = value / (1024 * 1024) / 60
                elif metric_type == 'read_ops':
                    entry['read_iops'] = value / 60
                elif metric_type == 'write_ops':
                    entry['write_iops'] = value / 60
                elif metric_type in ('storage_used', 'used_storage_openzfs'):
                    used_gib = round(value / (1024 * 1024 * 1024))
                    if used_gib > 0 or entry['used_capacity'] == 0:
                        entry['used_capacity'] = used_gib
                elif metric_type == 'storage_capacity':
                    entry['storage_capacity'] = round(value / (1024 * 1024 * 1024))
                elif metric_type == 'metadata_ops':
                    entry['metadata_iops'] = value / 60
                elif metric_type == 'cp_read_ops':
                    entry['capacity_pool_read_iops'] = value / 60
                elif metric_type == 'cp_write_ops':
                    entry['capacity_pool_write_iops'] = value / 60
                elif metric_type == 'files_used':
                    entry['files_used'] = int(value)
                elif metric_type == 'files_capacity':
                    entry['files_capacity'] = int(value)
                elif metric_type in ('lat_read', 'lat_write', 'lat_meta'):
                    key = {'lat_read': 'read_ms', 'lat_write': 'write_ms', 'lat_meta': 'metadata_ms'}[metric_type]
                    entry.setdefault('latency', {'read_ms': None, 'write_ms': None, 'metadata_ms': None})
                    entry['latency'][key] = float(value)

        return results
    
    def get_lustre_mds_list(self, fs_id: str) -> List[str]:
        """Discover all MDS/MDT servers for a Lustre file system.
        
        Uses list_metrics to find all FileServer dimension values for the given
        file system ID.
        
        Args:
            fs_id: The FSx file system ID (fs-xxx)
            
        Returns:
            List of FileServer values (e.g., ["MDS0000", "MDS0001"])
        """
        mds_servers = []
        
        try:
            # Use list_metrics to discover FileServer dimension values
            list_response = self._client.list_metrics(
                Namespace='AWS/FSx',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'FileSystemId', 'Value': fs_id}]
            )
            
            for metric in list_response.get('Metrics', []):
                dims = {d['Name']: d['Value'] for d in metric['Dimensions']}
                file_server = dims.get('FileServer')
                if file_server and file_server not in mds_servers:
                    mds_servers.append(file_server)
            
            # Sort for consistent ordering
            mds_servers.sort()
            
        except Exception as e:
            logger.warning(f"Failed to get Lustre MDS list for file system {fs_id}: {e}")
        
        return mds_servers
    
    def get_lustre_mds_cpu(self, fs_id: str, mds_id: str) -> float:
        """Get CPU utilization for a specific Lustre MDS.
        
        Queries CPUUtilization with FileSystemId + FileServer dimensions.
        
        Args:
            fs_id: The FSx file system ID (fs-xxx)
            mds_id: The MDS server ID (e.g., "MDS0000")
            
        Returns:
            CPU utilization percentage (0-100), or 0.0 on error
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        
        try:
            response = self._client.get_metric_data(
                MetricDataQueries=[{
                    'Id': 'cpu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/FSx',
                            'MetricName': 'CPUUtilization',
                            'Dimensions': [
                                {'Name': 'FileSystemId', 'Value': fs_id},
                                {'Name': 'FileServer', 'Value': mds_id},
                            ],
                        },
                        'Period': 60,
                        'Stat': 'Average',
                    },
                }],
                StartTime=start_time,
                EndTime=end_time,
            )
            
            for result in response.get('MetricDataResults', []):
                values = result.get('Values', [])
                if values:
                    return values[0]
                    
        except Exception as e:
            logger.warning(f"Failed to get CPU for Lustre MDS {mds_id}: {e}")
        
        return 0.0
    
    def get_lustre_mds_cpu_batch(self, fs_id: str, mds_ids: List[str]) -> Dict[str, float]:
        """Get CPU utilization for multiple Lustre MDS servers in a single API call.
        
        This is more efficient than calling get_lustre_mds_cpu for each MDS.
        CloudWatch allows up to 500 metric queries per request.
        
        Args:
            fs_id: The FSx file system ID (fs-xxx)
            mds_ids: List of MDS server IDs (e.g., ["MDS0000", "MDS0001"])
            
        Returns:
            Dict mapping MDS ID to CPU utilization percentage (0-100)
        """
        result = {mds_id: 0.0 for mds_id in mds_ids}
        
        if not mds_ids:
            return result
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        
        # Build queries for all MDS servers (max 500 per request)
        queries = []
        for i, mds_id in enumerate(mds_ids[:500]):
            # Use sanitized ID for query (replace non-alphanumeric)
            safe_id = f"cpu_{i}"
            queries.append({
                'Id': safe_id,
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/FSx',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {'Name': 'FileSystemId', 'Value': fs_id},
                            {'Name': 'FileServer', 'Value': mds_id},
                        ],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
                'Label': mds_id,  # Store original MDS ID in label
            })
        
        try:
            response = self._client.get_metric_data(
                MetricDataQueries=queries,
                StartTime=start_time,
                EndTime=end_time,
            )
            
            for metric_result in response.get('MetricDataResults', []):
                label = metric_result.get('Label', '')
                values = metric_result.get('Values', [])
                if label and values:
                    result[label] = values[0]
                    
        except Exception as e:
            logger.warning(f"Failed to get batch CPU for Lustre MDS servers: {e}")
        
        return result

    def get_lustre_per_server_metrics(self, fs_id: str) -> Dict[str, Dict[str, Any]]:
        """Single GetMetricData call that returns per-MDS, per-OSS, and per-OST metrics.

        Uses SEARCH expressions so no client-side dimension discovery is needed.
        Each SEARCH returns one timeseries per matching dimension value and
        CloudWatch labels them with a rendered form that embeds the dimension
        value (e.g. ``{FileSystemId} MDS0000 CPUUtilization``). We pattern-match
        on the OSS/OST/MDS prefix to route the result to the right bucket.

        Returned shape::

            {
                'mds': {mds_id: cpu_util},
                'oss': {oss_id: {'network_throughput_util': .., 'disk_throughput_util': ..}},
                'ost': {ost_id: {'disk_iops_util': .., 'storage_capacity_util': ..}},
            }
        """
        result: Dict[str, Dict[str, Any]] = {'mds': {}, 'oss': {}, 'ost': {}}
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=10)

        # Custom label uses CloudWatch's `${PROP('Dim.FileServer')}` template
        # syntax so every returned timeseries carries an unambiguous
        # "<qid>|<dimension-value>" label.
        def search(qid: str, dim_schema: str, metric_name: str) -> Dict[str, Any]:
            return {
                'Id': qid,
                'Expression': (
                    f"SEARCH('{{AWS/FSx,FileSystemId,{dim_schema}}} "
                    f"MetricName=\"{metric_name}\" FileSystemId=\"{fs_id}\"', 'Average', 60)"
                ),
                'Period': 60,
                'Label': f"{qid}|${{PROP('Dim.{dim_schema}')}}",
                'ReturnData': True,
            }

        queries = [
            search('mds_cpu', 'FileServer', 'CPUUtilization'),
            search('oss_net', 'FileServer', 'NetworkThroughputUtilization'),
            search('oss_dtu', 'FileServer', 'FileServerDiskThroughputUtilization'),
            search('ost_iops', 'StorageTargetId', 'DiskIopsUtilization'),
            search('ost_cap', 'StorageTargetId', 'StorageCapacityUtilization'),
        ]

        try:
            response = self._client.get_metric_data(
                MetricDataQueries=queries,
                StartTime=start_time,
                EndTime=end_time,
                ScanBy='TimestampDescending',
            )
            for r in response.get('MetricDataResults', []):
                values = r.get('Values', [])
                if not values:
                    continue
                value = float(values[0])
                label = r.get('Label', '')
                if '|' not in label:
                    continue
                qid, dim_value = label.split('|', 1)
                dim_value = dim_value.strip()
                if not dim_value:
                    continue
                if qid == 'mds_cpu':
                    if dim_value.startswith('MDS'):
                        result['mds'][dim_value] = value
                elif qid in ('oss_net', 'oss_dtu'):
                    if not dim_value.startswith('OSS'):
                        continue
                    entry = result['oss'].setdefault(dim_value, {})
                    entry['network_throughput_util' if qid == 'oss_net' else 'disk_throughput_util'] = value
                elif qid in ('ost_iops', 'ost_cap'):
                    if not dim_value.startswith('OST'):
                        continue
                    entry = result['ost'].setdefault(dim_value, {})
                    entry['disk_iops_util' if qid == 'ost_iops' else 'storage_capacity_util'] = value
        except Exception as e:
            logger.warning(f"Failed to get Lustre per-server metrics for {fs_id}: {e}")

        return result
    
    def _build_perf_queries(self, fs_id: str, fs_type: FileSystemType
                            ) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """Return (queries, id->PerfMetrics-attr map) for perf utilization.

        Returned queries are safe to append to the standard FS-metrics
        ``GetMetricData`` request so both sets of data arrive in one call.
        """
        attr_map = {
            'net': 'network_throughput_util',
            'dtu': 'disk_throughput_util',
            'dtb': 'disk_throughput_burst_balance',
            'diu': 'disk_iops_util',
            'dib': 'disk_iops_burst_balance',
            'chr': 'cache_hit_ratio',
            'ssd': 'ssd_iops_util',
        }

        def _ms(qid: str, name: str, dims: List[Dict[str, str]], stat: str = 'Average',
                period: int = 60) -> Dict[str, Any]:
            return {
                'Id': qid,
                'MetricStat': {
                    'Metric': {'Namespace': 'AWS/FSx', 'MetricName': name, 'Dimensions': dims},
                    'Period': period,
                    'Stat': stat,
                },
                'ReturnData': True,
            }

        dim = [{'Name': 'FileSystemId', 'Value': fs_id}]
        queries: List[Dict[str, Any]] = []

        if fs_type in (FileSystemType.ONTAP, FileSystemType.OPENZFS, FileSystemType.WINDOWS):
            queries += [
                _ms('net', 'NetworkThroughputUtilization', dim),
                _ms('dtu', 'FileServerDiskThroughputUtilization', dim),
                _ms('dtb', 'FileServerDiskThroughputBalance', dim, stat='Minimum', period=300),
                _ms('diu', 'FileServerDiskIopsUtilization', dim),
                _ms('dib', 'FileServerDiskIopsBalance', dim, stat='Minimum', period=300),
                _ms('ssd', 'DiskIopsUtilization', dim),
            ]
            if fs_type in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
                queries.append(_ms('chr', 'FileServerCacheHitRatio', dim))
        elif fs_type == FileSystemType.LUSTRE:
            # Per-OSS/OST metrics averaged server-side via SEARCH — the
            # per-server breakdown is fetched separately by
            # get_lustre_per_server_metrics for display in the OSS/OST tables.
            queries += [
                {
                    'Id': 'net',
                    'Expression': (
                        f"AVG(SEARCH('{{AWS/FSx,FileSystemId,FileServer}} "
                        f"MetricName=\"NetworkThroughputUtilization\" FileSystemId=\"{fs_id}\"', 'Average', 60))"
                    ),
                    'Period': 60,
                },
                {
                    'Id': 'dtu',
                    'Expression': (
                        f"AVG(SEARCH('{{AWS/FSx,FileSystemId,FileServer}} "
                        f"MetricName=\"FileServerDiskThroughputUtilization\" FileSystemId=\"{fs_id}\"', 'Average', 60))"
                    ),
                    'Period': 60,
                },
                {
                    'Id': 'ssd',
                    'Expression': (
                        f"AVG(SEARCH('{{AWS/FSx,FileSystemId,StorageTargetId}} "
                        f"MetricName=\"DiskIopsUtilization\" FileSystemId=\"{fs_id}\"', 'Average', 60))"
                    ),
                    'Period': 60,
                },
            ]
        return queries, attr_map

    def _build_latency_queries(self, fs_id: str, fs_type: FileSystemType
                               ) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """Return (queries, id->LatencyMetrics-attr map) for average latency.

        Latency = OperationTime * 1000 / Operations (CloudWatch does the
        division server-side via a math expression). Lustre does not publish
        DataRead/Write/MetadataOperationTime — we emit no queries for it.
        Windows lacks MetadataOperationTime.
        """
        attr_map: Dict[str, str] = {}
        if fs_type == FileSystemType.LUSTRE:
            return [], attr_map

        dim = [{'Name': 'FileSystemId', 'Value': fs_id}]
        namespace = 'AWS/FSx'
        queries: List[Dict[str, Any]] = []

        def _add(pair_id: str, ops_name: str, time_name: str, attr: str) -> None:
            # Sum of operations and Sum of operation-time over Period=60;
            # dividing gives average ms per op for that minute. ReturnData
            # disabled on the inputs so only the derived value comes back.
            queries.append({
                'Id': f'{pair_id}_ops',
                'MetricStat': {
                    'Metric': {'Namespace': namespace, 'MetricName': ops_name, 'Dimensions': dim},
                    'Period': 60, 'Stat': 'Sum',
                },
                'ReturnData': False,
            })
            queries.append({
                'Id': f'{pair_id}_time',
                'MetricStat': {
                    'Metric': {'Namespace': namespace, 'MetricName': time_name, 'Dimensions': dim},
                    'Period': 60, 'Stat': 'Sum',
                },
                'ReturnData': False,
            })
            queries.append({
                'Id': pair_id,
                'Expression': f'({pair_id}_time * 1000) / {pair_id}_ops',
                'Period': 60,
                'ReturnData': True,
            })
            attr_map[pair_id] = attr

        _add('lat_read', 'DataReadOperations', 'DataReadOperationTime', 'read_ms')
        _add('lat_write', 'DataWriteOperations', 'DataWriteOperationTime', 'write_ms')
        # MetadataOperationTime is ONTAP + OpenZFS only (not Windows).
        if fs_type in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
            _add('lat_meta', 'MetadataOperations', 'MetadataOperationTime', 'metadata_ms')

        return queries, attr_map

    def get_performance_metrics(self, fs_id: str, fs_type: FileSystemType) -> PerfMetrics:
        """Fetch file-server performance utilization metrics.

        Thin wrapper around ``get_file_system_metrics``; the perf queries now
        share a single GetMetricData request with the core FS metrics.
        """
        metrics = self.get_file_system_metrics(fs_id, fs_type)
        return metrics.perf_metrics or PerfMetrics()

    def _build_metric_queries(self, fs_id: str, fs_type: FileSystemType) -> List[Dict[str, Any]]:
        """Build CloudWatch metric queries based on file system type."""
        namespace = 'AWS/FSx'
        dimension = {'Name': 'FileSystemId', 'Value': fs_id}
        
        queries = [
            {
                'Id': 'read_bytes',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataReadBytes',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'write_bytes',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataWriteBytes',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'read_ops',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataReadOperations',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
            {
                'Id': 'write_ops',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'DataWriteOperations',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Sum',
                },
            },
        ]
        
        # Add capacity metric based on type
        if fs_type == FileSystemType.LUSTRE:
            queries.append({
                'Id': 'free_capacity',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'FreeDataStorageCapacity',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Sum',  # Sum across all OSTs
                },
            })
            # Lustre CPU is fetched separately due to FileServer dimension requirement
        elif fs_type == FileSystemType.WINDOWS:
            queries.append({
                'Id': 'free_capacity',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'FreeStorageCapacity',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
            # Windows has CPU utilization metric
            queries.append({
                'Id': 'cpu_util',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
        elif fs_type == FileSystemType.ONTAP:
            queries.append({
                'Id': 'used_capacity',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'StorageUsed',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
            # Capacity pool usage for pricing
            queries.append({
                'Id': 'capacity_pool_used',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'StorageUsed',
                        'Dimensions': [
                            dimension,
                            {'Name': 'StorageTier', 'Value': 'StandardCapacityPool'},
                            {'Name': 'DataType', 'Value': 'All'},
                        ],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
            # ONTAP has CPU utilization metric
            queries.append({
                'Id': 'cpu_util',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
        elif fs_type == FileSystemType.OPENZFS:
            queries.append({
                'Id': 'used_capacity',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'UsedStorageCapacity',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
            # OpenZFS has CPU utilization metric
            queries.append({
                'Id': 'cpu_util',
                'MetricStat': {
                    'Metric': {
                        'Namespace': namespace,
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [dimension],
                    },
                    'Period': 60,
                    'Stat': 'Average',
                },
            })
        
        return queries


class StaticPricingProvider:
    """Pricing provider using external JSON pricing data.
    
    Loads pricing from pricing_data.json for the specified region.
    Returns PricingBreakdown with itemized monthly costs.
    """
    
    def __init__(self, region: str):
        self._region = region
        self._data = self._load_pricing_data(region)
    
    def _load_pricing_data(self, region: str) -> dict:
        """Load pricing JSON and extract region-specific data."""
        import json
        from pathlib import Path
        try:
            pricing_path = Path(__file__).parent / 'pricing_data.json'
            with open(pricing_path) as f:
                all_data = json.load(f)
            return all_data.get('regions', {}).get(region, {})
        except Exception as e:
            logger.warning(f"Failed to load pricing data: {e}")
            return {}
    
    def file_system_price(self, fs: FileSystem) -> Optional[PricingBreakdown]:
        """Calculate itemized monthly pricing breakdown.
        
        Returns:
            PricingBreakdown with monthly costs, or None if pricing unavailable
        """
        if not self._data:
            return None
        
        if fs.type == FileSystemType.ONTAP:
            return self._calculate_ontap_price(fs)
        elif fs.type == FileSystemType.OPENZFS:
            return self._calculate_openzfs_price(fs)
        elif fs.type == FileSystemType.WINDOWS:
            return self._calculate_windows_price(fs)
        elif fs.type == FileSystemType.LUSTRE:
            return self._calculate_lustre_price(fs)
        
        return None
    
    def _calculate_ontap_price(self, fs: FileSystem) -> PricingBreakdown:
        """Calculate monthly pricing for ONTAP file system."""
        prices = self._data.get('ONTAP', {})
        deployment = fs.deployment_type or 'SINGLE_AZ_1'
        breakdown = PricingBreakdown()
        
        # SSD storage (provisioned)
        breakdown.storage = fs.storage_capacity * prices.get('storage', {}).get(deployment, 0)
        
        # Capacity pool (usage-based from CloudWatch)
        if fs.capacity_pool_used_gb and fs.capacity_pool_used_gb > 0:
            cp_price = prices.get('capacity_pool', {}).get(deployment, 0)
            breakdown.capacity_pool = fs.capacity_pool_used_gb * cp_price
        
        # Throughput
        breakdown.throughput = fs.throughput_capacity * prices.get('throughput', {}).get(deployment, 0)
        
        # IOPS (above baseline)
        baseline_per_gb = prices.get('iops_baseline_per_gb', 3)
        baseline_iops = fs.storage_capacity * baseline_per_gb
        if fs.provisioned_iops > baseline_iops:
            extra = fs.provisioned_iops - baseline_iops
            breakdown.iops = extra * prices.get('iops', {}).get(deployment, 0)
        
        return breakdown
    
    def _calculate_openzfs_price(self, fs: FileSystem) -> Optional[PricingBreakdown]:
        """Calculate monthly pricing for OpenZFS file system."""
        prices = self._data.get('OPENZFS', {})
        deployment = fs.deployment_type or 'SINGLE_AZ_1'
        
        # Intelligent-Tiering: N/A
        storage_price = prices.get('storage', {}).get(deployment)
        if storage_price is None:
            return None
        
        breakdown = PricingBreakdown()
        breakdown.storage = fs.storage_capacity * storage_price
        breakdown.throughput = fs.throughput_capacity * prices.get('throughput', {}).get(deployment, 0)
        
        # IOPS (above baseline)
        baseline_per_gb = prices.get('iops_baseline_per_gb', 3)
        baseline_iops = fs.storage_capacity * baseline_per_gb
        if fs.provisioned_iops > baseline_iops:
            extra = fs.provisioned_iops - baseline_iops
            breakdown.iops = extra * prices.get('iops', {}).get(deployment, 0)
        
        return breakdown
    
    def _calculate_windows_price(self, fs: FileSystem) -> PricingBreakdown:
        """Calculate monthly pricing for Windows File Server file system."""
        prices = self._data.get('WINDOWS', {})
        deployment = fs.deployment_type or 'SINGLE_AZ_1'
        storage_type = fs.storage_type or 'SSD'
        breakdown = PricingBreakdown()
        
        # Storage (SSD or HDD)
        storage_prices = prices.get('storage', {}).get(deployment, {})
        breakdown.storage = fs.storage_capacity * storage_prices.get(storage_type, 0)
        
        # Throughput
        breakdown.throughput = fs.throughput_capacity * prices.get('throughput', {}).get(deployment, 0)
        
        # IOPS (SSD only, above baseline)
        if storage_type == 'SSD' and fs.provisioned_iops > 0:
            baseline_per_gb = prices.get('iops_baseline_per_gb', 3)
            baseline_iops = fs.storage_capacity * baseline_per_gb
            if fs.provisioned_iops > baseline_iops:
                extra = fs.provisioned_iops - baseline_iops
                breakdown.iops = extra * prices.get('iops', {}).get(deployment, 0)
        
        return breakdown
    
    def _calculate_lustre_price(self, fs: FileSystem) -> Optional[PricingBreakdown]:
        """Calculate monthly pricing for Lustre file system."""
        prices = self._data.get('LUSTRE', {})
        deployment = fs.deployment_type or 'SCRATCH_2'
        breakdown = PricingBreakdown()
        
        # Storage price depends on deployment type and throughput tier
        storage_price = self._lustre_storage_price(fs, prices, deployment)
        if storage_price is None:
            return None  # Intelligent-Tiering or unknown config
        
        breakdown.storage = fs.storage_capacity * storage_price
        
        # Metadata IOPS (above baseline)
        baseline_per_gb = prices.get('metadata_iops_baseline_per_gb', 1.25)
        baseline_iops = fs.storage_capacity * baseline_per_gb
        if fs.provisioned_iops > baseline_iops:
            extra = fs.provisioned_iops - baseline_iops
            breakdown.iops = extra * prices.get('metadata_iops', 0.066)
        
        return breakdown
    
    def _lustre_storage_price(self, fs: FileSystem, prices: dict, deployment: str) -> Optional[float]:
        """Look up Lustre storage price by deployment type and throughput tier."""
        storage = prices.get('storage', {})
        throughput_per_tib = fs.throughput_capacity or 200
        
        if deployment in ('SCRATCH_1', 'SCRATCH_2'):
            return storage.get(deployment, 0.14)
        
        deploy_prices = storage.get(deployment, {})
        if not deploy_prices:
            return None
        
        # For PERSISTENT types, look up by storage type and throughput tier
        storage_type = fs.storage_type or 'SSD'
        tier_prices = deploy_prices.get(storage_type, {})
        if isinstance(tier_prices, dict):
            return tier_prices.get(str(throughput_per_tib), tier_prices.get(str(min(int(k) for k in tier_prices.keys()))) if tier_prices else None)
        return tier_prices if isinstance(tier_prices, (int, float)) else None
