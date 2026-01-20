"""AWS client wrappers for FSx, CloudWatch, and Pricing."""

import boto3
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Tuple

from .model import FileSystem, FileSystemType, Metrics, Volume, MetadataServer


def create_session(region: str, profile: Optional[str] = None) -> boto3.Session:
    """Create a shared boto3 session for all clients."""
    return boto3.Session(profile_name=profile, region_name=region)


class FSxClient:
    """Wrapper for AWS FSx API."""
    
    def __init__(self, region: str, profile: Optional[str] = None, session: Optional[boto3.Session] = None):
        if session is None:
            session = boto3.Session(profile_name=profile, region_name=region)
        self._client = session.client('fsx')
    
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
        except Exception:
            pass
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
        except Exception:
            pass  # Return empty list on error
        
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


class CloudWatchClient:
    """Wrapper for AWS CloudWatch API."""
    
    def __init__(self, region: str, profile: Optional[str] = None, session: Optional[boto3.Session] = None):
        if session is None:
            session = boto3.Session(profile_name=profile, region_name=region)
        self._client = session.client('cloudwatch')
    
    def get_file_system_metrics(self, fs_id: str, fs_type: FileSystemType) -> Metrics:
        """Retrieve CloudWatch metrics for a specific file system."""
        metrics = Metrics()
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        
        # Define metrics based on file system type
        metric_queries = self._build_metric_queries(fs_id, fs_type)
        
        if not metric_queries:
            return metrics
        
        try:
            response = self._client.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time,
            )
            
            for result in response.get('MetricDataResults', []):
                values = result.get('Values', [])
                if not values:
                    continue
                
                value = values[0]
                metric_id = result.get('Id', '')
                
                if metric_id == 'read_bytes':
                    # CloudWatch returns bytes per period, convert to MB/s
                    metrics.read_throughput = value / (1024 * 1024) / 60  # bytes -> MB/s
                elif metric_id == 'write_bytes':
                    metrics.write_throughput = value / (1024 * 1024) / 60  # bytes -> MB/s
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
            
            # For Lustre, fetch CPU separately with FileServer dimension
            if fs_type == FileSystemType.LUSTRE and metrics.cpu_utilization == 0:
                cpu = self._get_lustre_cpu(fs_id, start_time, end_time)
                if cpu is not None:
                    metrics.cpu_utilization = cpu
                    
        except Exception:
            pass  # Return empty metrics on error
        
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
        except Exception:
            pass
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
                        'Period': 60, 'Stat': 'Average',
                    },
                    'Label': f'{fs_id}|read_bytes',
                },
                {
                    'Id': f'wb_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteBytes', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Average',
                    },
                    'Label': f'{fs_id}|write_bytes',
                },
                {
                    'Id': f'ro_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataReadOperations', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Average',
                    },
                    'Label': f'{fs_id}|read_ops',
                },
                {
                    'Id': f'wo_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteOperations', 'Dimensions': dimension},
                        'Period': 60, 'Stat': 'Average',
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
                    
        except Exception:
            pass  # Return empty metrics on error
        
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
                    # Convert bytes per period to MB/s
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
                    
        except Exception:
            pass  # Return zeros on error
        
        return result
    
    def get_volume_metrics_batch(self, fs_id: str, volume_ids: List[str]) -> Dict[str, Dict[str, float]]:
        """Get CloudWatch metrics for multiple volumes in a single API call.
        
        This is more efficient than calling get_volume_metrics for each volume.
        CloudWatch allows up to 500 metric queries per request.
        
        Args:
            fs_id: The FSx file system ID (fs-xxx)
            volume_ids: List of volume IDs (fsvol-xxx)
            
        Returns:
            Dict mapping volume_id to metrics dict with keys:
            read_throughput, write_throughput, read_iops, write_iops, used_capacity, storage_capacity
        """
        # Initialize results with zeros
        results = {
            vol_id: {
                'read_throughput': 0.0,
                'write_throughput': 0.0,
                'read_iops': 0.0,
                'write_iops': 0.0,
                'used_capacity': 0,
                'storage_capacity': 0,  # From CloudWatch for ONTAP
            }
            for vol_id in volume_ids
        }
        
        if not volume_ids:
            return results
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        namespace = 'AWS/FSx'
        
        # Build queries for all volumes (7 metrics per volume, max 500 total)
        # ONTAP: StorageUsed + StorageCapacity (with StorageTier dimension)
        # OpenZFS: UsedStorageCapacity
        queries = []
        max_volumes = min(len(volume_ids), 71)  # 7 metrics * 71 = 497 queries max
        
        for i, vol_id in enumerate(volume_ids[:max_volumes]):
            dimensions = [
                {'Name': 'FileSystemId', 'Value': fs_id},
                {'Name': 'VolumeId', 'Value': vol_id},
            ]
            
            queries.extend([
                {
                    'Id': f'rb_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataReadBytes', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{vol_id}|read_bytes',
                },
                {
                    'Id': f'wb_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteBytes', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{vol_id}|write_bytes',
                },
                {
                    'Id': f'ro_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataReadOperations', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{vol_id}|read_ops',
                },
                {
                    'Id': f'wo_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'DataWriteOperations', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Sum',
                    },
                    'Label': f'{vol_id}|write_ops',
                },
                {
                    'Id': f'su_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'StorageUsed', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Average',
                    },
                    'Label': f'{vol_id}|storage_used',
                },
                {
                    'Id': f'sc_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'StorageCapacity', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Average',
                    },
                    'Label': f'{vol_id}|storage_capacity',
                },
                {
                    'Id': f'usc_{i}',
                    'MetricStat': {
                        'Metric': {'Namespace': namespace, 'MetricName': 'UsedStorageCapacity', 'Dimensions': dimensions},
                        'Period': 60, 'Stat': 'Average',
                    },
                    'Label': f'{vol_id}|used_storage_openzfs',
                },
            ])
        
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
                
                vol_id, metric_type = label.split('|', 1)
                value = values[0]
                
                if vol_id not in results:
                    continue
                
                if metric_type == 'read_bytes':
                    results[vol_id]['read_throughput'] = value / (1024 * 1024) / 60
                elif metric_type == 'write_bytes':
                    results[vol_id]['write_throughput'] = value / (1024 * 1024) / 60
                elif metric_type == 'read_ops':
                    results[vol_id]['read_iops'] = value / 60
                elif metric_type == 'write_ops':
                    results[vol_id]['write_iops'] = value / 60
                elif metric_type in ('storage_used', 'used_storage_openzfs'):
                    # Used capacity in bytes -> GiB
                    used_gib = round(value / (1024 * 1024 * 1024))
                    if used_gib > 0 or results[vol_id]['used_capacity'] == 0:
                        results[vol_id]['used_capacity'] = used_gib
                elif metric_type == 'storage_capacity':
                    # Volume capacity from CloudWatch (ONTAP StorageCapacity metric)
                    capacity_gib = round(value / (1024 * 1024 * 1024))
                    results[vol_id]['storage_capacity'] = capacity_gib
                    
        except Exception:
            pass  # Return zeros on error
        
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
            
        except Exception:
            pass  # Return empty list on error
        
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
                    
        except Exception:
            pass  # Return 0.0 on error
        
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
                    
        except Exception:
            pass  # Return zeros on error
        
        return result
    
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
                    'Stat': 'Average',
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
                    'Stat': 'Average',
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
                    'Stat': 'Average',
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
                    'Stat': 'Average',
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
    """Pricing provider with embedded AWS pricing data for FSx file systems.
    
    Prices are based on AWS Price List API data for us-east-1 region.
    Pricing components:
    - Storage: $/GB-Month for SSD or HDD
    - Throughput: $/MBps-Month for provisioned throughput
    - IOPS: $/IOPS-Month for provisioned IOPS
    
    Monthly prices are converted to hourly (divide by 730 hours/month).
    """
    
    # FSx for ONTAP pricing (us-east-1)
    ONTAP_PRICES = {
        'storage': {
            'SINGLE_AZ_1': {'SSD': 0.125},      # $/GB-Mo
            'SINGLE_AZ_2': {'SSD': 0.125},
            'MULTI_AZ_1': {'SSD': 0.250},
            'MULTI_AZ_2': {'SSD': 0.250},
        },
        'throughput': {
            'SINGLE_AZ_1': 0.72,   # $/MBps-Mo
            'SINGLE_AZ_2': 1.60,
            'MULTI_AZ_1': 1.20,
            'MULTI_AZ_2': 2.50,
        },
        'iops': {
            'SINGLE_AZ_1': 0.017,  # $/IOPS-Mo
            'SINGLE_AZ_2': 0.017,
            'MULTI_AZ_1': 0.034,
            'MULTI_AZ_2': 0.034,
        },
        'capacity_pool': 0.0219,  # $/GB-Mo for capacity pool (Single-AZ)
    }
    
    # FSx for OpenZFS pricing (us-east-1)
    OPENZFS_PRICES = {
        'storage': {
            'SINGLE_AZ_1': {'SSD': 0.09},
            'SINGLE_AZ_2': {'SSD': 0.09},
            'SINGLE_AZ_HA_1': {'SSD': 0.09},
            'SINGLE_AZ_HA_2': {'SSD': 0.09},
            'MULTI_AZ_1': {'SSD': 0.18},
        },
        'throughput': {
            'SINGLE_AZ_1': 0.26,
            'SINGLE_AZ_2': 0.26,
            'SINGLE_AZ_HA_1': 0.52,
            'SINGLE_AZ_HA_2': 0.52,
            'MULTI_AZ_1': 0.87,
        },
        'iops': {
            'SINGLE_AZ_1': 0.006,
            'SINGLE_AZ_2': 0.006,
            'SINGLE_AZ_HA_1': 0.012,
            'SINGLE_AZ_HA_2': 0.012,
            'MULTI_AZ_1': 0.024,
        },
    }
    
    # FSx for Windows File Server pricing (us-east-1)
    WINDOWS_PRICES = {
        'storage': {
            'SINGLE_AZ_1': {'SSD': 0.13, 'HDD': 0.013},
            'SINGLE_AZ_2': {'SSD': 0.13, 'HDD': 0.013},
            'MULTI_AZ_1': {'SSD': 0.23, 'HDD': 0.025},
        },
        'throughput': {
            'SINGLE_AZ_1': 2.20,
            'SINGLE_AZ_2': 2.20,
            'MULTI_AZ_1': 4.50,
        },
        'iops': {
            'SINGLE_AZ_1': 0.012,
            'SINGLE_AZ_2': 0.012,
            'MULTI_AZ_1': 0.024,
        },
    }
    
    # FSx for Lustre pricing (us-east-1)
    # Lustre pricing is based on storage type and throughput tier
    LUSTRE_PRICES = {
        'storage': {
            # SSD tiers by throughput (MB/s per TiB)
            'SCRATCH_1': {'SSD': 0.14},
            'SCRATCH_2': {'SSD': 0.14},
            'PERSISTENT_1': {
                'SSD': {
                    50: 0.14,    # 50 MB/s per TiB
                    100: 0.19,   # 100 MB/s per TiB
                    200: 0.29,   # 200 MB/s per TiB
                },
            },
            'PERSISTENT_2': {
                'SSD': {
                    125: 0.145,  # 125 MB/s per TiB
                    250: 0.21,   # 250 MB/s per TiB
                    500: 0.34,   # 500 MB/s per TiB
                    1000: 0.60,  # 1000 MB/s per TiB
                },
            },
        },
        'hdd': {
            12: 0.025,   # 12 MB/s per TiB
            40: 0.083,   # 40 MB/s per TiB
        },
        'metadata_iops': 0.055,  # $/IOPS-Mo for metadata IOPS
        'throughput': 0.52,      # $/MBps-Mo for Intelligent-Tiering
    }
    
    # Hours per month for conversion
    HOURS_PER_MONTH = 730
    
    def __init__(self, region: str):
        self._region = region
    
    def file_system_price(self, fs: FileSystem) -> Optional[float]:
        """Calculate the hourly price for a file system based on its configuration.
        
        Returns:
            Hourly price in USD, or None if pricing cannot be calculated
        """
        if fs.type == FileSystemType.ONTAP:
            return self._calculate_ontap_price(fs)
        elif fs.type == FileSystemType.OPENZFS:
            return self._calculate_openzfs_price(fs)
        elif fs.type == FileSystemType.WINDOWS:
            return self._calculate_windows_price(fs)
        elif fs.type == FileSystemType.LUSTRE:
            return self._calculate_lustre_price(fs)
        
        return None
    
    def _calculate_ontap_price(self, fs: FileSystem) -> float:
        """Calculate hourly price for ONTAP file system."""
        monthly_cost = 0.0
        deployment = fs.deployment_type or 'SINGLE_AZ_1'
        
        # Storage cost
        storage_prices = self.ONTAP_PRICES['storage'].get(deployment, {'SSD': 0.125})
        storage_price = storage_prices.get('SSD', 0.125)
        monthly_cost += fs.storage_capacity * storage_price
        
        # Throughput cost
        throughput_price = self.ONTAP_PRICES['throughput'].get(deployment, 0.72)
        monthly_cost += fs.throughput_capacity * throughput_price
        
        # IOPS cost (only if provisioned above baseline)
        # ONTAP baseline is 3 IOPS per GB, charged for IOPS above that
        baseline_iops = fs.storage_capacity * 3
        if fs.provisioned_iops > baseline_iops:
            iops_price = self.ONTAP_PRICES['iops'].get(deployment, 0.017)
            extra_iops = fs.provisioned_iops - baseline_iops
            monthly_cost += extra_iops * iops_price
        
        return monthly_cost / self.HOURS_PER_MONTH
    
    def _calculate_openzfs_price(self, fs: FileSystem) -> float:
        """Calculate hourly price for OpenZFS file system."""
        monthly_cost = 0.0
        deployment = fs.deployment_type or 'SINGLE_AZ_1'
        
        # Storage cost
        storage_prices = self.OPENZFS_PRICES['storage'].get(deployment, {'SSD': 0.09})
        storage_price = storage_prices.get('SSD', 0.09)
        monthly_cost += fs.storage_capacity * storage_price
        
        # Throughput cost
        throughput_price = self.OPENZFS_PRICES['throughput'].get(deployment, 0.26)
        monthly_cost += fs.throughput_capacity * throughput_price
        
        # IOPS cost (only if provisioned above baseline)
        # OpenZFS baseline is 3 IOPS per GB, charged for IOPS above that
        baseline_iops = fs.storage_capacity * 3
        if fs.provisioned_iops > baseline_iops:
            iops_price = self.OPENZFS_PRICES['iops'].get(deployment, 0.006)
            extra_iops = fs.provisioned_iops - baseline_iops
            monthly_cost += extra_iops * iops_price
        
        return monthly_cost / self.HOURS_PER_MONTH
    
    def _calculate_windows_price(self, fs: FileSystem) -> float:
        """Calculate hourly price for Windows File Server file system."""
        monthly_cost = 0.0
        deployment = fs.deployment_type or 'SINGLE_AZ_1'
        storage_type = fs.storage_type or 'SSD'
        
        # Storage cost
        storage_prices = self.WINDOWS_PRICES['storage'].get(deployment, {'SSD': 0.13, 'HDD': 0.013})
        storage_price = storage_prices.get(storage_type, 0.13)
        monthly_cost += fs.storage_capacity * storage_price
        
        # Throughput cost
        throughput_price = self.WINDOWS_PRICES['throughput'].get(deployment, 2.20)
        monthly_cost += fs.throughput_capacity * throughput_price
        
        # IOPS cost (SSD only, above baseline)
        # Windows SSD baseline is 3 IOPS per GB
        if storage_type == 'SSD' and fs.provisioned_iops > 0:
            baseline_iops = fs.storage_capacity * 3
            if fs.provisioned_iops > baseline_iops:
                iops_price = self.WINDOWS_PRICES['iops'].get(deployment, 0.012)
                extra_iops = fs.provisioned_iops - baseline_iops
                monthly_cost += extra_iops * iops_price
        
        return monthly_cost / self.HOURS_PER_MONTH
    
    def _calculate_lustre_price(self, fs: FileSystem) -> float:
        """Calculate hourly price for Lustre file system."""
        monthly_cost = 0.0
        deployment = fs.deployment_type or 'SCRATCH_2'
        throughput_per_tib = fs.throughput_capacity or 200
        
        # Determine storage price based on deployment type and throughput
        if deployment in ('SCRATCH_1', 'SCRATCH_2'):
            storage_price = 0.14  # Scratch storage
        elif deployment == 'PERSISTENT_1':
            # PERSISTENT_1 SSD pricing by throughput tier
            ssd_prices = self.LUSTRE_PRICES['storage']['PERSISTENT_1'].get('SSD', {})
            storage_price = ssd_prices.get(throughput_per_tib, 0.19)
        elif deployment == 'PERSISTENT_2':
            # PERSISTENT_2 SSD pricing by throughput tier
            ssd_prices = self.LUSTRE_PRICES['storage']['PERSISTENT_2'].get('SSD', {})
            storage_price = ssd_prices.get(throughput_per_tib, 0.145)
        elif 'HDD' in deployment or fs.storage_type == 'HDD':
            # HDD pricing by throughput tier
            storage_price = self.LUSTRE_PRICES['hdd'].get(throughput_per_tib, 0.025)
        else:
            storage_price = 0.14  # Default to scratch pricing
        
        monthly_cost += fs.storage_capacity * storage_price
        
        # Metadata IOPS cost (if provisioned)
        if fs.provisioned_iops > 0:
            monthly_cost += fs.provisioned_iops * self.LUSTRE_PRICES['metadata_iops']
        
        return monthly_cost / self.HOURS_PER_MONTH
