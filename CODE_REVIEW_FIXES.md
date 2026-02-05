# Code Review Fixes - Summary

All Bandit and Semgrep findings have been addressed.

## Changes Made

### 1. Logging Infrastructure Added

**Files Modified:**
- `fsx_viewer/__main__.py` - Added logging setup with `FSX_VIEWER_DEBUG` environment variable support
- `fsx_viewer/cli.py` - Added logger
- `fsx_viewer/aws_client.py` - Added logger
- `fsx_viewer/controller.py` - Added logger

**Logging Configuration:**
- Default level: `WARNING` (shows warnings and errors only)
- Debug mode: Set `FSX_VIEWER_DEBUG=1` environment variable for `DEBUG` level
- Format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### 2. Fixed: Try-Except-Pass Blocks (18 occurrences)

All silent error suppression has been replaced with logged warnings/errors:

**cli.py (1 fix):**
- Line 46: Config file parsing now logs warning when file is malformed

**aws_client.py (10 fixes):**
- Line 55: `describe_file_system` - logs warning with file system ID
- Line 178: `describe_volumes` - logs warning with file system ID
- Line 274: `get_file_system_metrics` - logs warning with file system ID
- Line 327: `_get_lustre_cpu` - logs warning with file system ID
- Line 514: `get_file_system_metrics_batch` - logs warning
- Line 656: `get_volume_metrics` - logs warning with volume ID
- Line 804: `get_volume_metrics_batch` - logs warning
- Line 840: `get_lustre_mds_list` - logs warning with file system ID
- Line 886: `get_lustre_mds_cpu` - logs warning with MDS ID
- Line 947: `get_lustre_mds_cpu_batch` - logs warning

**controller.py (7 fixes):**
- Line 135: `refresh_file_systems` - logs error
- Line 176: `refresh_metrics` - logs warning
- Line 190: `_fetch_lustre_cpu_batch` inner function - logs warning with file system ID
- Line 348: `_fetch_file_system` - logs warning with file system ID
- Line 411: `_refresh_file_system_metrics` - logs warning
- Line 427: `refresh_volumes` - logs warning
- Line 498: `refresh_mds_metrics` - logs warning

### 3. Fixed: Future.result() Calls (3 occurrences)

All `for future in as_completed(futures): pass` loops now properly handle exceptions:

**controller.py:**
- Line 195: `_fetch_lustre_cpu_batch` - calls `future.result()` with exception logging
- Line 318: `_initial_fetch_async` - calls `future.result()` with exception logging
- Line 392: `_poll_metrics` - calls `future.result()` with exception logging

**Pattern used:**
```python
for future in as_completed(futures):
    try:
        future.result()
    except Exception as e:
        logger.warning(f"Task failed: {e}")
```

### 4. Fixed: Missing Encoding Parameter (1 occurrence)

**cli.py:**
- Line 36: Added `encoding="utf-8"` to `open()` call for cross-platform compatibility

### 5. Fixed: Useless If Statement (1 occurrence)

**ui.py:**
- Line 160: Removed redundant `elif field == "creation"` branch (both branches returned same value)

## Testing

All modified files pass Python syntax validation:
```bash
uv run python -m py_compile fsx_viewer/__main__.py fsx_viewer/cli.py fsx_viewer/controller.py fsx_viewer/aws_client.py fsx_viewer/ui.py
```

## Usage

### Normal Operation
```bash
fsx-viewer --region us-east-1
```
Only warnings and errors will be logged (to stderr).

### Debug Mode
```bash
FSX_VIEWER_DEBUG=1 fsx-viewer --region us-east-1
```
All debug messages, warnings, and errors will be logged.

## Impact

- **No breaking changes** - All functionality remains the same
- **Better observability** - Errors are now visible for troubleshooting
- **Graceful degradation maintained** - Application still continues on errors (doesn't crash)
- **Thread safety preserved** - All logging is thread-safe by default in Python
- **Performance impact** - Minimal (logging only occurs on errors)

## Security Improvements

- File operations now use explicit UTF-8 encoding (prevents platform-specific issues)
- Thread exceptions are now visible (prevents silent failures)
- AWS API errors are logged (helps identify permission/throttling issues)
