"""Terminal UI using Rich library."""

import math
import sys
import threading
from typing import Optional, List, Callable

from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from .model import Store, FileSystem, Stats, FileSystemType, DetailStore, Volume, MetadataServer, ObjectStorageServer, ObjectStorageTarget, MetadataTarget, LatencyMetrics


def _has_vt_support() -> bool:
    """Check if the terminal supports ANSI/VT escape sequences."""
    if sys.platform != 'win32':
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_ulong()
        kernel32.GetConsoleMode(handle, ctypes.byref(mode))
        return bool(mode.value & 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    except Exception:
        return False


_VT_OK = _has_vt_support()


def _clear_screen():
    """Clear the terminal screen using the best available method."""
    if _VT_OK:
        sys.stdout.write('\033[H\033[2J')
        sys.stdout.flush()
    elif sys.platform == 'win32':
        import os; os.system('cls')

def interpolate_color(position: float) -> str:
    """Interpolate RGB color for smooth gradient from green -> yellow -> orange -> red.
    
    Uses multiple color stops for a smoother, more visually appealing gradient.
    
    Args:
        position: Value from 0.0 to 1.0 representing position in gradient
        
    Returns:
        Hex color string like '#rrggbb'
    """
    # Clamp position to valid range
    position = max(0.0, min(1.0, position))
    
    # Define color stops for smoother gradient:
    # 0.0  -> Green:       (0, 180, 0)
    # 0.35 -> Yellow-Green: (140, 180, 0)
    # 0.5  -> Yellow:      (220, 180, 0)
    # 0.7  -> Orange:      (235, 120, 0)
    # 1.0  -> Red:         (220, 40, 0)
    
    if position <= 0.35:
        # Green to Yellow-Green (0.0 -> 0.35)
        t = position / 0.35
        r = int(0 + t * 140)
        g = 180
        b = 0
    elif position <= 0.5:
        # Yellow-Green to Yellow (0.35 -> 0.5)
        t = (position - 0.35) / 0.15
        r = int(140 + t * 80)
        g = 180
        b = 0
    elif position <= 0.7:
        # Yellow to Orange (0.5 -> 0.7)
        t = (position - 0.5) / 0.2
        r = int(220 + t * 15)
        g = int(180 - t * 60)
        b = 0
    else:
        # Orange to Red (0.7 -> 1.0)
        t = (position - 0.7) / 0.3
        r = int(235 - t * 15)
        g = int(120 - t * 80)
        b = 0
    
    # Clamp RGB values
    r = max(0, min(255, r))
    g = max(0, min(255, g))
    b = max(0, min(255, b))
    
    return f"#{r:02x}{g:02x}{b:02x}"


def make_volume_sorter(sort_spec: str):
    """Create a sort key function for volumes.
    
    Format: field=order (e.g., 'capacity=dsc', 'name=asc')
    Supported fields: name, capacity, utilization, iops, throughput
    """
    if not sort_spec:
        sort_spec = "name=asc"
    
    parts = sort_spec.split("=")
    field = parts[0].lower()
    order = parts[1].lower() if len(parts) > 1 else "asc"
    reverse = order == "dsc"
    
    def get_key(vol: Volume):
        if field == "name":
            return vol.name.lower()
        elif field == "capacity":
            return vol.storage_capacity
        elif field == "utilization":
            return vol.utilization()
        elif field == "iops":
            return vol.total_iops()
        elif field == "throughput":
            return vol.total_throughput()
        else:
            return vol.name.lower()
    
    return lambda vol: get_key(vol), reverse


class Style:
    """Color configuration for the UI."""
    
    def __init__(self, good: str = "green", ok: str = "yellow", bad: str = "red"):
        self.good = good
        self.ok = ok
        self.bad = bad
    
    @classmethod
    def parse(cls, style_str: str) -> "Style":
        """Parse a comma-separated color string (e.g., 'green,yellow,red')."""
        if not style_str:
            return cls()
        
        parts = [p.strip() for p in style_str.split(",")]
        if len(parts) >= 3:
            return cls(good=parts[0], ok=parts[1], bad=parts[2])
        elif len(parts) == 2:
            return cls(good=parts[0], ok=parts[1])
        elif len(parts) == 1:
            return cls(good=parts[0])
        return cls()
    
    def color_for_utilization(self, utilization: float) -> str:
        """Return the appropriate color for a utilization percentage (0.0 to 1.0)."""
        if utilization < 0.8:
            return self.good
        elif utilization < 0.9:
            return self.ok
        else:
            return self.bad


def make_sorter(sort_spec: str) -> Callable[[FileSystem], any]:
    """Create a sort key function from a sort specification.
    
    Format: field=order (e.g., 'capacity=dsc', 'name=asc')
    Supported fields: name, type, capacity, utilization, cost, creation
    """
    if not sort_spec:
        sort_spec = "creation=dsc"
    
    parts = sort_spec.split("=")
    field = parts[0].lower()
    order = parts[1].lower() if len(parts) > 1 else "asc"
    reverse = order == "dsc"
    
    def get_key(fs: FileSystem):
        if field == "name":
            return fs.name.lower()
        elif field == "type":
            return fs.type.value
        elif field == "capacity":
            return fs.storage_capacity
        elif field == "utilization":
            return fs.utilization()
        elif field == "cost":
            return fs.hourly_price
        else:  # Default to creation time
            return fs.creation_time
    
    return lambda fs: get_key(fs), reverse


class UI:
    """Terminal UI for displaying FSx file systems."""
    
    def __init__(
        self,
        store: Store,
        sort: str = "creation=dsc",
        style: Optional[Style] = None,
        disable_pricing: bool = False,
        page_size: int = 10,
        region: Optional[str] = None,
    ):
        self._store = store
        self._style = style or Style()
        self._disable_pricing = disable_pricing
        self._page_size = page_size
        self._current_page = 0
        self._selected_index = 0  # Index within current page
        self._console = Console()
        self._running = False
        self._sort_key, self._sort_reverse = make_sorter(sort)
        self._selected_fs_id: Optional[str] = None  # Set when user presses Enter
        self._ssh_fs_id: Optional[str] = None  # Set when user presses 'c' on an ONTAP FS
        self._region = region
    
    def _get_sorted_file_systems(self, stats: Stats) -> List[FileSystem]:
        """Get file systems sorted according to sort spec."""
        return sorted(stats.file_systems, key=self._sort_key, reverse=self._sort_reverse)
    
    def _get_page_count(self, total_items: int) -> int:
        """Calculate total number of pages."""
        if total_items == 0:
            return 1
        return math.ceil(total_items / self._page_size)
    
    def _get_page_items(self, items: List[FileSystem]) -> List[FileSystem]:
        """Get items for the current page."""
        start = self._current_page * self._page_size
        end = start + self._page_size
        return items[start:end]
    
    def select_next(self) -> None:
        """Move selection down (j key)."""
        stats = self._store.stats()
        sorted_fs = self._get_sorted_file_systems(stats)
        page_items = self._get_page_items(sorted_fs)
        
        if self._selected_index < len(page_items) - 1:
            self._selected_index += 1
        elif self._current_page < self._get_page_count(len(sorted_fs)) - 1:
            # Move to next page
            self._current_page += 1
            self._selected_index = 0
    
    def select_prev(self) -> None:
        """Move selection up (k key)."""
        if self._selected_index > 0:
            self._selected_index -= 1
        elif self._current_page > 0:
            # Move to previous page
            self._current_page -= 1
            self._selected_index = self._page_size - 1
    
    def get_selected_fs_id(self) -> Optional[str]:
        """Get the file system ID that was selected (after Enter pressed)."""
        return self._selected_fs_id

    def get_ssh_fs_id(self) -> Optional[str]:
        """Get the file system ID that was chosen for SSH (after 'c' pressed)."""
        return self._ssh_fs_id

    def _get_current_selection(self) -> Optional[FileSystem]:
        """Get the currently highlighted file system."""
        stats = self._store.stats()
        sorted_fs = self._get_sorted_file_systems(stats)
        page_items = self._get_page_items(sorted_fs)
        
        if 0 <= self._selected_index < len(page_items):
            return page_items[self._selected_index]
        return None

    
    def render_summary(self, stats: Stats) -> Text:
        """Render the summary header with aggregate stats."""
        total_tib = stats.total_capacity / 1024
        used_tib = stats.total_used_capacity / 1024
        utilization = (stats.total_used_capacity / stats.total_capacity * 100) if stats.total_capacity > 0 else 0
        
        summary = Text()
        if self._region:
            summary.append(f"[{self._region}] ", style="dim")
        summary.append(f"{stats.total_file_systems} file systems", style="bold")
        summary.append(" | ")
        summary.append(f"{used_tib:.1f}/{total_tib:.1f} TiB", style="cyan")
        summary.append(f" ({utilization:.1f}% used)")
        
        if not self._disable_pricing and stats.total_hourly_cost > 0:
            monthly = stats.total_hourly_cost * 730
            summary.append(" | ")
            summary.append(f"${monthly:.2f}/mo", style="green")
        
        return summary
    
    def render_progress_bar(self, utilization: float, width: int = 30, gradient: bool = False) -> Text:
        """Render a progress bar for utilization.
        
        If gradient=True, the bar uses smooth color blending based on position.
        Color transitions from green (left) -> yellow -> orange -> red (right).
        Otherwise, the entire bar uses a single color based on utilization threshold.
        """
        filled = int(utilization * width)
        empty = width - filled
        
        bar = Text()
        
        if gradient and filled > 0:
            # Smooth gradient: color based on position in the full bar width
            for i in range(filled):
                position = (i + 1) / width  # Position relative to full bar
                color = interpolate_color(position)
                bar.append("█", style=color)
        else:
            # Single color based on overall utilization
            color = self._style.color_for_utilization(utilization)
            bar.append("█" * filled, style=color)
        
        bar.append("░" * empty, style="dim")
        return bar
    
    def render_file_system_row(self, fs: FileSystem, selected: bool = False) -> List:
        """Render a single file system as a table row.
        
        Args:
            fs: The file system to render
            selected: If True, highlight the file system ID
        """
        utilization = fs.utilization()
        
        # Progress bar with capacity info (width=30 for smoother gradient)
        progress = self.render_progress_bar(utilization, width=30, gradient=True)
        capacity_gib = fs.storage_capacity
        used_gib = fs.used_capacity
        
        # Combine progress bar with capacity text
        capacity_text = Text()
        capacity_text.append_text(progress)
        capacity_text.append(f" {used_gib}/{capacity_gib} GiB")
        
        # CPU utilization with gradient progress bar (width=25 for smoother gradient)
        cpu_pct = fs.cpu_utilization / 100.0  # Convert to 0-1 range
        if fs.cpu_utilization > 0:
            cpu_bar = self.render_progress_bar(cpu_pct, width=25, gradient=True)
            cpu_text = Text()
            cpu_text.append_text(cpu_bar)
            cpu_text.append(f" {fs.cpu_utilization:.0f}%")
        else:
            cpu_text = Text("-")
        
        # Throughput
        throughput = fs.total_throughput()
        throughput_str = f"{throughput:.1f}" if throughput > 0 else "-"
        
        # IOPS
        iops = fs.total_iops()
        iops_str = f"{iops:.0f}" if iops > 0 else "-"
        
        # Price
        if self._disable_pricing or not fs.has_price():
            price_str = "-"
        else:
            monthly = fs.monthly_price()
            price_str = f"${monthly:.0f}/mo"
        
        # File system ID with name on new line - highlight if selected
        fs_id_text = Text()
        if selected:
            fs_id_text.append(fs.id, style="reverse bold cyan")
        else:
            fs_id_text.append(fs.id, style="cyan")
        # Add name on new line (truncate if too long)
        name_display = fs.name if len(fs.name) <= 20 else fs.name[:17] + "..."
        fs_id_text.append(f"\n{name_display}", style="dim")
        
        return [
            fs_id_text,
            fs.type.value,
            capacity_text,
            cpu_text,
            throughput_str,
            iops_str,
            price_str,
        ]
    
    def render(self) -> Table:
        """Render the current state as a Rich Table."""
        stats = self._store.stats()
        
        # Create main table
        table = Table(
            title=self.render_summary(stats),
            show_header=True,
            header_style="bold",
            border_style="dim",
            expand=True,
        )
        
        # Add columns (wider columns for smoother gradient bars)
        table.add_column("ID", style="cyan", no_wrap=True, min_width=15)
        table.add_column("Type", width=8)
        table.add_column("Capacity (GiB)", width=48)  # 30 bar + text
        table.add_column("CPU (%)", width=32)  # 25 bar + text
        table.add_column("MiB/s", width=10, justify="right")
        table.add_column("IOPS", width=7, justify="right")
        if not self._disable_pricing:
            table.add_column("$/mo", width=11, justify="right")
        
        if stats.total_file_systems == 0:
            return table
        
        # Sort and paginate
        sorted_fs = self._get_sorted_file_systems(stats)
        page_items = self._get_page_items(sorted_fs)
        
        # Ensure selected index is within bounds
        if self._selected_index >= len(page_items):
            self._selected_index = max(0, len(page_items) - 1)
        
        # Add rows with selection highlighting on ID only
        for idx, fs in enumerate(page_items):
            is_selected = (idx == self._selected_index)
            row = self.render_file_system_row(fs, selected=is_selected)
            if self._disable_pricing:
                row = row[:-1]  # Remove price column
            table.add_row(*row)
        
        return table
    
    def render_help(self) -> Text:
        """Render help text with styled key bindings."""
        stats = self._store.stats()
        total_pages = self._get_page_count(stats.total_file_systems)
        
        help_text = Text()
        help_text.append(f"Page {self._current_page + 1}/{total_pages}", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("j/k", style="bold white")
        help_text.append(": select", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("Enter", style="bold white")
        help_text.append(": details", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("c", style="bold white")
        help_text.append(": ssh (ONTAP)", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("h/l", style="bold white")
        help_text.append(": page", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("q/Esc", style="bold white")
        help_text.append(": quit", style="dim")
        help_text.append(" (arrow keys also supported)", style="dim italic")
        return help_text
    
    def render_full(self) -> Panel:
        """Render the full UI including table and help."""
        table = self.render()
        help_text = self.render_help()
        
        stats = self._store.stats()
        if stats.total_file_systems == 0:
            content = Text("Discovering file systems...", style="dim italic")
        else:
            content = table
        
        return Panel(
            content,
            subtitle=help_text,
            border_style="blue",
        )
    
    def next_page(self) -> None:
        """Navigate to the next page."""
        stats = self._store.stats()
        total_pages = self._get_page_count(stats.total_file_systems)
        if self._current_page < total_pages - 1:
            self._current_page += 1
    
    def prev_page(self) -> None:
        """Navigate to the previous page."""
        if self._current_page > 0:
            self._current_page -= 1
    
    def run(self, refresh_callback: Optional[Callable[[], None]] = None) -> None:
        """Run the UI main loop with Live display.

        Args:
            refresh_callback: Optional callback for refresh events
        """
        self._running = True
        self._selected_fs_id = None

        _is_win = sys.platform == 'win32'

        # Rich's Live(screen=True) owns the alternate screen buffer — no manual
        # ANSI toggles or terminal clears are needed.

        # Clear screen before drawing so a previous mode's output is gone.
        _clear_screen()

        try:
            with Live(
                self.render_full(),
                console=self._console,
                auto_refresh=False,
                screen=(not _VT_OK),
                vertical_overflow="visible",
            ) as live:
                # Set up keyboard handling in a separate thread
                if sys.platform == 'win32':
                    import msvcrt
                    import time as _wtime
                    # Prime the display
                    live.update(self.render_full(), refresh=True)
                    last_render = _wtime.monotonic()
                    while self._running:
                        dirty = False
                        if msvcrt.kbhit():
                            key = msvcrt.getwch()
                            if key == '\xe0' or key == '\x00':  # Special key prefix
                                key2 = msvcrt.getwch()
                                if key2 == 'H':    self.select_prev(); dirty = True
                                elif key2 == 'P':  self.select_next(); dirty = True
                                elif key2 == 'K':  self.prev_page(); self._selected_index = 0; dirty = True
                                elif key2 == 'M':  self.next_page(); self._selected_index = 0; dirty = True
                            elif key == 'q' or key == '\x03' or key == '\x1b':
                                self._running = False; break
                            elif key == 'j':  self.select_next(); dirty = True
                            elif key == 'k':  self.select_prev(); dirty = True
                            elif key == '\r':
                                selected = self._get_current_selection()
                                if selected:
                                    self._selected_fs_id = selected.id
                                    self._running = False; break
                            elif key == 'c':
                                selected = self._get_current_selection()
                                if selected and selected.type == FileSystemType.ONTAP and selected.management_ip:
                                    self._ssh_fs_id = selected.id
                                    self._running = False; break
                            elif key == 'l':  self.next_page(); self._selected_index = 0; dirty = True
                            elif key == 'h':  self.prev_page(); self._selected_index = 0; dirty = True
                        else:
                            _wtime.sleep(0.05)
                        now = _wtime.monotonic()
                        if dirty or (now - last_render) >= 1.0:
                            rendered = self.render_full()
                            live.update(rendered, refresh=True)
                            last_render = now
                else:
                    import select
                    import termios
                    import tty

                    old_settings = termios.tcgetattr(sys.stdin)
                    try:
                        tty.setcbreak(sys.stdin.fileno())
                        import os as _os
                        import time as _time
                        stdin_fd = sys.stdin.fileno()
                        buf = ''
                        esc_started_at = None
                        last_tick = 0.0
                        ESC_TIMEOUT = 0.15
                        RENDER_INTERVAL = 0.25

                        # Prime the display
                        live.update(self.render_full(), refresh=True)
                        last_tick = _time.monotonic()

                        while self._running:
                            dirty = False

                            if select.select([stdin_fd], [], [], 0.03)[0]:
                                try:
                                    chunk = _os.read(stdin_fd, 1024).decode('utf-8', errors='replace')
                                except OSError:
                                    chunk = ''
                                if chunk:
                                    buf += chunk

                            stop = False
                            while buf:
                                if buf[0] == '\x1b':
                                    if esc_started_at is None:
                                        esc_started_at = _time.monotonic()
                                    if len(buf) >= 3 and buf[1] in ('[', 'O'):
                                        third = buf[2]
                                        buf = buf[3:]
                                        esc_started_at = None
                                        if third == 'A':    self.select_prev(); dirty = True
                                        elif third == 'B':  self.select_next(); dirty = True
                                        elif third == 'D':  self.prev_page(); self._selected_index = 0; dirty = True
                                        elif third == 'C':  self.next_page(); self._selected_index = 0; dirty = True
                                        continue
                                    if len(buf) == 2 and buf[1] in ('[', 'O'):
                                        if select.select([stdin_fd], [], [], 0.05)[0]:
                                            try:
                                                more = _os.read(stdin_fd, 16).decode('utf-8', errors='replace')
                                            except OSError:
                                                more = ''
                                            if more:
                                                buf += more
                                                continue
                                    if _time.monotonic() - esc_started_at >= ESC_TIMEOUT:
                                        # Bare Esc (no escape sequence) -> quit.
                                        buf = buf[1:]
                                        esc_started_at = None
                                        self._running = False
                                        stop = True
                                        break
                                    break
                                ch = buf[0]
                                buf = buf[1:]
                                if ch == 'q' or ch == '\x03':
                                    self._running = False
                                    stop = True
                                    break
                                elif ch == 'j':  self.select_next(); dirty = True
                                elif ch == 'k':  self.select_prev(); dirty = True
                                elif ch == 'l':  self.next_page(); self._selected_index = 0; dirty = True
                                elif ch == 'h':  self.prev_page(); self._selected_index = 0; dirty = True
                                elif ch == 'c':
                                    selected = self._get_current_selection()
                                    if selected and selected.type == FileSystemType.ONTAP and selected.management_ip:
                                        self._ssh_fs_id = selected.id
                                        self._running = False
                                        stop = True
                                        break
                                elif ch in ('\r', '\n'):
                                    selected = self._get_current_selection()
                                    if selected:
                                        self._selected_fs_id = selected.id
                                        self._running = False
                                        stop = True
                                        break
                            if stop:
                                break

                            now = _time.monotonic()
                            if dirty or (now - last_tick) >= RENDER_INTERVAL:
                                live.update(self.render_full(), refresh=True)
                                last_tick = now
                    finally:
                        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except Exception:
            # Fallback for non-TTY environments
            self._console.print(self.render_full())

    def stop(self) -> None:
        """Stop the UI loop."""
        self._running = False


class DetailUI:
    """Detail view UI for a single file system."""
    
    def __init__(
        self,
        store: DetailStore,
        style: Optional[Style] = None,
        disable_pricing: bool = False,
        page_size: int = 10,
        sort: str = "name=asc",
        name_filter: Optional[str] = None,
        region: Optional[str] = None,
    ):
        self._store = store
        self._style = style or Style()
        self._disable_pricing = disable_pricing
        self._page_size = page_size
        self._current_page = 0
        self._console = Console()
        self._running = False
        self._sort_key, self._sort_reverse = make_volume_sorter(sort)
        self._name_filter = name_filter
        self._region = region
        self._selected_index = 0      # index into current page of volumes
        self._selected_volume_id: Optional[str] = None
        self._volume_detail_mode = False  # True when drilled into volume AP view
    
    def _get_page_count(self, total_items: int) -> int:
        """Calculate total number of pages."""
        if total_items == 0:
            return 1
        return math.ceil(total_items / self._page_size)
    
    def _get_page_items(self, items: List, page: int = None) -> List:
        """Get items for the specified page (or current page)."""
        if page is None:
            page = self._current_page
        start = page * self._page_size
        end = start + self._page_size
        return items[start:end]
    
    def next_page(self) -> None:
        """Navigate to the next page."""
        fs = self._store.get_file_system()
        if fs is None:
            return

        # In volume-detail mode, paginate the selected volume's access points
        if self._volume_detail_mode:
            vol = None
            for v in self._store.get_volumes():
                if v.id == self._selected_volume_id:
                    vol = v
                    break
            total_items = len(vol.access_points) if vol else 0
        # Get total items based on file system type (use filtered count for volumes)
        elif fs.type in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
            total_items = len(self._get_sorted_volumes())
        elif fs.type == FileSystemType.LUSTRE:
            total_items = len(self._store.get_mds_servers())
        else:
            return  # Windows has no sub-resources to paginate

        total_pages = self._get_page_count(total_items)
        if self._current_page < total_pages - 1:
            self._current_page += 1
    
    def prev_page(self) -> None:
        """Navigate to the previous page."""
        if self._current_page > 0:
            self._current_page -= 1
    
    def _get_sorted_volumes(self) -> List[Volume]:
        """Get volumes filtered and sorted according to specs."""
        volumes = self._store.get_volumes()
        
        # Apply name filter if specified
        if self._name_filter:
            filter_lower = self._name_filter.lower()
            volumes = [v for v in volumes if filter_lower in v.name.lower()]
        
        return sorted(volumes, key=self._sort_key, reverse=self._sort_reverse)

    def _current_selected_volume(self) -> Optional[Volume]:
        """Return the volume currently highlighted in the table, or None."""
        volumes = self._get_sorted_volumes()
        if not volumes:
            return None
        page = self._get_page_items(volumes)
        if not page:
            return None
        idx = max(0, min(self._selected_index, len(page) - 1))
        vol = page[idx]
        self._selected_volume_id = vol.id
        return vol

    def select_next_volume(self) -> None:
        volumes = self._get_sorted_volumes()
        page = self._get_page_items(volumes)
        if self._selected_index < len(page) - 1:
            self._selected_index += 1
        elif self._current_page < self._get_page_count(len(volumes)) - 1:
            self._current_page += 1
            self._selected_index = 0
        self._current_selected_volume()

    def select_prev_volume(self) -> None:
        if self._selected_index > 0:
            self._selected_index -= 1
        elif self._current_page > 0:
            self._current_page -= 1
            self._selected_index = self._page_size - 1
        self._current_selected_volume()

    def enter_volume_detail(self) -> None:
        """Drill into the selected volume's access-point detail view."""
        if self._current_selected_volume() is not None:
            self._volume_detail_mode = True
            self._current_page = 0  # reuse pagination for AP list

    def exit_volume_detail(self) -> None:
        self._volume_detail_mode = False
        self._current_page = 0

    def _render_volume_detail(self) -> Panel:
        """Render the drilled-down volume view.

        For ONTAP volumes, shows a 60/40 panel (volume stats on the left,
        latency on the right) followed by the S3 access-points table below.
        For OpenZFS volumes, keeps the original compact layout (header +
        access-points) since no extra per-volume metrics are published.
        """
        fs = self._store.get_file_system()
        vol = None
        for v in self._store.get_volumes():
            if v.id == self._selected_volume_id:
                vol = v
                break

        if vol is None or fs is None:
            return Panel(Text("Volume not found", style="red"), title="Volume Detail")

        header = Text()
        header.append(f"Volume {vol.id}", style="bold cyan")
        header.append(f"  {vol.name}\n", style="bold")
        header.append(f"Type: {vol.type}   ")
        header.append(f"Capacity: {vol.used_capacity}/{vol.storage_capacity} GiB\n")

        # AP table (used by both branches).
        ap_table = Table(show_header=True, header_style="bold", border_style="dim", expand=True)
        ap_table.add_column("Name", style="cyan", no_wrap=True)
        ap_table.add_column("Alias")
        ap_table.add_column("Lifecycle", width=12)
        ap_table.add_column("VPC", width=24)

        aps = vol.access_points
        total_pages = self._get_page_count(len(aps)) if aps else 1
        if not aps:
            ap_table.add_row("-", "no S3 access points", "", "")
        else:
            page = self._get_page_items(aps)
            for ap in page:
                ap_table.add_row(ap.name or "-", ap.alias or "-", ap.lifecycle or "-", ap.vpc_id or "Internet")
            header.append(f"\nPage {self._current_page + 1}/{total_pages}  ({len(aps)} access point{'s' if len(aps) != 1 else ''})\n", style="dim")

        help_text = Text("\n[h/l] page  [q/Esc] back to volume list", style="dim")

        # OpenZFS: keep the original compact layout (no extra volume metrics published).
        if vol.type != 'ONTAP':
            return Panel(Group(header, ap_table, help_text),
                         title=f"{fs.name} / {vol.id}", border_style="cyan")

        # ONTAP: build the perf + latency panel in a 60/40 grid, then AP table below.
        perf_table = self._render_volume_perf_panel(vol)
        latency_table = self._render_volume_latency_table(vol)

        if perf_table is not None and latency_table is not None:
            grid = Table.grid(expand=True, padding=(0, 1))
            grid.add_column(ratio=3)  # 60%
            grid.add_column(ratio=2)  # 40%
            grid.add_row(perf_table, latency_table)
            top = grid
        elif perf_table is not None:
            top = perf_table
        elif latency_table is not None:
            top = latency_table
        else:
            top = Text("")

        return Panel(
            Group(header, Text(""), top, Text(""), ap_table, help_text),
            title=f"{fs.name} / {vol.id}",
            border_style="cyan",
        )

    def _render_volume_perf_panel(self, vol: Volume) -> Optional[Table]:
        """Compact per-volume stats table (ONTAP only).

        Contents: capacity bar, inode bar, R/W IOPS, metadata IOPS, R/W
        throughput, capacity-pool ops.
        """
        t = Table(
            title="Volume stats",
            title_style="bold dim",
            title_justify="left",
            show_header=False,
            border_style="dim",
            expand=True,
            padding=(0, 1),
        )
        t.add_column("Metric", style="dim", min_width=22)
        t.add_column("Value", min_width=30)

        # Capacity bar.
        if vol.storage_capacity > 0:
            cap_frac = vol.utilization()
            cap_cell = Text()
            cap_cell.append_text(self._render_progress_bar(cap_frac, width=30, gradient=True))
            cap_cell.append(f" {vol.used_capacity}/{vol.storage_capacity} GiB ({cap_frac*100:.1f}%)")
            t.add_row("Capacity", cap_cell)
        else:
            t.add_row("Capacity", Text(f"{vol.used_capacity} GiB", style="dim"))

        # Inode utilisation.
        if vol.files_capacity > 0:
            inode_frac = vol.inode_utilization()
            inode_cell = Text()
            inode_cell.append_text(self._render_progress_bar(inode_frac, width=30, gradient=True))
            inode_cell.append(f" {vol.files_used:,}/{vol.files_capacity:,} ({inode_frac*100:.1f}%)")
            t.add_row("Inode util", inode_cell)
        else:
            t.add_row("Inode util", Text("—", style="dim"))

        # Client IOPS (read / write / metadata).
        def _num(v: float, fmt: str = "{:.0f}") -> Text:
            if v <= 0:
                return Text("—", style="dim")
            return Text(fmt.format(v), style="bold bright_white")

        t.add_row("Read IOPS", _num(vol.read_iops))
        t.add_row("Write IOPS", _num(vol.write_iops))
        t.add_row("Metadata IOPS", _num(vol.metadata_iops))

        # Throughput (MiB/s).
        t.add_row("Read throughput", _num(vol.read_throughput, "{:.1f} MiB/s"))
        t.add_row("Write throughput", _num(vol.write_throughput, "{:.1f} MiB/s"))

        # Capacity pool tiering ops.
        cp_read_cell = _num(vol.capacity_pool_read_iops)
        cp_write_cell = _num(vol.capacity_pool_write_iops)
        t.add_row("Capacity pool read ops/s", cp_read_cell)
        t.add_row("Capacity pool write ops/s", cp_write_cell)

        return t

    def _render_volume_latency_table(self, vol: Volume) -> Optional[Table]:
        """Per-volume latency table (ONTAP only). Same styling as the FS-level panel."""
        lat = vol.latency_metrics or LatencyMetrics()
        t = Table(
            title="Latency (avg ms/op)",
            title_style="bold dim",
            title_justify="left",
            show_header=False,
            border_style="dim",
            expand=True,
            padding=(0, 1),
        )
        t.add_column("Op", style="dim", min_width=10)
        t.add_column("Value", justify="right")

        def color(ms: Optional[float]) -> str:
            if ms is None:
                return "dim"
            if ms > 10:
                return "bold red"
            if ms > 2:
                return "bold yellow"
            return "bold bright_white"

        def cell(ms: Optional[float]) -> Text:
            if ms is None:
                return Text("—", style="dim")
            return Text(f"{ms:.2f} ms", style=color(ms))

        t.add_row("Read", cell(lat.read_ms))
        t.add_row("Write", cell(lat.write_ms))
        t.add_row("Metadata", cell(lat.metadata_ms))
        return t
    
    def _render_perf_panel(self, fs: FileSystem) -> List[Table]:
        """Render performance utilization as a single combined table.

        Returns a one-element list (or empty) so the caller can splat it into
        a Group the same way as before.
        """
        perf = fs.perf_metrics
        has_cpu = fs.cpu_utilization > 0
        if (perf is None or not perf.any()) and not has_cpu:
            return []

        # (label, value, inverted); inverted=True means higher is better, so
        # the gradient is flipped (100% = green, 0% = red).
        rows: List = []

        def add(label: str, value: Optional[float], inverted: bool = False) -> None:
            if value is None:
                return
            rows.append((label, value, inverted))

        if has_cpu:
            add("CPU util", fs.cpu_utilization)
        if perf is not None:
            add("Network throughput util", perf.network_throughput_util)
            add("Disk throughput util", perf.disk_throughput_util)
            add("Disk throughput burst", perf.disk_throughput_burst_balance, inverted=True)
            add("Disk IOPS util", perf.disk_iops_util)
            add("Disk IOPS burst", perf.disk_iops_burst_balance, inverted=True)
            add("Cache hit ratio", perf.cache_hit_ratio, inverted=True)
            add("Disk IOPS util (SSD)", perf.ssd_iops_util)

        if not rows:
            return []

        t = Table(
            title="File Server and SSD performance",
            title_style="bold dim",
            title_justify="left",
            show_header=False,
            border_style="dim",
            expand=True,
            padding=(0, 1),
        )
        t.add_column("Metric", style="dim", min_width=22)
        t.add_column("Value", min_width=45)
        for label, value, inverted in rows:
            frac = max(0.0, min(1.0, float(value) / 100.0))
            if inverted:
                # Higher value = better: invert the gradient palette so a full
                # bar renders green (healthy), empty bar renders red.
                bar = Text()
                filled = int(frac * 30)
                for i in range(filled):
                    pos = 1.0 - ((i + 1) / 30)
                    bar.append("█", style=interpolate_color(pos))
                bar.append("░" * (30 - filled), style="dim")
            else:
                bar = self._render_progress_bar(frac, width=30, gradient=True)
            cell = Text()
            cell.append_text(bar)
            cell.append(f" {value:5.1f}%")
            t.add_row(label, cell)

        return [t]

    def _render_latency_table(self, fs: FileSystem) -> Optional[Table]:
        """Render the read/write/metadata latency table (avg ms/op).

        Returns None only for Lustre (no latency metrics are published).
        For other FS types the table is always rendered; rows with no recent
        operations (zero ops in the last minute) display as ``—``.
        Values are bold and color-graded:
        - bright_white for <= 2 ms (normal)
        - yellow for 2-10 ms (slow)
        - red for > 10 ms (very slow)
        """
        if fs.type == FileSystemType.LUSTRE:
            return None
        lat = fs.latency_metrics or LatencyMetrics()

        t = Table(
            title="Latency (avg ms/op)",
            title_style="bold dim",
            title_justify="left",
            show_header=False,
            border_style="dim",
            expand=True,
            padding=(0, 1),
        )
        t.add_column("Op", style="dim", min_width=10)
        t.add_column("Value", justify="right")

        def color(ms: Optional[float]) -> str:
            if ms is None:
                return "dim"
            if ms > 10:
                return "bold red"
            if ms > 2:
                return "bold yellow"
            return "bold bright_white"

        def cell(ms: Optional[float]) -> Text:
            if ms is None:
                return Text("—", style="dim")
            return Text(f"{ms:.2f} ms", style=color(ms))

        # Order: Read, Write, Metadata. Hide Metadata row entirely on Windows
        # (it never has the metric) to avoid a perpetually-dim row.
        t.add_row("Read", cell(lat.read_ms))
        t.add_row("Write", cell(lat.write_ms))
        if fs.type != FileSystemType.WINDOWS:
            t.add_row("Metadata", cell(lat.metadata_ms))
        return t

    def _perf_parts(self, fs: FileSystem) -> List:
        """Return a renderable list ([Text(''), ...]) for the Group.

        When both the perf panel and latency table are present, they render
        side-by-side in a 60/40 grid that reflows with terminal width.
        Otherwise either one (or neither) is emitted full-width.
        """
        perf_tables = self._render_perf_panel(fs)
        latency_table = self._render_latency_table(fs)

        if not perf_tables and latency_table is None:
            return []

        if perf_tables and latency_table is not None:
            layout = Table.grid(expand=True, padding=(0, 1))
            layout.add_column(ratio=3)  # 60%
            layout.add_column(ratio=2)  # 40%
            layout.add_row(perf_tables[0], latency_table)
            return [Text(""), layout]

        parts: List = []
        for t in perf_tables:
            parts += [Text(""), t]
        if latency_table is not None:
            parts += [Text(""), latency_table]
        return parts

    def _render_progress_bar(self, utilization: float, width: int = 12, gradient: bool = False) -> Text:
        """Render a progress bar for utilization.
        
        If gradient=True, the bar uses smooth color blending based on position.
        Color transitions from green (left) -> yellow -> orange -> red (right).
        Otherwise, the entire bar uses a single color based on utilization threshold.
        """
        filled = int(utilization * width)
        empty = width - filled
        
        bar = Text()
        
        if gradient and filled > 0:
            # Smooth gradient: color based on position in the full bar width
            for i in range(filled):
                position = (i + 1) / width  # Position relative to full bar
                color = interpolate_color(position)
                bar.append("█", style=color)
        else:
            # Single color based on overall utilization
            color = self._style.color_for_utilization(utilization)
            bar.append("█" * filled, style=color)
        
        bar.append("░" * empty, style="dim")
        return bar
    
    def _render_pricing_breakdown(self, fs: FileSystem) -> Text:
        """Render itemized monthly cost breakdown."""
        text = Text()
        if self._disable_pricing or fs.pricing_breakdown is None:
            return text
        
        b = fs.pricing_breakdown
        text.append("Monthly Cost: ", style="dim")
        
        parts = []
        if b.storage > 0:
            parts.append(f"Storage ${b.storage:,.0f}")
        if b.throughput > 0:
            parts.append(f"Throughput ${b.throughput:,.0f}")
        if b.iops > 0:
            parts.append(f"IOPS ${b.iops:,.0f}")
        if b.capacity_pool > 0:
            parts.append(f"Cap Pool ${b.capacity_pool:,.0f}")
        
        if parts:
            text.append(" + ".join(parts))
            text.append(f" = ", style="dim")
        text.append(f"${b.total:,.0f}/mo", style="bold green")
        
        return text
    
    def _render_header(self, fs: FileSystem) -> Text:
        """Render the file system header with basic info."""
        header = Text()
        if self._region:
            header.append(f"[{self._region}] ", style="dim")
        header.append(f"{fs.id}", style="bold cyan")
        header.append(" | ")
        header.append(f"{fs.type.value}", style="bold")
        header.append(" | ")
        header.append(f"{fs.storage_capacity} GiB", style="cyan")

        if fs.used_capacity > 0:
            utilization = fs.utilization() * 100
            header.append(f" ({utilization:.1f}% used)")

        # Deployment type, HA pairs (ONTAP only when >1), and AZs.
        if fs.deployment_type:
            header.append(" | ")
            header.append("Deployment: ", style="dim")
            header.append(fs.deployment_type)
        if fs.type == FileSystemType.ONTAP and fs.ha_pairs > 1:
            header.append(" | ")
            header.append("HA pairs: ", style="dim")
            header.append(str(fs.ha_pairs))
        if any(fs.availability_zones):
            header.append(" | ")
            header.append("AZ: ", style="dim")
            header.append(self._format_azs(fs))

        return header

    def _format_azs(self, fs: FileSystem) -> str:
        """Format AZ list, labelling preferred vs standby for Multi-AZ FS."""
        # Single-AZ (or missing preferred subnet info): just the AZ name(s).
        is_multi_az = bool(fs.preferred_subnet_id) and len([s for s in fs.availability_zones if s]) > 1
        if not is_multi_az:
            return ", ".join(az for az in fs.availability_zones if az)

        # Pair each AZ with its subnet; preferred subnet -> preferred AZ.
        pairs = list(zip(fs.subnet_ids, fs.availability_zones))
        preferred = next((az for sn, az in pairs if sn == fs.preferred_subnet_id and az), None)
        standby = [az for sn, az in pairs if sn != fs.preferred_subnet_id and az]
        parts = []
        if preferred:
            parts.append(f"{preferred} (preferred)")
        parts.extend(f"{az} (standby)" for az in standby)
        return ", ".join(parts)

    def _render_fs_metrics(self, fs: FileSystem) -> Text:
        """Render provisioned throughput/IOPS for the file system."""
        metrics = Text()

        # Provisioned throughput capacity. Lustre's value is MBps per TiB;
        # others are flat MBps.
        if fs.throughput_capacity > 0:
            metrics.append("Provisioned throughput: ", style="dim")
            if fs.type == FileSystemType.LUSTRE:
                metrics.append(f"{fs.throughput_capacity} MBps/TiB")
            else:
                metrics.append(f"{fs.throughput_capacity} MBps")
        else:
            metrics.append("Provisioned throughput: ", style="dim")
            metrics.append("-")

        metrics.append(" | ")
        metrics.append("Provisioned IOPS: ", style="dim")
        metrics.append(f"{fs.provisioned_iops:,}" if fs.provisioned_iops > 0 else "-")

        return metrics
    
    def _render_page_info(self, total_items: int) -> Text:
        """Render pagination info and help text with styled key bindings."""
        total_pages = self._get_page_count(total_items)
        
        help_text = Text()
        help_text.append(f"Page {self._current_page + 1}/{total_pages}", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("h/l", style="bold white")
        help_text.append(": page", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("j/k", style="bold white")
        help_text.append(": select volume", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("Enter", style="bold white")
        help_text.append(": volume details", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("q/Esc", style="bold white")
        help_text.append(": quit", style="dim")
        help_text.append(" (arrow keys also supported)", style="dim italic")
        return help_text
    
    def render(self) -> Panel:
        """Render the appropriate detail view based on file system type."""
        fs = self._store.get_file_system()
        if fs is None:
            return Panel(
                Text("Discovering file system...", style="dim italic"),
                title="FSx Detail View",
                border_style="blue",
            )

        # Drill-down: volume access-point detail view
        if self._volume_detail_mode:
            return self._render_volume_detail()

        if fs.type == FileSystemType.ONTAP:
            return self._render_ontap_detail(fs)
        elif fs.type == FileSystemType.OPENZFS:
            return self._render_openzfs_detail(fs)
        elif fs.type == FileSystemType.LUSTRE:
            return self._render_lustre_detail(fs)
        else:  # WINDOWS
            return self._render_windows_detail(fs)
    
    def _render_volume_table(self, volumes: List[Volume], fs_type: str, fs_capacity: int = 0) -> Table:
        """Render a table of volumes (shared by ONTAP and OpenZFS).
        
        Args:
            volumes: List of volumes to render
            fs_type: "ONTAP" or "OPENZFS"
            fs_capacity: File system capacity in GiB (used for OpenZFS volumes without quota)
        """
        table = Table(
            show_header=True,
            header_style="bold",
            border_style="dim",
            expand=True,
        )
        
        # Add columns with metric symbols (wider for smoother gradients)
        table.add_column("Volume ID", style="cyan", no_wrap=True, min_width=15)
        table.add_column("Name", min_width=10)
        table.add_column("Capacity (GiB)", width=48)  # 30 bar + text
        table.add_column("IOPS (r/w)", width=12, justify="right")
        table.add_column("MiB/s (r/w)", width=14, justify="right")
        table.add_column("S3", width=4, justify="right")

        if not volumes:
            return table

        for idx, vol in enumerate(volumes):
            # Capacity with progress bar (smooth gradient, width=30)
            used = vol.used_capacity
            capacity = vol.storage_capacity
            
            # For OpenZFS without quota, use file system capacity
            if fs_type == "OPENZFS" and capacity == 0 and fs_capacity > 0:
                capacity = fs_capacity
            
            # Calculate utilization
            if capacity > 0:
                utilization = min(1.0, used / capacity)
            else:
                utilization = 0.0
            
            progress = self._render_progress_bar(utilization, width=30, gradient=True)
            capacity_text = Text()
            capacity_text.append_text(progress)
            capacity_text.append(f" {used}/{capacity} GiB")
            
            # IOPS (read/write/total)
            total_iops = vol.total_iops()
            if total_iops > 0:
                iops_str = f"{vol.read_iops:.0f}r/{vol.write_iops:.0f}w"
            else:
                iops_str = "-"
            
            # Throughput (read/write/total)
            total_throughput = vol.total_throughput()
            if total_throughput > 0:
                throughput_str = f"{vol.read_throughput:.1f}r/{vol.write_throughput:.1f}w"
            else:
                throughput_str = "-"
            
            # S3 AP count (highlight selected row's ID)
            ap_count = len(vol.access_points)
            ap_str = str(ap_count) if ap_count > 0 else "-"

            vol_id_text = vol.id
            if self._selected_volume_id == vol.id:
                vol_id_text = Text(vol.id, style="reverse cyan")

            table.add_row(
                vol_id_text,
                vol.name,
                capacity_text,
                iops_str,
                throughput_str,
                ap_str,
            )
        
        return table
    
    def _render_ontap_detail(self, fs: FileSystem) -> Panel:
        """Render ONTAP file system with volume table."""
        volumes = self._get_sorted_volumes()
        
        # Header section
        header = self._render_header(fs)
        metrics = self._render_fs_metrics(fs)
        pricing = self._render_pricing_breakdown(fs)
        perf_parts = self._perf_parts(fs)

        # Volume table with pagination
        if volumes:
            total_pages = self._get_page_count(len(volumes))
            page_volumes = self._get_page_items(volumes)
            volume_table = self._render_volume_table(page_volumes, "ONTAP")
            page_info = self._render_page_info(len(volumes))
            parts = [header, metrics, pricing, *perf_parts,
                     Text(""), volume_table, Text(""), page_info]
            content = Group(*parts)
        else:
            parts = [header, metrics, *perf_parts,
                     Text(""), Text("Discovering volumes...", style="dim italic")]
            content = Group(*parts)
        
        return Panel(
            content,
            title=f"FSx ONTAP Detail - {fs.id}",
            border_style="blue",
        )
    
    def _render_openzfs_detail(self, fs: FileSystem) -> Panel:
        """Render OpenZFS file system with volume table."""
        volumes = self._get_sorted_volumes()
        
        header = self._render_header(fs)
        metrics = self._render_fs_metrics(fs)
        pricing = self._render_pricing_breakdown(fs)
        perf_parts = self._perf_parts(fs)

        # Volume table with pagination (pass fs capacity for volumes without quota)
        if volumes:
            total_pages = self._get_page_count(len(volumes))
            page_volumes = self._get_page_items(volumes)
            volume_table = self._render_volume_table(page_volumes, "OPENZFS", fs.storage_capacity)
            page_info = self._render_page_info(len(volumes))
            parts = [header, metrics, pricing, *perf_parts,
                     Text(""), volume_table, Text(""), page_info]
            content = Group(*parts)
        else:
            parts = [header, metrics, *perf_parts,
                     Text(""), Text("Discovering volumes...", style="dim italic")]
            content = Group(*parts)
        
        return Panel(
            content,
            title=f"FSx OpenZFS Detail - {fs.id}",
            border_style="blue",
        )
    
    def _render_lustre_obj_storage_panel(self,
                                          oss_list: List[ObjectStorageServer],
                                          ost_list: List[ObjectStorageTarget]
                                          ) -> Optional[Table]:
        """Lustre 'Object storage performance' section.

        Three rows, averaged across all OSSs/OSTs:
        - Network throughput utilization (OSS)
        - Disk throughput utilization (OSS)
        - Disk IOPS utilization (OST)
        """
        if not oss_list and not ost_list:
            return None

        def _avg(values: List[float]) -> Optional[float]:
            vals = [v for v in values if v is not None]
            return sum(vals) / len(vals) if vals else None

        net_avg = _avg([o.network_throughput_util for o in oss_list])
        dtu_avg = _avg([o.disk_throughput_util for o in oss_list])
        iops_avg = _avg([o.disk_iops_util for o in ost_list if o.disk_iops_util is not None])

        t = Table(
            title="Object storage performance (avg across OSSs/OSTs)",
            title_style="bold dim", title_justify="left",
            show_header=False, border_style="dim", expand=True, padding=(0, 1),
        )
        t.add_column("Metric", style="dim", no_wrap=True)
        t.add_column("Value", width=40, no_wrap=True)

        def _cell(v: Optional[float]) -> Text:
            if v is None:
                return Text("—", style="dim")
            frac = max(0.0, min(1.0, v / 100.0))
            bar = self._render_progress_bar(frac, width=30, gradient=True)
            cell = Text()
            cell.append_text(bar)
            cell.append(f" {v:5.1f}%")
            return cell

        t.add_row("Network throughput util (OSS)", _cell(net_avg))
        t.add_row("Disk throughput util (OSS)", _cell(dtu_avg))
        t.add_row("Disk IOPS util (OST)", _cell(iops_avg))
        return t

    def _render_lustre_metadata_panel(self, fs: FileSystem,
                                       mds_list: List[MetadataServer]) -> Optional[Table]:
        """Lustre 'Metadata performance' section.

        Two rows averaged across all MDSs/MDTs:
        - Metadata IOPS utilization (MDT) — client-derived, stored on
          fs.metadata_iops_util_avg by the controller.
        - CPU utilization (MDS)
        """
        if not mds_list and fs.metadata_iops_util_avg is None:
            return None

        cpu_vals = [m.cpu_utilization for m in mds_list if m.cpu_utilization is not None]
        cpu_avg = (sum(cpu_vals) / len(cpu_vals)) if cpu_vals else None

        t = Table(
            title="Metadata performance (avg across all MDSs/MDTs)",
            title_style="bold dim", title_justify="left",
            show_header=False, border_style="dim", expand=True, padding=(0, 1),
        )
        t.add_column("Metric", style="dim", no_wrap=True)
        t.add_column("Value", width=40, no_wrap=True)

        def _cell(v: Optional[float]) -> Text:
            if v is None:
                return Text("—", style="dim")
            frac = max(0.0, min(1.0, v / 100.0))
            bar = self._render_progress_bar(frac, width=30, gradient=True)
            cell = Text()
            cell.append_text(bar)
            cell.append(f" {v:5.1f}%")
            return cell

        t.add_row("Metadata IOPS util (MDT)", _cell(fs.metadata_iops_util_avg))
        t.add_row("CPU util (MDS)", _cell(cpu_avg))
        return t

    def _render_mds_mdt_table(self,
                               mds_list: List[MetadataServer],
                               mdt_list: List[MetadataTarget]) -> Optional[Table]:
        """Combined MDS/MDT row table: one row per MDS/MDT pair.

        Columns: MDS ID | CPU utilization (MDS) | MDT ID | Metadata IOPS utilization (MDT)
        MDS and MDT are 1:1 by convention; when counts differ we pad with —.
        """
        if not mds_list and not mdt_list:
            return None

        t = Table(
            show_header=True, header_style="bold",
            border_style="dim", expand=True,
        )
        t.add_column("MDS ID", style="cyan", no_wrap=True, min_width=10)
        t.add_column("CPU util (MDS)", min_width=40)
        t.add_column("MDT ID", style="cyan", no_wrap=True, min_width=10)
        t.add_column("Metadata IOPS util (MDT)", min_width=40)

        def _cell(v: Optional[float]) -> Text:
            if v is None:
                return Text("—", style="dim")
            frac = max(0.0, min(1.0, v / 100.0))
            bar = self._render_progress_bar(frac, width=30, gradient=True)
            cell = Text()
            cell.append_text(bar)
            cell.append(f" {v:5.1f}%")
            return cell

        # Zip MDSs and MDTs by ordered index; pad the shorter side.
        n = max(len(mds_list), len(mdt_list))
        for i in range(n):
            mds = mds_list[i] if i < len(mds_list) else None
            mdt = mdt_list[i] if i < len(mdt_list) else None
            t.add_row(
                mds.id if mds else "—",
                _cell(mds.cpu_utilization) if mds else Text("—", style="dim"),
                mdt.id if mdt else "—",
                _cell(mdt.metadata_iops_util) if mdt else Text("—", style="dim"),
            )
        return t

    def _render_lustre_detail(self, fs: FileSystem) -> Panel:
        """Render Lustre file system: header + client connections line +
        Object storage performance + Metadata performance + MDS/MDT table.
        The FS-level file-server/latency panels are intentionally omitted
        for Lustre (their constituent metrics are presented here instead).
        """
        mds_servers = self._store.get_mds_servers()
        oss_servers = self._store.get_oss_servers()
        ost_targets = self._store.get_ost_targets()
        mdt_targets = self._store.get_mdt_targets()

        # Header and file-system-level metadata lines.
        header = self._render_header(fs)
        metrics = self._render_fs_metrics(fs)
        pricing = self._render_pricing_breakdown(fs)

        # Client connections line (Lustre-only).
        client_line = Text()
        client_line.append("Client connections: ", style="dim")
        if fs.client_connections is None:
            client_line.append("—", style="dim")
        else:
            client_line.append(f"{fs.client_connections}")

        # Section tables (any may be None during initial load).
        obj_panel = self._render_lustre_obj_storage_panel(oss_servers, ost_targets)
        meta_panel = self._render_lustre_metadata_panel(fs, mds_servers)
        pair_table = self._render_mds_mdt_table(mds_servers, mdt_targets)

        parts: List = [header, metrics, pricing, client_line]

        # Pack Object-storage + Metadata panels side-by-side 50:50 when both
        # are present; otherwise emit whichever exists on its own row.
        if obj_panel is not None and meta_panel is not None:
            side_by_side = Table.grid(expand=True, padding=(0, 1))
            side_by_side.add_column(ratio=1)
            side_by_side.add_column(ratio=1)
            side_by_side.add_row(obj_panel, meta_panel)
            parts += [Text(""), side_by_side]
        elif obj_panel is not None:
            parts += [Text(""), obj_panel]
        elif meta_panel is not None:
            parts += [Text(""), meta_panel]

        if pair_table is not None:
            parts += [Text(""), pair_table]

        if obj_panel is None and meta_panel is None and pair_table is None:
            parts += [Text(""), Text("Discovering Lustre metrics...", style="dim italic")]

        return Panel(
            Group(*parts),
            title=f"FSx Lustre Detail - {fs.id}",
            border_style="blue",
        )
    
    def _render_windows_detail(self, fs: FileSystem) -> Panel:
        """Render Windows file system (no sub-resources)."""
        header = self._render_header(fs)
        pricing = self._render_pricing_breakdown(fs)
        
        # Metrics table
        metrics_table = Table(
            show_header=True,
            header_style="bold",
            border_style="dim",
            expand=True,
        )
        
        metrics_table.add_column("Metric", style="dim", min_width=15)
        metrics_table.add_column("Value", min_width=45)  # Wider for 30-char bars
        
        # Capacity with progress bar (smooth gradient, width=30)
        utilization = fs.utilization()
        progress = self._render_progress_bar(utilization, width=30, gradient=True)
        capacity_text = Text()
        capacity_text.append_text(progress)
        capacity_text.append(f" {fs.used_capacity}/{fs.storage_capacity} GiB ({utilization*100:.1f}%)")
        metrics_table.add_row("Capacity", capacity_text)
        
        # CPU (width=30 for smoother gradient)
        if fs.cpu_utilization > 0:
            cpu_pct = fs.cpu_utilization / 100.0
            cpu_bar = self._render_progress_bar(cpu_pct, width=30, gradient=True)
            cpu_text = Text()
            cpu_text.append_text(cpu_bar)
            cpu_text.append(f" {fs.cpu_utilization:.1f}%")
        else:
            cpu_text = Text("-")
        metrics_table.add_row("CPU", cpu_text)
        
        # Throughput
        throughput = fs.total_throughput()
        throughput_text = Text(f"{throughput:.1f} MiB/s" if throughput > 0 else "-")
        metrics_table.add_row("Throughput", throughput_text)
        
        # IOPS
        iops = fs.total_iops()
        iops_text = Text(f"{iops:.0f}" if iops > 0 else "-")
        metrics_table.add_row("IOPS", iops_text)
        
        # Message about no sub-resources
        no_sub_msg = Text("No sub-resources available for Windows file systems", style="dim italic")

        perf_parts = self._perf_parts(fs)
        parts = [header, pricing, Text(""), metrics_table, *perf_parts,
                 Text(""), no_sub_msg]
        content = Group(*parts)
        
        return Panel(
            content,
            title=f"FSx Windows Detail - {fs.id}",
            border_style="blue",
        )
    
    def run(self, refresh_callback: Optional[Callable[[], None]] = None) -> None:
        """Run the UI main loop with Live display.

        Args:
            refresh_callback: Optional callback for refresh events
        """
        self._running = True

        _is_win = sys.platform == 'win32'

        # Rich's Live(screen=True) owns the alternate screen buffer — no manual
        # ANSI toggles or terminal clears are needed.

        # Clear screen before drawing so a previous mode's output is gone.
        _clear_screen()

        try:
            with Live(
                self.render(),
                console=self._console,
                auto_refresh=False,
                screen=(not _VT_OK),
                vertical_overflow="visible",
            ) as live:
                # Set up keyboard handling in a separate thread
                if sys.platform == 'win32':
                    import msvcrt
                    import time as _wtime
                    # Prime the display
                    live.update(self.render(), refresh=True)
                    last_render = _wtime.monotonic()
                    while self._running:
                        dirty = False
                        if msvcrt.kbhit():
                            key = msvcrt.getwch()
                            if key == '\xe0' or key == '\x00':  # Special key prefix
                                key2 = msvcrt.getwch()
                                if key2 == 'H':    self.select_prev_volume(); dirty = True  # Up
                                elif key2 == 'P':  self.select_next_volume(); dirty = True  # Down
                                elif key2 == 'K':  self.prev_page(); dirty = True           # Left
                                elif key2 == 'M':  self.next_page(); dirty = True           # Right
                            elif key == 'q' or key == '\x03':
                                if self._volume_detail_mode:
                                    self.exit_volume_detail(); dirty = True
                                else:
                                    self._running = False; break
                            elif key == '\x1b':  # Esc
                                if self._volume_detail_mode:
                                    self.exit_volume_detail(); dirty = True
                                else:
                                    self._running = False; break
                            elif key == 'l':  self.next_page(); dirty = True
                            elif key == 'h':  self.prev_page(); dirty = True
                            elif key == 'j':  self.select_next_volume(); dirty = True
                            elif key == 'k':  self.select_prev_volume(); dirty = True
                            elif key == '\r':
                                if not self._volume_detail_mode:
                                    self.enter_volume_detail(); dirty = True
                        else:
                            _wtime.sleep(0.05)
                        now = _wtime.monotonic()
                        if dirty or (now - last_render) >= 1.0:
                            live.update(self.render(), refresh=True)
                            last_render = now
                else:
                    import select
                    import termios
                    import tty

                    old_settings = termios.tcgetattr(sys.stdin)
                    try:
                        tty.setcbreak(sys.stdin.fileno())
                        import os as _os
                        import time as _time
                        stdin_fd = sys.stdin.fileno()
                        buf = ''
                        esc_started_at = None

                        def handle_key(k: str) -> bool:
                            if k in ('LEFT',):
                                self.prev_page()
                            elif k in ('RIGHT',):
                                self.next_page()
                            elif k in ('UP',):
                                self.select_prev_volume()
                            elif k in ('DOWN',):
                                self.select_next_volume()
                            elif k == 'ESC':
                                if self._volume_detail_mode:
                                    self.exit_volume_detail()
                                else:
                                    return True
                            elif k == 'q' or k == '\x03':
                                if self._volume_detail_mode:
                                    self.exit_volume_detail()
                                else:
                                    return True
                            elif k == 'l':
                                self.next_page()
                            elif k == 'h':
                                self.prev_page()
                            elif k == 'j':
                                self.select_next_volume()
                            elif k == 'k':
                                self.select_prev_volume()
                            elif k in ('\r', '\n'):
                                if not self._volume_detail_mode:
                                    self.enter_volume_detail()
                            return False

                        ESC_TIMEOUT = 0.15

                        # Prime the display
                        live.update(self.render(), refresh=True)
                        last_tick = _time.monotonic()

                        while self._running:
                            dirty = False
                            if select.select([stdin_fd], [], [], 0.03)[0]:
                                try:
                                    chunk = _os.read(stdin_fd, 1024).decode('utf-8', errors='replace')
                                except OSError:
                                    chunk = ''
                                buf += chunk

                            while buf:
                                if buf[0] == '\x1b':
                                    if esc_started_at is None:
                                        esc_started_at = _time.monotonic()
                                    if len(buf) >= 3 and buf[1] in ('[', 'O'):
                                        third = buf[2]
                                        key_map = {'A': 'UP', 'B': 'DOWN', 'C': 'RIGHT', 'D': 'LEFT'}
                                        key = key_map.get(third, None)
                                        buf = buf[3:]
                                        esc_started_at = None
                                        if key is not None:
                                            if handle_key(key):
                                                self._running = False
                                                break
                                            dirty = True
                                        continue
                                    if len(buf) == 2 and buf[1] in ('[', 'O'):
                                        if select.select([stdin_fd], [], [], 0.05)[0]:
                                            try:
                                                more = _os.read(stdin_fd, 16).decode('utf-8', errors='replace')
                                            except OSError:
                                                more = ''
                                            if more:
                                                buf += more
                                                continue
                                    if _time.monotonic() - esc_started_at >= ESC_TIMEOUT:
                                        buf = buf[1:]
                                        esc_started_at = None
                                        if handle_key('ESC'):
                                            self._running = False
                                            break
                                        dirty = True
                                        continue
                                    break
                                else:
                                    ch = buf[0]
                                    buf = buf[1:]
                                    if handle_key(ch):
                                        self._running = False
                                        break
                                    dirty = True

                            if dirty or (_time.monotonic() - last_tick) >= 0.25:
                                live.update(self.render(), refresh=True)
                                last_tick = _time.monotonic()
                    finally:
                        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except Exception:
            # Fallback for non-TTY environments
            self._console.print(self.render())

    def stop(self) -> None:
        """Stop the UI loop."""
        self._running = False
