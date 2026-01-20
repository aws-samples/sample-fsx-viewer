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

from .model import Store, FileSystem, Stats, FileSystemType, DetailStore, Volume, MetadataServer


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
        elif field == "creation":
            return fs.creation_time
        else:
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
        progress = self.render_progress_bar(utilization, width=30)
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
        table.add_column("MB/s", width=10, justify="right")
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
        """Render help text."""
        stats = self._store.stats()
        total_pages = self._get_page_count(stats.total_file_systems)
        
        help_text = Text()
        help_text.append(f"Page {self._current_page + 1}/{total_pages}", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("j/k: select", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("Enter: details", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("h/l: page", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("q: quit", style="dim")
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
    
    def run(self, refresh_callback: Optional[Callable[[], None]] = None, manage_screen: bool = True) -> None:
        """Run the UI main loop with Live display.
        
        Args:
            refresh_callback: Optional callback for refresh events
            manage_screen: If True, manage alternate screen buffer. Set False when caller manages it.
        """
        self._running = True
        self._selected_fs_id = None
        
        # Switch to alternate screen buffer and move cursor to top (if managing screen)
        if manage_screen:
            sys.stdout.write('\033[?1049h')  # Switch to alternate screen
        sys.stdout.write('\033[H')       # Move cursor to top-left
        sys.stdout.write('\033[2J')      # Clear screen
        sys.stdout.flush()
        
        try:
            with Live(
                self.render_full(),
                console=self._console,
                refresh_per_second=2,
                vertical_overflow="visible",
            ) as live:
                # Set up keyboard handling in a separate thread
                import select
                import termios
                import tty
                
                old_settings = termios.tcgetattr(sys.stdin)
                try:
                    tty.setcbreak(sys.stdin.fileno())
                    
                    while self._running:
                        # Update display
                        live.update(self.render_full())
                        
                        # Check for keyboard input (non-blocking)
                        if select.select([sys.stdin], [], [], 0.5)[0]:
                            key = sys.stdin.read(1)
                            if key == 'q' or key == '\x03':  # q or Ctrl+C
                                self._running = False
                                break
                            elif key == 'j':  # j for select next (vim down)
                                self.select_next()
                            elif key == 'k':  # k for select previous (vim up)
                                self.select_prev()
                            elif key == '\r' or key == '\n':  # Enter to view details
                                selected = self._get_current_selection()
                                if selected:
                                    self._selected_fs_id = selected.id
                                    self._running = False
                                    break
                            elif key == 'l':  # l for next page
                                self.next_page()
                                self._selected_index = 0
                            elif key == 'h':  # h for previous page
                                self.prev_page()
                                self._selected_index = 0
                finally:
                    termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except Exception:
            # Fallback for non-TTY environments
            self._console.print(self.render_full())
        finally:
            # Restore original screen buffer (only if we're managing it)
            if manage_screen:
                sys.stdout.write('\033[?1049l')
                sys.stdout.flush()
    
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
    ):
        self._store = store
        self._style = style or Style()
        self._disable_pricing = disable_pricing
        self._page_size = page_size
        self._current_page = 0
        self._console = Console()
        self._running = False
        self._sort_key, self._sort_reverse = make_volume_sorter(sort)
    
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
        
        # Get total items based on file system type
        if fs.type in (FileSystemType.ONTAP, FileSystemType.OPENZFS):
            total_items = len(self._store.get_volumes())
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
        """Get volumes sorted according to sort spec."""
        volumes = self._store.get_volumes()
        return sorted(volumes, key=self._sort_key, reverse=self._sort_reverse)
    
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
    
    def _render_header(self, fs: FileSystem) -> Text:
        """Render the file system header with basic info."""
        header = Text()
        header.append(f"{fs.id}", style="bold cyan")
        header.append(" | ")
        header.append(f"{fs.type.value}", style="bold")
        header.append(" | ")
        header.append(f"{fs.storage_capacity} GiB", style="cyan")
        
        if fs.used_capacity > 0:
            utilization = fs.utilization() * 100
            header.append(f" ({utilization:.1f}% used)")
        
        return header
    
    def _render_fs_metrics(self, fs: FileSystem) -> Text:
        """Render file system level metrics."""
        metrics = Text()
        
        # Throughput
        throughput = fs.total_throughput()
        metrics.append("Throughput: ", style="dim")
        metrics.append(f"{throughput:.1f} MB/s" if throughput > 0 else "-")
        metrics.append(" | ")
        
        # IOPS
        iops = fs.total_iops()
        metrics.append("IOPS: ", style="dim")
        metrics.append(f"{iops:.0f}" if iops > 0 else "-")
        
        # CPU (if available)
        if fs.cpu_utilization > 0:
            metrics.append(" | ")
            metrics.append("CPU: ", style="dim")
            metrics.append(f"{fs.cpu_utilization:.0f}%")
        
        return metrics
    
    def _render_page_info(self, total_items: int) -> Text:
        """Render pagination info and help text."""
        total_pages = self._get_page_count(total_items)
        
        help_text = Text()
        help_text.append(f"Page {self._current_page + 1}/{total_pages}", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("h/l: page", style="dim")
        help_text.append(" • ", style="dim")
        help_text.append("q: quit", style="dim")
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
        table.add_column("MB/s (r/w)", width=14, justify="right")
        
        if not volumes:
            return table
        
        for vol in volumes:
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
            
            table.add_row(
                vol.id,
                vol.name,
                capacity_text,
                iops_str,
                throughput_str,
            )
        
        return table
    
    def _render_ontap_detail(self, fs: FileSystem) -> Panel:
        """Render ONTAP file system with volume table."""
        volumes = self._get_sorted_volumes()
        
        # Header section
        header = self._render_header(fs)
        
        # File system metrics
        metrics = self._render_fs_metrics(fs)
        
        # Volume table with pagination
        if volumes:
            total_pages = self._get_page_count(len(volumes))
            page_volumes = self._get_page_items(volumes)
            volume_table = self._render_volume_table(page_volumes, "ONTAP")
            
            # Page info
            page_info = self._render_page_info(len(volumes))
            content = Group(header, metrics, Text(""), volume_table, Text(""), page_info)
        else:
            content = Group(
                header,
                metrics,
                Text(""),
                Text("Discovering volumes...", style="dim italic"),
            )
        
        return Panel(
            content,
            title=f"FSx ONTAP Detail - {fs.id}",
            border_style="blue",
        )
    
    def _render_openzfs_detail(self, fs: FileSystem) -> Panel:
        """Render OpenZFS file system with volume table."""
        volumes = self._get_sorted_volumes()
        
        # Header section
        header = self._render_header(fs)
        
        # File system metrics
        metrics = self._render_fs_metrics(fs)
        
        # Volume table with pagination (pass fs capacity for volumes without quota)
        if volumes:
            total_pages = self._get_page_count(len(volumes))
            page_volumes = self._get_page_items(volumes)
            volume_table = self._render_volume_table(page_volumes, "OPENZFS", fs.storage_capacity)
            
            # Page info
            page_info = self._render_page_info(len(volumes))
            content = Group(header, metrics, Text(""), volume_table, Text(""), page_info)
        else:
            content = Group(
                header,
                metrics,
                Text(""),
                Text("Discovering volumes...", style="dim italic"),
            )
        
        return Panel(
            content,
            title=f"FSx OpenZFS Detail - {fs.id}",
            border_style="blue",
        )
    
    def _render_lustre_detail(self, fs: FileSystem) -> Panel:
        """Render Lustre file system with MDS CPU breakdown."""
        mds_servers = self._store.get_mds_servers()
        
        # Header with file system info
        header = self._render_header(fs)
        
        # File system metrics
        metrics = self._render_fs_metrics(fs)
        
        # MDS table
        mds_table = Table(
            show_header=True,
            header_style="bold",
            border_style="dim",
            expand=True,
        )
        
        mds_table.add_column("MDS ID", style="cyan", no_wrap=True, min_width=10)
        mds_table.add_column("CPU (%)", width=40)  # 30 bar + text
        
        if mds_servers:
            # Paginate MDS servers
            page_mds = self._get_page_items(mds_servers)
            
            for mds in page_mds:
                # CPU with gradient progress bar (width=30 for smoother gradient)
                cpu_pct = mds.cpu_utilization / 100.0  # Convert to 0-1 range
                cpu_bar = self._render_progress_bar(cpu_pct, width=30, gradient=True)
                cpu_text = Text()
                cpu_text.append_text(cpu_bar)
                cpu_text.append(f" {mds.cpu_utilization:.1f}%")
                
                mds_table.add_row(mds.id, cpu_text)
            
            # Page info
            page_info = self._render_page_info(len(mds_servers))
            content = Group(header, metrics, Text(""), mds_table, Text(""), page_info)
        else:
            content = Group(
                header,
                metrics,
                Text(""),
                Text("Discovering MDS servers...", style="dim italic"),
            )
        
        return Panel(
            content,
            title=f"FSx Lustre Detail - {fs.id}",
            border_style="blue",
        )
    
    def _render_windows_detail(self, fs: FileSystem) -> Panel:
        """Render Windows file system (no sub-resources)."""
        # Header with file system info
        header = self._render_header(fs)
        
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
        throughput_text = Text(f"{throughput:.1f} MB/s" if throughput > 0 else "-")
        metrics_table.add_row("Throughput", throughput_text)
        
        # IOPS
        iops = fs.total_iops()
        iops_text = Text(f"{iops:.0f}" if iops > 0 else "-")
        metrics_table.add_row("IOPS", iops_text)
        
        # Message about no sub-resources
        no_sub_msg = Text("No sub-resources available for Windows file systems", style="dim italic")
        
        content = Group(header, Text(""), metrics_table, Text(""), no_sub_msg)
        
        return Panel(
            content,
            title=f"FSx Windows Detail - {fs.id}",
            border_style="blue",
        )
    
    def run(self, refresh_callback: Optional[Callable[[], None]] = None, manage_screen: bool = True) -> None:
        """Run the UI main loop with Live display.
        
        Args:
            refresh_callback: Optional callback for refresh events
            manage_screen: If True, manage alternate screen buffer. Set False when caller manages it.
        """
        self._running = True
        
        # Switch to alternate screen buffer and move cursor to top (if managing screen)
        if manage_screen:
            sys.stdout.write('\033[?1049h')  # Switch to alternate screen
        sys.stdout.write('\033[H')       # Move cursor to top-left
        sys.stdout.write('\033[2J')      # Clear screen
        sys.stdout.flush()
        
        try:
            with Live(
                self.render(),
                console=self._console,
                refresh_per_second=2,
                vertical_overflow="visible",
            ) as live:
                # Set up keyboard handling in a separate thread
                import select
                import termios
                import tty
                
                old_settings = termios.tcgetattr(sys.stdin)
                try:
                    tty.setcbreak(sys.stdin.fileno())
                    
                    while self._running:
                        # Update display
                        live.update(self.render())
                        
                        # Check for keyboard input (non-blocking)
                        if select.select([sys.stdin], [], [], 0.5)[0]:
                            key = sys.stdin.read(1)
                            if key == 'q' or key == '\x03':  # q or Ctrl+C
                                self._running = False
                                break
                            elif key == 'l':  # l for next page
                                self.next_page()
                            elif key == 'h':  # h for previous page
                                self.prev_page()
                finally:
                    termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except Exception:
            # Fallback for non-TTY environments
            self._console.print(self.render())
        finally:
            # Restore original screen buffer (only if we're managing it)
            if manage_screen:
                sys.stdout.write('\033[?1049l')
                sys.stdout.flush()
    
    def stop(self) -> None:
        """Stop the UI loop."""
        self._running = False
