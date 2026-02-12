#!/usr/bin/env python3
"""
Build optimized MAME ROM SQLite database from ProgettoSnaps DAT files.

Downloads the latest MAME DAT archive and creates an optimized SQLite database with:
- ROM size limits: 2KB min, 8MB max, power of 2 only
- Normalized tables: manufacturers, machines, roms, rom_names, machine_roms
- Binary SHA1/CRC storage
- Compressed descriptions (zlib)
- Neo-Geo BIOS ROMs in separate entry
- Orphan reference cleanup

Usage:
    python3 build_mame_database.py [options]

Options:
    --no-download   Skip download, use existing DAT files in ROM/DATs/
    --dat-file      Specify a single DAT file to process
    --add-dat       Add DAT file to existing database (requires --dat-file)
    --output        Output database path (default: mame_roms.db)
    --keep-temp     Keep downloaded temporary files

Requirements:
    - Python 3.6+
    - py7zr module for 7z extraction: pip install py7zr
"""

import sqlite3
import xml.etree.ElementTree as ET
import zlib
import math
import os
import sys
import re
import argparse
import tempfile
import zipfile
import shutil
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError
from html.parser import HTMLParser

# Try to import py7zr for 7z support
try:
    import py7zr
    HAS_7Z = True
except ImportError:
    HAS_7Z = False


# ============================================================================
# Constants
# ============================================================================

MIN_ROM_SIZE = 256               # 256 bytes minimum
MAX_ROM_SIZE = 8 * 1024 * 1024   # 8 MB maximum
NEOGEO_BIOS_THRESHOLD = 4000    # ROMs in more machines than this are considered BIOS

DAT_INDEX_URL = "https://www.progettosnaps.net/dats/MAME/"
DAT_DOWNLOAD_BASE = "https://www.progettosnaps.net"

USER_AGENT = "Mozilla/5.0 (compatible; MAME-DB-Builder/1.0)"


# ============================================================================
# Download Functions
# ============================================================================

class DATLinkParser(HTMLParser):
    """Parse HTML to find MAME DAT download links."""

    def __init__(self):
        super().__init__()
        self.dat_links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            href = dict(attrs).get('href', '')
            # Look for MAME_Dats archives only (not diff files)
            if 'MAME_Dats' in href and '.7z' in href:
                self.dat_links.append(href)


def extract_version(url):
    """Extract MAME version number from URL.

    Handles patterns like:
    - MAME_Dats_284.7z -> 284
    - MAME_Dats_0284.7z -> 284
    - MAME_Dats_037b1.7z -> 37 (old beta format)
    - MAME_0.284.7z -> 284
    """
    # Look for version number pattern: digits possibly followed by letter suffix
    # Match the last occurrence of a version-like pattern before .7z or .zip
    match = re.search(r'MAME[_\s]*(?:Dats[_\s]*)?0?(\d+)(?:b\d+)?(?:\.7z|\.zip)', url, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except:
            return 0
    return 0


def get_latest_dat_url():
    """Find the latest MAME DAT archive URL from ProgettoSnaps."""
    print("Fetching DAT file list from ProgettoSnaps...")

    try:
        req = Request(DAT_INDEX_URL, headers={'User-Agent': USER_AGENT})
        with urlopen(req, timeout=30) as response:
            html = response.read().decode('utf-8', errors='replace')
    except URLError as e:
        print(f"Error fetching index: {e}")
        return None

    parser = DATLinkParser()
    parser.feed(html)

    if not parser.dat_links:
        print("No DAT links found on page")
        return None

    # Extract versions and find the maximum
    versioned_links = []
    for link in parser.dat_links:
        version = extract_version(link)
        if version > 0:
            versioned_links.append((version, link))

    if not versioned_links:
        print("No valid version numbers found in links")
        return None

    # Sort by version descending and get the latest
    versioned_links.sort(key=lambda x: x[0], reverse=True)
    latest_version, latest = versioned_links[0]
    print(f"Found {len(versioned_links)} MAME DAT versions, selecting latest: {latest_version}")

    # Build full URL
    if latest.startswith('download/'):
        match = re.search(r'file=([^&]+)', latest)
        if match:
            file_path = match.group(1)
            return f"{DAT_DOWNLOAD_BASE}{file_path}"
        return f"{DAT_DOWNLOAD_BASE}/dats/MAME/{latest}"
    elif latest.startswith('/'):
        return f"{DAT_DOWNLOAD_BASE}{latest}"
    elif latest.startswith('http'):
        return latest
    else:
        return f"{DAT_DOWNLOAD_BASE}/dats/MAME/{latest}"


def download_dat_archive(url, output_dir):
    """Download and extract DAT archive (ZIP or 7z)."""
    print(f"Downloading: {url}")

    try:
        req = Request(url, headers={'User-Agent': USER_AGENT})
        with urlopen(req, timeout=300) as response:
            archive_data = response.read()
    except URLError as e:
        print(f"Download error: {e}")
        return []

    print(f"Downloaded {len(archive_data) / 1024 / 1024:.1f} MB")

    # Determine archive type
    is_7z = url.endswith('.7z') or archive_data[:6] == b"7z\xbc\xaf'\x1c"

    if is_7z:
        if not HAS_7Z:
            print("Error: 7z archive detected but py7zr module not installed")
            print("Install with: pip install py7zr")
            return []
        return extract_7z(archive_data, output_dir)
    else:
        return extract_zip(archive_data, output_dir)


def extract_7z(data, output_dir):
    """Extract .7z archive and return list of XML DAT files.

    Prefers files from XMLs/ folder (proper XML format) over
    ClrMAME or ROMCenter formats which are not valid XML.
    """
    archive_path = os.path.join(output_dir, "mame_dats.7z")
    with open(archive_path, 'wb') as f:
        f.write(data)

    dat_files = []
    try:
        with py7zr.SevenZipFile(archive_path, mode='r') as archive:
            all_files = archive.getnames()

            # Prefer XML files from XMLs folder
            xml_files = [f for f in all_files if 'XMLs/' in f and f.lower().endswith('.xml')]
            if xml_files:
                print(f"Found {len(xml_files)} XML files in archive")
                archive.extractall(path=output_dir)
                for xml_name in xml_files:
                    xml_path = os.path.join(output_dir, xml_name)
                    if os.path.exists(xml_path):
                        dat_files.append(xml_path)
            else:
                # Fallback to DAT files if no XMLs folder
                dat_names = [f for f in all_files if f.lower().endswith('.dat')]
                print(f"Found {len(dat_names)} DAT files in archive")
                archive.extractall(path=output_dir)
                for dat_name in dat_names:
                    dat_path = os.path.join(output_dir, dat_name)
                    if os.path.exists(dat_path):
                        dat_files.append(dat_path)

    except Exception as e:
        print(f"Error extracting 7z: {e}")

    return dat_files


def extract_zip(data, output_dir):
    """Extract .zip archive and return list of XML DAT files.

    Prefers files from XMLs/ folder (proper XML format) over
    ClrMAME or ROMCenter formats which are not valid XML.
    """
    archive_path = os.path.join(output_dir, "mame_dats.zip")
    with open(archive_path, 'wb') as f:
        f.write(data)

    dat_files = []
    try:
        with zipfile.ZipFile(archive_path, 'r') as zf:
            all_files = zf.namelist()

            # Prefer XML files from XMLs folder
            xml_files = [f for f in all_files if 'XMLs/' in f and f.lower().endswith('.xml')]
            if xml_files:
                print(f"Found {len(xml_files)} XML files in archive")
                for xml_name in xml_files:
                    zf.extract(xml_name, output_dir)
                    xml_path = os.path.join(output_dir, xml_name)
                    if os.path.exists(xml_path):
                        dat_files.append(xml_path)
            else:
                # Fallback to DAT files if no XMLs folder
                dat_names = [f for f in all_files if f.lower().endswith('.dat')]
                print(f"Found {len(dat_names)} DAT files in archive")
                for dat_name in dat_names:
                    zf.extract(dat_name, output_dir)
                    dat_path = os.path.join(output_dir, dat_name)
                    if os.path.exists(dat_path):
                        dat_files.append(dat_path)

    except zipfile.BadZipFile:
        print("Invalid ZIP file")

    return dat_files


# ============================================================================
# Database Creation Functions
# ============================================================================

def create_database(db_path):
    """Create SQLite database with optimized schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop existing tables
    for table in ['machine_roms', 'machines', 'roms', 'rom_names', 'manufacturers']:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

    # Create tables
    cursor.execute("""
        CREATE TABLE manufacturers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    """)

    cursor.execute("""
        CREATE TABLE rom_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE roms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sha1 BLOB,
            crc BLOB,
            size_pow2 INTEGER,
            name_id INTEGER REFERENCES rom_names(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE machines (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            cloneof_id INTEGER,
            romof_id INTEGER,
            description BLOB,
            year INTEGER CHECK(year >= 0 AND year <= 65535),
            manufacturer_id INTEGER REFERENCES manufacturers(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE machine_roms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER NOT NULL,
            rom_id INTEGER NOT NULL,
            name_id INTEGER REFERENCES rom_names(id),
            FOREIGN KEY (machine_id) REFERENCES machines(id),
            FOREIGN KEY (rom_id) REFERENCES roms(id)
        )
    """)

    conn.commit()
    return conn


def open_existing_database(db_path):
    """Open existing database and load caches."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    required = {'manufacturers', 'rom_names', 'roms', 'machines', 'machine_roms'}

    if not required.issubset(tables):
        raise ValueError(f"Database missing required tables. Found: {tables}")

    return conn


def load_caches_from_db(conn):
    """Load existing data into caches for incremental updates."""
    cursor = conn.cursor()

    # Load manufacturers
    manufacturers_cache = {}
    cursor.execute("SELECT id, name FROM manufacturers")
    for row in cursor.fetchall():
        manufacturers_cache[row[1]] = row[0]

    # Load rom_names
    rom_names_cache = {}
    cursor.execute("SELECT id, name FROM rom_names")
    for row in cursor.fetchall():
        rom_names_cache[row[1]] = row[0]

    # Load roms (by SHA1 hex string)
    roms_cache = {}
    cursor.execute("SELECT id, sha1, name_id FROM roms")
    for row in cursor.fetchall():
        if row[1]:
            sha1_hex = row[1].hex()
            roms_cache[sha1_hex] = (row[0], row[2])

    # Load existing machine names
    existing_machines = set()
    cursor.execute("SELECT name FROM machines")
    for row in cursor.fetchall():
        existing_machines.add(row[0])

    # Get max machine ID
    cursor.execute("SELECT MAX(id) FROM machines")
    max_id = cursor.fetchone()[0] or 0

    return manufacturers_cache, rom_names_cache, roms_cache, existing_machines, max_id


def is_valid_rom_size(size):
    """Check if ROM size is valid (power of 2, within limits)."""
    if size is None or size < MIN_ROM_SIZE or size > MAX_ROM_SIZE:
        return False
    # Check if power of 2
    return size > 0 and (size & (size - 1)) == 0


def parse_machine(machine_elem):
    """Extract machine data and ROMs from XML element."""
    attrs = {
        'name': machine_elem.get('name'),
        'cloneof': machine_elem.get('cloneof'),
        'romof': machine_elem.get('romof'),
    }

    # Child elements
    desc_elem = machine_elem.find('description')
    attrs['description'] = desc_elem.text if desc_elem is not None else None

    year_elem = machine_elem.find('year')
    if year_elem is not None and year_elem.text:
        try:
            attrs['year'] = int(year_elem.text[:4])
        except:
            attrs['year'] = None
    else:
        attrs['year'] = None

    manuf_elem = machine_elem.find('manufacturer')
    attrs['manufacturer'] = manuf_elem.text if manuf_elem is not None else None

    # Extract ROMs (filter by size)
    roms = []
    for rom_elem in machine_elem.findall('rom'):
        size_str = rom_elem.get('size')
        try:
            size = int(size_str) if size_str else None
        except:
            size = None

        if not is_valid_rom_size(size):
            continue

        sha1 = rom_elem.get('sha1')
        if not sha1:  # Skip ROMs without SHA1 (nodump)
            continue

        rom = {
            'name': rom_elem.get('name'),
            'size': size,
            'crc': rom_elem.get('crc'),
            'sha1': sha1,
        }
        roms.append(rom)

    return attrs, roms


def process_dat_file(dat_path, conn, manufacturers_cache, rom_names_cache,
                     roms_cache, existing_machines, start_machine_id):
    """Process DAT file and populate database."""
    print(f"Processing: {os.path.basename(dat_path)}")
    cursor = conn.cursor()

    machines_data = []        # [(id, name, cloneof, romof, desc, year, manuf_id)]
    machine_roms_data = []    # [(machine_id, rom_id, name_id)]

    machine_id = start_machine_id
    machines_added = 0
    machines_skipped = 0
    roms_added = 0

    # Parse XML
    try:
        context = ET.iterparse(dat_path, events=('end',))
    except Exception as e:
        print(f"  Error opening file: {e}")
        return 0, 0, machine_id

    for event, elem in context:
        if elem.tag not in ('machine', 'game'):
            continue

        machine_attrs, roms = parse_machine(elem)

        # Skip machines without valid ROMs or name
        if not roms or not machine_attrs['name']:
            elem.clear()
            continue

        # Skip existing machines (first file wins)
        if machine_attrs['name'] in existing_machines:
            machines_skipped += 1
            elem.clear()
            continue

        machine_id += 1
        existing_machines.add(machine_attrs['name'])

        # Get/create manufacturer
        manuf_name = machine_attrs['manufacturer']
        if manuf_name:
            if manuf_name not in manufacturers_cache:
                cursor.execute("INSERT INTO manufacturers (name) VALUES (?)", (manuf_name,))
                manufacturers_cache[manuf_name] = cursor.lastrowid
            manuf_id = manufacturers_cache[manuf_name]
        else:
            manuf_id = None

        # Compress description
        desc = machine_attrs['description']
        if desc:
            desc_blob = zlib.compress(desc.encode('utf-8'), level=9)
        else:
            desc_blob = None

        # Store machine data (cloneof/romof resolved later)
        machines_data.append((
            machine_id,
            machine_attrs['name'],
            machine_attrs['cloneof'],
            machine_attrs['romof'],
            desc_blob,
            machine_attrs['year'],
            manuf_id
        ))

        # Process ROMs
        for rom in roms:
            sha1 = rom['sha1']
            rom_name = rom['name']

            # Get/create rom_name
            if rom_name not in rom_names_cache:
                cursor.execute("INSERT INTO rom_names (name) VALUES (?)", (rom_name,))
                rom_names_cache[rom_name] = cursor.lastrowid
            name_id = rom_names_cache[rom_name]

            # Get/create ROM by SHA1
            if sha1 not in roms_cache:
                sha1_bin = bytes.fromhex(sha1)
                crc_bin = bytes.fromhex(rom['crc']) if rom['crc'] else None
                size_pow2 = int(math.log2(rom['size']))

                cursor.execute("""
                    INSERT INTO roms (sha1, crc, size_pow2, name_id)
                    VALUES (?, ?, ?, ?)
                """, (sha1_bin, crc_bin, size_pow2, name_id))

                roms_cache[sha1] = (cursor.lastrowid, name_id)
                roms_added += 1

            rom_id, _ = roms_cache[sha1]
            machine_roms_data.append((machine_id, rom_id, name_id))

        machines_added += 1
        if machines_added % 10000 == 0:
            print(f"    {machines_added} machines, {roms_added} unique ROMs...")

        elem.clear()

    print(f"    Added: {machines_added} machines, {roms_added} ROMs | Skipped: {machines_skipped} duplicates")

    # Create name -> id mapping for cloneof/romof resolution
    name_to_id = {m[1]: m[0] for m in machines_data}

    # Also load existing machine name -> id mappings
    cursor.execute("SELECT id, name FROM machines")
    for row in cursor.fetchall():
        name_to_id[row[1]] = row[0]

    # Insert machines with resolved references
    for m in machines_data:
        id_, name, cloneof, romof, desc, year, manuf_id = m
        cloneof_id = name_to_id.get(cloneof)
        romof_id = name_to_id.get(romof)
        cursor.execute("""
            INSERT INTO machines (id, name, cloneof_id, romof_id, description, year, manufacturer_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (id_, name, cloneof_id, romof_id, desc, year, manuf_id))

    # Insert machine_roms
    cursor.executemany("""
        INSERT INTO machine_roms (machine_id, rom_id, name_id)
        VALUES (?, ?, ?)
    """, machine_roms_data)

    conn.commit()
    return machines_added, roms_added, machine_id


def create_indexes(conn):
    """Create database indexes."""
    print("Creating indexes...")
    cursor = conn.cursor()

    # Check if indexes exist first
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    existing = {row[0] for row in cursor.fetchall()}

    indexes = [
        ("idx_rom_names_name", "CREATE INDEX idx_rom_names_name ON rom_names(name)"),
        ("idx_roms_sha1", "CREATE INDEX idx_roms_sha1 ON roms(sha1)"),
        ("idx_roms_crc", "CREATE INDEX idx_roms_crc ON roms(crc)"),
        ("idx_roms_name_id", "CREATE INDEX idx_roms_name_id ON roms(name_id)"),
        ("idx_machines_name", "CREATE INDEX idx_machines_name ON machines(name)"),
        ("idx_machines_cloneof_id", "CREATE INDEX idx_machines_cloneof_id ON machines(cloneof_id)"),
        ("idx_machines_romof_id", "CREATE INDEX idx_machines_romof_id ON machines(romof_id)"),
        ("idx_machines_manufacturer_id", "CREATE INDEX idx_machines_manufacturer_id ON machines(manufacturer_id)"),
        ("idx_machine_roms_machine", "CREATE INDEX idx_machine_roms_machine ON machine_roms(machine_id)"),
        ("idx_machine_roms_rom", "CREATE INDEX idx_machine_roms_rom ON machine_roms(rom_id)"),
        ("idx_machine_roms_name_id", "CREATE INDEX idx_machine_roms_name_id ON machine_roms(name_id)"),
    ]

    for idx_name, idx_sql in indexes:
        if idx_name not in existing:
            cursor.execute(idx_sql)

    conn.commit()


def drop_indexes(conn):
    """Drop indexes for faster bulk inserts."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
    for (name,) in cursor.fetchall():
        cursor.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()


def extract_neogeo_bios(conn):
    """Move Neo-Geo BIOS ROMs to separate entry."""
    print("Extracting Neo-Geo BIOS ROMs...")
    cursor = conn.cursor()

    # Check if neogeo_bios already exists
    cursor.execute("SELECT id FROM machines WHERE name = 'neogeo_bios'")
    if cursor.fetchone():
        print("  neogeo_bios entry already exists, skipping")
        return

    # Find ROMs with more than threshold occurrences
    cursor.execute(f"""
        SELECT r.id, COUNT(mr.machine_id) as cnt
        FROM machine_roms mr
        JOIN roms r ON mr.rom_id = r.id
        GROUP BY mr.rom_id
        HAVING cnt > {NEOGEO_BIOS_THRESHOLD}
    """)
    bios_roms = cursor.fetchall()

    if not bios_roms:
        print("  No BIOS ROMs found")
        return

    bios_rom_ids = [r[0] for r in bios_roms]
    print(f"  Found {len(bios_rom_ids)} BIOS ROMs")

    # Get/create SNK manufacturer
    cursor.execute("SELECT id FROM manufacturers WHERE name = 'SNK'")
    row = cursor.fetchone()
    if row:
        snk_id = row[0]
    else:
        cursor.execute("INSERT INTO manufacturers (name) VALUES ('SNK')")
        snk_id = cursor.lastrowid

    # Create neogeo_bios machine entry
    desc = zlib.compress(
        "Neo-Geo BIOS ROMs - Shared system ROMs for all Neo-Geo games (MVS/AES)".encode('utf-8'),
        level=9
    )

    cursor.execute("SELECT MAX(id) FROM machines")
    max_id = cursor.fetchone()[0] or 0
    neogeo_id = max_id + 1

    cursor.execute("""
        INSERT INTO machines (id, name, cloneof_id, romof_id, description, year, manufacturer_id)
        VALUES (?, 'neogeo_bios', NULL, NULL, ?, 1990, ?)
    """, (neogeo_id, desc, snk_id))

    # Count current links
    placeholders = ','.join('?' * len(bios_rom_ids))
    cursor.execute(f"SELECT COUNT(*) FROM machine_roms WHERE rom_id IN ({placeholders})", bios_rom_ids)
    links_before = cursor.fetchone()[0]

    # Add links to neogeo_bios
    for rom_id in bios_rom_ids:
        cursor.execute("SELECT name_id FROM machine_roms WHERE rom_id = ? LIMIT 1", (rom_id,))
        name_id = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO machine_roms (machine_id, rom_id, name_id)
            VALUES (?, ?, ?)
        """, (neogeo_id, rom_id, name_id))

    # Remove BIOS links from other machines
    cursor.execute(f"""
        DELETE FROM machine_roms
        WHERE rom_id IN ({placeholders})
        AND machine_id != ?
    """, bios_rom_ids + [neogeo_id])

    links_deleted = cursor.rowcount
    print(f"  Created neogeo_bios entry, removed {links_deleted} duplicate links")

    conn.commit()


def cleanup_orphans(conn):
    """Remove machines without ROMs and clean orphan references."""
    print("Cleaning up orphans...")
    cursor = conn.cursor()

    # Remove machines without ROMs
    cursor.execute("""
        DELETE FROM machines
        WHERE id NOT IN (SELECT DISTINCT machine_id FROM machine_roms)
    """)
    machines_deleted = cursor.rowcount
    print(f"  Removed {machines_deleted} machines without ROMs")

    # Remove orphan ROM names
    cursor.execute("""
        DELETE FROM rom_names
        WHERE id NOT IN (SELECT name_id FROM roms WHERE name_id IS NOT NULL)
        AND id NOT IN (SELECT name_id FROM machine_roms WHERE name_id IS NOT NULL)
    """)
    names_deleted = cursor.rowcount
    print(f"  Removed {names_deleted} orphan ROM names")

    # Remove orphan manufacturers
    cursor.execute("""
        DELETE FROM manufacturers
        WHERE id NOT IN (SELECT manufacturer_id FROM machines WHERE manufacturer_id IS NOT NULL)
    """)
    manuf_deleted = cursor.rowcount
    print(f"  Removed {manuf_deleted} orphan manufacturers")

    # Clean orphan cloneof_id/romof_id references
    cursor.execute("""
        UPDATE machines SET cloneof_id = NULL
        WHERE cloneof_id IS NOT NULL
        AND cloneof_id NOT IN (SELECT id FROM machines)
    """)
    print(f"  Cleaned {cursor.rowcount} orphan cloneof references")

    cursor.execute("""
        UPDATE machines SET romof_id = NULL
        WHERE romof_id IS NOT NULL
        AND romof_id NOT IN (SELECT id FROM machines)
    """)
    print(f"  Cleaned {cursor.rowcount} orphan romof references")

    conn.commit()


def optimize_database(conn):
    """Run VACUUM to optimize database file."""
    print("Optimizing database...")
    conn.execute("VACUUM")


def print_stats(conn, db_path):
    """Print database statistics."""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("DATABASE STATISTICS")
    print("=" * 60)

    tables = ['manufacturers', 'rom_names', 'roms', 'machines', 'machine_roms']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:<20} {count:>12,} entries")

    db_size = os.path.getsize(db_path)
    print(f"\n  Database size: {db_size / 1024 / 1024:.2f} MB")
    print("=" * 60)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Build MAME ROM SQLite database from ProgettoSnaps DAT files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download and build from latest online archive
  python3 build_mame_database.py

  # Build from a specific DAT file
  python3 build_mame_database.py --dat-file MAME.dat

  # Add a DAT file to existing database
  python3 build_mame_database.py --add-dat --dat-file extra.dat

  # Build from local DAT files (no download)
  python3 build_mame_database.py --no-download

Requirements:
  For 7z archive support: pip install py7zr
"""
    )
    parser.add_argument('--no-download', action='store_true',
                        help="Skip download, use existing DAT files in ROM/DATs/")
    parser.add_argument('--dat-file', type=str,
                        help="Path to a specific DAT file to process")
    parser.add_argument('--add-dat', action='store_true',
                        help="Add DAT file to existing database (use with --dat-file)")
    parser.add_argument('--output', type=str, default='mame_roms.db',
                        help="Output database path (default: mame_roms.db)")
    parser.add_argument('--keep-temp', action='store_true',
                        help="Keep downloaded temporary files")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    db_path = script_dir / args.output

    # Validate arguments
    if args.add_dat and not args.dat_file:
        print("Error: --add-dat requires --dat-file")
        sys.exit(1)

    # Check py7zr availability
    if not HAS_7Z and not args.no_download and not args.dat_file:
        print("Warning: py7zr module not installed")
        print("Install with: pip install py7zr")
        print("Or use --dat-file to specify a pre-extracted DAT file")
        sys.exit(1)

    # Mode: Add to existing database
    if args.add_dat:
        if not os.path.exists(db_path):
            print(f"Error: Database not found: {db_path}")
            print("Create a database first without --add-dat")
            sys.exit(1)

        if not os.path.exists(args.dat_file):
            print(f"Error: DAT file not found: {args.dat_file}")
            sys.exit(1)

        print(f"Adding to existing database: {db_path}")
        conn = open_existing_database(str(db_path))

        # Load existing caches
        print("Loading existing data...")
        manufacturers_cache, rom_names_cache, roms_cache, existing_machines, max_id = \
            load_caches_from_db(conn)
        print(f"  Loaded: {len(existing_machines)} machines, {len(roms_cache)} ROMs")

        # Drop indexes for faster insert
        drop_indexes(conn)

        # Process DAT file
        machines, roms, _ = process_dat_file(
            args.dat_file, conn,
            manufacturers_cache, rom_names_cache, roms_cache,
            existing_machines, max_id
        )

        # Recreate indexes
        create_indexes(conn)
        cleanup_orphans(conn)
        optimize_database(conn)
        print_stats(conn, str(db_path))
        conn.close()
        print(f"\nDatabase updated: {db_path}")
        return

    # Mode: Build from specific DAT file
    if args.dat_file:
        if not os.path.exists(args.dat_file):
            print(f"Error: DAT file not found: {args.dat_file}")
            sys.exit(1)

        print(f"\nCreating database: {db_path}")
        conn = create_database(str(db_path))

        manufacturers_cache = {}
        rom_names_cache = {}
        roms_cache = {}
        existing_machines = set()

        machines, roms, _ = process_dat_file(
            args.dat_file, conn,
            manufacturers_cache, rom_names_cache, roms_cache,
            existing_machines, 0
        )

        create_indexes(conn)
        extract_neogeo_bios(conn)
        cleanup_orphans(conn)
        optimize_database(conn)
        print_stats(conn, str(db_path))
        conn.close()
        print(f"\nDatabase created: {db_path}")
        return

    # Mode: No download - use local files
    if args.no_download:
        dat_dir = script_dir / "ROM" / "DATs"
        if not dat_dir.exists():
            dat_dir = script_dir

        dat_files = list(dat_dir.glob("**/*.dat")) + list(dat_dir.glob("**/*.DAT"))
        if not dat_files:
            print(f"Error: No DAT files found in {dat_dir}")
            print("Use --dat-file to specify a file or remove --no-download")
            sys.exit(1)

        print(f"Found {len(dat_files)} DAT files")
        print(f"\nCreating database: {db_path}")
        conn = create_database(str(db_path))

        manufacturers_cache = {}
        rom_names_cache = {}
        roms_cache = {}
        existing_machines = set()
        machine_id = 0

        for dat_file in sorted(dat_files):
            machines, roms, machine_id = process_dat_file(
                str(dat_file), conn,
                manufacturers_cache, rom_names_cache, roms_cache,
                existing_machines, machine_id
            )

        create_indexes(conn)
        extract_neogeo_bios(conn)
        cleanup_orphans(conn)
        optimize_database(conn)
        print_stats(conn, str(db_path))
        conn.close()
        print(f"\nDatabase created: {db_path}")
        return

    # Mode: Download from ProgettoSnaps
    dat_url = get_latest_dat_url()
    if not dat_url:
        print("Error: Could not find DAT download URL")
        print("Try specifying --dat-file manually")
        sys.exit(1)

    # Use temp directory or specified location
    if args.keep_temp:
        temp_dir = str(script_dir / "temp_dats")
        os.makedirs(temp_dir, exist_ok=True)
        cleanup_temp = False
    else:
        temp_dir = tempfile.mkdtemp(prefix="mame_dats_")
        cleanup_temp = True

    try:
        # Download and extract archive
        dat_files = download_dat_archive(dat_url, temp_dir)

        if not dat_files:
            print("Error: No DAT files extracted")
            sys.exit(1)

        print(f"\nExtracted {len(dat_files)} DAT files")
        print(f"Creating database: {db_path}")

        conn = create_database(str(db_path))

        manufacturers_cache = {}
        rom_names_cache = {}
        roms_cache = {}
        existing_machines = set()
        machine_id = 0

        # Sort DAT files - process main MAME DAT first
        def sort_key(path):
            name = os.path.basename(path).lower()
            if 'mame' in name and 'mess' not in name:
                return (0, name)
            return (1, name)

        dat_files.sort(key=sort_key)

        # Process all DAT files
        print("\n" + "-" * 60)
        for dat_file in dat_files:
            machines, roms, machine_id = process_dat_file(
                dat_file, conn,
                manufacturers_cache, rom_names_cache, roms_cache,
                existing_machines, machine_id
            )
        print("-" * 60)

        create_indexes(conn)
        extract_neogeo_bios(conn)
        cleanup_orphans(conn)
        optimize_database(conn)
        print_stats(conn, str(db_path))
        conn.close()
        print(f"\nDatabase created: {db_path}")

    finally:
        if cleanup_temp and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()
