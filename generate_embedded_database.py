#!/usr/bin/env python3
"""
Generate embedded ROM database for Raspberry Pi Pico 2.

Extracts data from the SQLite MAME database and creates an optimized binary
file for embedding in the arcade_rom_pro firmware.

ID sizes:
- roms: 24 bits (8-bit size_pow2 + 16-bit index, SHA1 sorted)
- machine_roms: 24 bits
- rom_names: 24 bits
- manufacturers: 16 bits
- machines: 24 bits
- year: 16 bits

Binary file structure:
1. Header (fixed size)
2. Size index table (for fast ROM lookup by size)
3. ROMs table (sorted by size_pow2, then SHA1)
4. Machines table
5. Machine-ROMs mapping (sorted by rom_id)
6. Manufacturers table
7. ROM names table
8. Strings pool
"""

import sqlite3
import struct
import zlib
import os
import sys
import argparse
from pathlib import Path
from collections import defaultdict

# ============================================================================
# Configuration - Modify these values as needed
# ============================================================================

# Default values (can be overridden via command line)
DEFAULT_MIN_SIZE_POW2 = 11   # 2^11 = 2 KB
DEFAULT_MAX_SIZE_POW2 = 23  # 2^23 = 8 MB

# ============================================================================
# Constants
# ============================================================================

MAGIC = b'MRDB'
VERSION = 1

NULL_ID_24 = 0xFFFFFF  # NULL value for 24-bit IDs
NULL_ID_16 = 0xFFFF    # NULL value for 16-bit IDs

# Header structure (64 bytes)
HEADER_SIZE = 64

# ============================================================================
# Helper functions
# ============================================================================

def write_uint24(value):
    """Pack a 24-bit unsigned integer (little-endian)."""
    return struct.pack('<I', value)[:3]


def read_uint24(data, offset):
    """Unpack a 24-bit unsigned integer (little-endian)."""
    return struct.unpack('<I', data[offset:offset+3] + b'\x00')[0]


def make_rom_id(size_pow2, index):
    """Create ROM ID: size_pow2 in high 8 bits, index in low 16 bits."""
    if index > 0xFFFF:
        raise ValueError(f"ROM index {index} exceeds 16-bit limit for size_pow2={size_pow2}")
    return (size_pow2 << 16) | index


def extract_size_pow2(rom_id):
    """Extract size_pow2 from ROM ID."""
    return (rom_id >> 16) & 0xFF


def extract_rom_index(rom_id):
    """Extract ROM index from ROM ID."""
    return rom_id & 0xFFFF


# ============================================================================
# Data extraction from SQLite
# ============================================================================

def load_database(db_path, min_size_pow2, max_size_pow2):
    """Load all data from SQLite database."""
    print(f"Loading database: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Load manufacturers
    print("  Loading manufacturers...")
    cursor.execute("SELECT id, name FROM manufacturers ORDER BY id")
    manufacturers = {row['id']: row['name'] for row in cursor.fetchall()}
    print(f"    {len(manufacturers)} manufacturers")

    # Load ROM names
    print("  Loading ROM names...")
    cursor.execute("SELECT id, name FROM rom_names ORDER BY id")
    rom_names = {row['id']: row['name'] for row in cursor.fetchall()}
    print(f"    {len(rom_names)} ROM names")

    # Load ROMs (filtered by size and require valid SHA1)
    print(f"  Loading ROMs (size: 2^{min_size_pow2} to 2^{max_size_pow2} bytes)...")
    cursor.execute("""
        SELECT id, sha1, size_pow2, name_id
        FROM roms
        WHERE size_pow2 >= ? AND size_pow2 <= ?
        AND sha1 IS NOT NULL AND length(sha1) = 20
        ORDER BY size_pow2, sha1
    """, (min_size_pow2, max_size_pow2))

    roms = []
    for row in cursor.fetchall():
        roms.append({
            'old_id': row['id'],
            'sha1': row['sha1'],
            'size_pow2': row['size_pow2'],
            'name_id': row['name_id'],
        })
    print(f"    {len(roms)} ROMs")

    # Load machines
    print("  Loading machines...")
    cursor.execute("""
        SELECT id, name, cloneof_id, romof_id, description, year, manufacturer_id
        FROM machines
        ORDER BY id
    """)
    machines = {}
    for row in cursor.fetchall():
        machines[row['id']] = {
            'name': row['name'],
            'cloneof_id': row['cloneof_id'],
            'romof_id': row['romof_id'],
            'description': row['description'],
            'year': row['year'] if row['year'] else 0,
            'manufacturer_id': row['manufacturer_id'],
        }
    print(f"    {len(machines)} machines")

    # Load machine_roms (only for filtered ROMs)
    print("  Loading machine-ROM mappings...")
    rom_old_ids = {r['old_id'] for r in roms}
    cursor.execute("""
        SELECT machine_id, rom_id, name_id
        FROM machine_roms
        ORDER BY rom_id, machine_id
    """)

    machine_roms = []
    for row in cursor.fetchall():
        if row['rom_id'] in rom_old_ids:
            machine_roms.append({
                'machine_id': row['machine_id'],
                'rom_id': row['rom_id'],
                'name_id': row['name_id'],
            })
    print(f"    {len(machine_roms)} mappings")

    conn.close()

    return {
        'manufacturers': manufacturers,
        'rom_names': rom_names,
        'roms': roms,
        'machines': machines,
        'machine_roms': machine_roms,
    }


# ============================================================================
# ID remapping
# ============================================================================

def remap_ids(data, min_size_pow2, max_size_pow2):
    """
    Remap all IDs to new compact values.

    ROM IDs: size_pow2 in high 8 bits, sorted index in low 16 bits.
    Other IDs: sequential starting from 0.
    """
    print("Remapping IDs...")

    # Build ROM ID mapping (old_id -> new_id)
    # Group ROMs by size, then assign sequential indices
    roms_by_size = defaultdict(list)
    for rom in data['roms']:
        roms_by_size[rom['size_pow2']].append(rom)

    rom_id_map = {}  # old_id -> new_id
    size_index = {}  # size_pow2 -> (start_index, count)

    global_index = 0
    for size_pow2 in range(min_size_pow2, max_size_pow2 + 1):
        roms_list = roms_by_size.get(size_pow2, [])
        # Already sorted by SHA1 from SQL query

        size_index[size_pow2] = (global_index, len(roms_list))

        for idx, rom in enumerate(roms_list):
            new_id = make_rom_id(size_pow2, idx)
            rom_id_map[rom['old_id']] = new_id
            rom['new_id'] = new_id
            global_index += 1

    print(f"  ROM ID mapping: {len(rom_id_map)} entries")
    for size_pow2 in range(min_size_pow2, max_size_pow2 + 1):
        start, count = size_index.get(size_pow2, (0, 0))
        if count > 0:
            size_bytes = 2 ** size_pow2
            if size_bytes >= 1024 * 1024:
                size_str = f"{size_bytes // (1024*1024)} MB"
            else:
                size_str = f"{size_bytes // 1024} KB"
            print(f"    size_pow2={size_pow2} ({size_str}): {count} ROMs")

    # Filter machines that have at least one ROM
    machines_with_roms = set()
    for mr in data['machine_roms']:
        machines_with_roms.add(mr['machine_id'])

    # Build machine ID mapping
    machine_id_map = {}  # old_id -> new_id
    new_machines = {}
    new_id = 0
    for old_id in sorted(data['machines'].keys()):
        if old_id in machines_with_roms:
            machine_id_map[old_id] = new_id
            new_machines[new_id] = data['machines'][old_id]
            new_machines[new_id]['old_id'] = old_id
            new_id += 1

    print(f"  Machine ID mapping: {len(machine_id_map)} entries (filtered from {len(data['machines'])})")

    # Build manufacturer ID mapping (only used manufacturers)
    used_manufacturers = set()
    for m in new_machines.values():
        if m['manufacturer_id'] is not None:
            used_manufacturers.add(m['manufacturer_id'])

    manufacturer_id_map = {}  # old_id -> new_id
    new_manufacturers = {}
    new_id = 0
    for old_id in sorted(used_manufacturers):
        manufacturer_id_map[old_id] = new_id
        new_manufacturers[new_id] = data['manufacturers'].get(old_id, "Unknown")
        new_id += 1

    print(f"  Manufacturer ID mapping: {len(manufacturer_id_map)} entries")

    # Build ROM name ID mapping (only used names)
    used_rom_names = set()
    for rom in data['roms']:
        if rom['name_id'] is not None:
            used_rom_names.add(rom['name_id'])
    for mr in data['machine_roms']:
        if mr['name_id'] is not None:
            used_rom_names.add(mr['name_id'])

    rom_name_id_map = {}  # old_id -> new_id
    new_rom_names = {}
    new_id = 0
    for old_id in sorted(used_rom_names):
        rom_name_id_map[old_id] = new_id
        new_rom_names[new_id] = data['rom_names'].get(old_id, "")
        new_id += 1

    print(f"  ROM name ID mapping: {len(rom_name_id_map)} entries")

    # Update references in machine_roms
    new_machine_roms = []
    for mr in data['machine_roms']:
        if mr['machine_id'] in machine_id_map and mr['rom_id'] in rom_id_map:
            new_machine_roms.append({
                'machine_id': machine_id_map[mr['machine_id']],
                'rom_id': rom_id_map[mr['rom_id']],
                'name_id': rom_name_id_map.get(mr['name_id'], NULL_ID_24) if mr['name_id'] else NULL_ID_24,
            })

    # Sort by rom_id for efficient lookup
    new_machine_roms.sort(key=lambda x: (x['rom_id'], x['machine_id']))
    print(f"  Machine-ROM mappings: {len(new_machine_roms)} entries")

    # Update references in machines
    for m in new_machines.values():
        old_cloneof = m['cloneof_id']
        old_romof = m['romof_id']
        old_manuf = m['manufacturer_id']

        m['cloneof_id'] = machine_id_map.get(old_cloneof, NULL_ID_24) if old_cloneof else NULL_ID_24
        m['romof_id'] = machine_id_map.get(old_romof, NULL_ID_24) if old_romof else NULL_ID_24
        m['manufacturer_id'] = manufacturer_id_map.get(old_manuf, NULL_ID_16) if old_manuf else NULL_ID_16

    # Update references in roms
    for rom in data['roms']:
        rom['name_id'] = rom_name_id_map.get(rom['name_id'], NULL_ID_24) if rom['name_id'] else NULL_ID_24

    return {
        'roms': data['roms'],  # Already has new_id
        'machines': new_machines,
        'machine_roms': new_machine_roms,
        'manufacturers': new_manufacturers,
        'rom_names': new_rom_names,
        'size_index': size_index,
        'rom_id_map': rom_id_map,
    }


# ============================================================================
# Binary generation
# ============================================================================

def build_strings_pool(data):
    """Build the strings pool and return (pool_bytes, offset_map)."""
    print("Building strings pool...")

    strings = set()

    # Collect all strings
    for name in data['manufacturers'].values():
        strings.add(name)

    for name in data['rom_names'].values():
        strings.add(name)

    for m in data['machines'].values():
        strings.add(m['name'])
        # Description is stored compressed separately

    # Sort for deterministic output
    strings = sorted(strings)

    # Build pool
    pool = bytearray()
    offset_map = {}

    for s in strings:
        offset_map[s] = len(pool)
        pool.extend(s.encode('utf-8'))
        pool.append(0)  # Null terminator

    print(f"  {len(strings)} unique strings, {len(pool)} bytes")

    return bytes(pool), offset_map


def build_descriptions_pool(data):
    """Build compressed descriptions pool."""
    print("Building descriptions pool...")

    pool = bytearray()
    desc_info = {}  # machine_new_id -> (offset, length)

    for new_id, m in sorted(data['machines'].items()):
        if m['description']:
            # Description is already zlib compressed in database
            desc_data = m['description']
            desc_info[new_id] = (len(pool), len(desc_data))
            pool.extend(desc_data)
        else:
            desc_info[new_id] = (0, 0)

    print(f"  {len([d for d in desc_info.values() if d[1] > 0])} descriptions, {len(pool)} bytes")

    return bytes(pool), desc_info


def generate_binary(data, strings_pool, string_offsets, desc_pool, desc_info,
                    min_size_pow2, max_size_pow2):
    """Generate the complete binary file."""
    print("Generating binary data...")

    # Calculate sizes
    num_sizes = max_size_pow2 - min_size_pow2 + 1
    size_index_size = num_sizes * 8  # 4 bytes start_offset + 4 bytes end_offset per size

    roms_count = len(data['roms'])
    machines_count = len(data['machines'])
    machine_roms_count = len(data['machine_roms'])
    manufacturers_count = len(data['manufacturers'])
    rom_names_count = len(data['rom_names'])

    # ROM entry: sha1(20) + name_id(3) = 23 bytes
    roms_size = roms_count * 23

    # Machine entry: name_offset(4) + desc_offset(4) + desc_len(2) +
    #                cloneof_id(3) + romof_id(3) + year(2) + manufacturer_id(2) = 20 bytes
    machines_size = machines_count * 20

    # Machine-ROM entry: machine_id(3) + rom_id(3) + name_id(3) = 9 bytes
    machine_roms_size = machine_roms_count * 9

    # Manufacturer entry: name_offset(4) = 4 bytes
    manufacturers_size = manufacturers_count * 4

    # ROM name entry: name_offset(4) = 4 bytes
    rom_names_size = rom_names_count * 4

    # Calculate offsets
    size_index_offset = HEADER_SIZE
    roms_offset = size_index_offset + size_index_size
    machines_offset = roms_offset + roms_size
    machine_roms_offset = machines_offset + machines_size
    manufacturers_offset = machine_roms_offset + machine_roms_size
    rom_names_offset = manufacturers_offset + manufacturers_size
    strings_offset = rom_names_offset + rom_names_size
    desc_offset = strings_offset + len(strings_pool)
    total_size = desc_offset + len(desc_pool)

    print(f"  Header: {HEADER_SIZE} bytes")
    print(f"  Size index: {size_index_size} bytes")
    print(f"  ROMs table: {roms_size} bytes ({roms_count} entries)")
    print(f"  Machines table: {machines_size} bytes ({machines_count} entries)")
    print(f"  Machine-ROMs: {machine_roms_size} bytes ({machine_roms_count} entries)")
    print(f"  Manufacturers: {manufacturers_size} bytes ({manufacturers_count} entries)")
    print(f"  ROM names: {rom_names_size} bytes ({rom_names_count} entries)")
    print(f"  Strings pool: {len(strings_pool)} bytes")
    print(f"  Descriptions pool: {len(desc_pool)} bytes")
    print(f"  Total: {total_size} bytes ({total_size / 1024 / 1024:.2f} MB)")

    # Build binary
    output = bytearray()

    # Header
    header = struct.pack(
        '<4sHBB IIIII IIIIIIII',
        MAGIC,
        VERSION,
        min_size_pow2,
        max_size_pow2,
        roms_count,
        machines_count,
        machine_roms_count,
        manufacturers_count,
        rom_names_count,
        size_index_offset,
        roms_offset,
        machines_offset,
        machine_roms_offset,
        manufacturers_offset,
        rom_names_offset,
        strings_offset,
        desc_offset,
    )
    output.extend(header)

    # Pad header to HEADER_SIZE
    output.extend(b'\x00' * (HEADER_SIZE - len(header)))

    # Size index table (byte offsets relative to roms_offset)
    ROM_ENTRY_SIZE = 23  # sha1(20) + name_id(3)
    for size_pow2 in range(min_size_pow2, max_size_pow2 + 1):
        start_idx, count = data['size_index'].get(size_pow2, (0, 0))
        start_offset = start_idx * ROM_ENTRY_SIZE
        end_offset = (start_idx + count) * ROM_ENTRY_SIZE
        output.extend(struct.pack('<I', start_offset))
        output.extend(struct.pack('<I', end_offset))

    # ROMs table (sorted by size_pow2, then SHA1)
    for rom in data['roms']:
        sha1 = rom['sha1'] if rom['sha1'] else b'\x00' * 20
        output.extend(sha1)
        output.extend(write_uint24(rom['name_id']))

    # Machines table
    for new_id in range(machines_count):
        m = data['machines'][new_id]
        name_off = string_offsets.get(m['name'], 0)
        d_off, d_len = desc_info.get(new_id, (0, 0))

        output.extend(struct.pack('<I', name_off))
        output.extend(struct.pack('<I', d_off))
        output.extend(struct.pack('<H', d_len))
        output.extend(write_uint24(m['cloneof_id']))
        output.extend(write_uint24(m['romof_id']))
        output.extend(struct.pack('<H', m['year'] & 0xFFFF))
        output.extend(struct.pack('<H', m['manufacturer_id'] & 0xFFFF))

    # Machine-ROMs table (sorted by rom_id)
    for mr in data['machine_roms']:
        output.extend(write_uint24(mr['machine_id']))
        output.extend(write_uint24(mr['rom_id']))
        output.extend(write_uint24(mr['name_id']))

    # Manufacturers table
    for new_id in range(manufacturers_count):
        name = data['manufacturers'][new_id]
        name_off = string_offsets.get(name, 0)
        output.extend(struct.pack('<I', name_off))

    # ROM names table
    for new_id in range(rom_names_count):
        name = data['rom_names'][new_id]
        name_off = string_offsets.get(name, 0)
        output.extend(struct.pack('<I', name_off))

    # Strings pool
    output.extend(strings_pool)

    # Descriptions pool
    output.extend(desc_pool)

    return bytes(output)


# ============================================================================
# C header generation
# ============================================================================

def generate_header(data, binary_data, output_path, min_size_pow2, max_size_pow2):
    """Generate C header file with structure definitions."""
    print(f"Generating C header: {output_path}")

    header_content = f'''// Auto-generated MAME ROM database for arcade_rom_pro
// Generated by generate_embedded_database.py
// DO NOT EDIT MANUALLY

#ifndef MAME_ROM_DATABASE_H
#define MAME_ROM_DATABASE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {{
#endif

// ============================================================================
// Database configuration
// ============================================================================

#define MRDB_MAGIC          0x4244524D  // "MRDB" in little-endian
#define MRDB_VERSION        {VERSION}
#define MRDB_MIN_SIZE_POW2  {min_size_pow2}
#define MRDB_MAX_SIZE_POW2  {max_size_pow2}
#define MRDB_NUM_SIZES      {max_size_pow2 - min_size_pow2 + 1}

#define MRDB_NULL_ID_24     0xFFFFFF
#define MRDB_NULL_ID_16     0xFFFF

// ============================================================================
// Database statistics
// ============================================================================

#define MRDB_ROMS_COUNT         {len(data['roms'])}
#define MRDB_MACHINES_COUNT     {len(data['machines'])}
#define MRDB_MACHINE_ROMS_COUNT {len(data['machine_roms'])}
#define MRDB_MANUFACTURERS_COUNT {len(data['manufacturers'])}
#define MRDB_ROM_NAMES_COUNT    {len(data['rom_names'])}
#define MRDB_TOTAL_SIZE         {len(binary_data)}

// ============================================================================
// Data structures
// ============================================================================

// Header structure (64 bytes)
typedef struct __attribute__((packed)) {{
    uint32_t magic;
    uint16_t version;
    uint8_t min_size_pow2;
    uint8_t max_size_pow2;

    uint32_t roms_count;
    uint32_t machines_count;
    uint32_t machine_roms_count;
    uint32_t manufacturers_count;
    uint32_t rom_names_count;

    uint32_t size_index_offset;
    uint32_t roms_offset;
    uint32_t machines_offset;
    uint32_t machine_roms_offset;
    uint32_t manufacturers_offset;
    uint32_t rom_names_offset;
    uint32_t strings_offset;
    uint32_t desc_offset;
}} MrdbHeader;

// Size index entry (8 bytes per size)
// Stores byte offsets relative to roms_offset for direct pointer access
typedef struct __attribute__((packed)) {{
    uint32_t start_offset;    // Byte offset to first ROM of this size (relative to roms_offset)
    uint32_t end_offset;      // Byte offset past last ROM of this size (relative to roms_offset)
}} MrdbSizeIndex;

// ROM entry (23 bytes)
typedef struct __attribute__((packed)) {{
    uint8_t sha1[20];
    uint8_t name_id[3];       // 24-bit ID into rom_names table
}} MrdbRom;

// Machine entry (20 bytes)
typedef struct __attribute__((packed)) {{
    uint32_t name_offset;     // Offset into strings pool
    uint32_t desc_offset;     // Offset into descriptions pool
    uint16_t desc_length;     // Compressed description length
    uint8_t cloneof_id[3];    // 24-bit machine ID or 0xFFFFFF
    uint8_t romof_id[3];      // 24-bit machine ID or 0xFFFFFF
    uint16_t year;            // Release year (16-bit)
    uint16_t manufacturer_id; // 16-bit ID or 0xFFFF
}} MrdbMachine;

// Machine-ROM mapping entry (9 bytes)
typedef struct __attribute__((packed)) {{
    uint8_t machine_id[3];    // 24-bit machine ID
    uint8_t rom_id[3];        // 24-bit ROM ID (size_pow2 << 16 | index)
    uint8_t name_id[3];       // 24-bit ROM name ID in this machine
}} MrdbMachineRom;

// Manufacturer entry (4 bytes)
typedef struct __attribute__((packed)) {{
    uint32_t name_offset;     // Offset into strings pool
}} MrdbManufacturer;

// ROM name entry (4 bytes)
typedef struct __attribute__((packed)) {{
    uint32_t name_offset;     // Offset into strings pool
}} MrdbRomName;

// ============================================================================
// Helper macros for 24-bit ID handling
// ============================================================================

#define MRDB_READ_UINT24(ptr) \\
    ((uint32_t)(ptr)[0] | ((uint32_t)(ptr)[1] << 8) | ((uint32_t)(ptr)[2] << 16))

#define MRDB_WRITE_UINT24(ptr, val) do {{ \\
    (ptr)[0] = (uint8_t)((val) & 0xFF); \\
    (ptr)[1] = (uint8_t)(((val) >> 8) & 0xFF); \\
    (ptr)[2] = (uint8_t)(((val) >> 16) & 0xFF); \\
}} while(0)

// Extract size_pow2 from ROM ID
#define MRDB_ROM_ID_SIZE_POW2(rom_id) (((rom_id) >> 16) & 0xFF)

// Extract index from ROM ID
#define MRDB_ROM_ID_INDEX(rom_id) ((rom_id) & 0xFFFF)

// Create ROM ID from size_pow2 and index
#define MRDB_MAKE_ROM_ID(size_pow2, index) (((uint32_t)(size_pow2) << 16) | (index))

// ============================================================================
// Database access functions (to be implemented)
// ============================================================================

// Get pointer to embedded database
const uint8_t* mrdb_get_data(void);

// Get database header
static inline const MrdbHeader* mrdb_get_header(const uint8_t* db) {{
    return (const MrdbHeader*)db;
}}

// Get size index table
static inline const MrdbSizeIndex* mrdb_get_size_index(const uint8_t* db) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const MrdbSizeIndex*)(db + hdr->size_index_offset);
}}

// Get ROMs table
static inline const MrdbRom* mrdb_get_roms(const uint8_t* db) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const MrdbRom*)(db + hdr->roms_offset);
}}

// Get machines table
static inline const MrdbMachine* mrdb_get_machines(const uint8_t* db) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const MrdbMachine*)(db + hdr->machines_offset);
}}

// Get machine-ROMs table
static inline const MrdbMachineRom* mrdb_get_machine_roms(const uint8_t* db) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const MrdbMachineRom*)(db + hdr->machine_roms_offset);
}}

// Get manufacturers table
static inline const MrdbManufacturer* mrdb_get_manufacturers(const uint8_t* db) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const MrdbManufacturer*)(db + hdr->manufacturers_offset);
}}

// Get ROM names table
static inline const MrdbRomName* mrdb_get_rom_names(const uint8_t* db) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const MrdbRomName*)(db + hdr->rom_names_offset);
}}

// Get string from pool
static inline const char* mrdb_get_string(const uint8_t* db, uint32_t offset) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return (const char*)(db + hdr->strings_offset + offset);
}}

// Get compressed description
static inline const uint8_t* mrdb_get_description(const uint8_t* db, uint32_t offset) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    return db + hdr->desc_offset + offset;
}}

// ============================================================================
// ROM lookup functions
// ============================================================================

#define MRDB_ROM_ENTRY_SIZE 23  // sha1(20) + name_id(3)

/**
 * Binary search for a ROM by size and SHA1.
 *
 * @param db        Pointer to the database
 * @param size_pow2 ROM size as power of 2 (e.g., 21 for 2MB)
 * @param sha1      20-byte SHA1 hash to search for
 * @return          Pointer to matching MrdbRom entry, or NULL if not found
 */
const MrdbRom* mrdb_find_rom_by_sha1(const uint8_t* db, uint8_t size_pow2, const uint8_t* sha1);

/**
 * Get ROM ID from a ROM pointer.
 *
 * @param db  Pointer to the database
 * @param rom Pointer to the ROM entry
 * @return    24-bit ROM ID (size_pow2 << 16 | index)
 */
static inline uint32_t mrdb_get_rom_id(const uint8_t* db, const MrdbRom* rom) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    const MrdbRom* roms_base = mrdb_get_roms(db);
    uint32_t index = (uint32_t)(rom - roms_base);

    // Find size_pow2 from the size index
    const MrdbSizeIndex* size_idx = mrdb_get_size_index(db);
    for (uint8_t sp = hdr->min_size_pow2; sp <= hdr->max_size_pow2; sp++) {{
        uint32_t start = size_idx[sp - hdr->min_size_pow2].start_offset / MRDB_ROM_ENTRY_SIZE;
        uint32_t end = size_idx[sp - hdr->min_size_pow2].end_offset / MRDB_ROM_ENTRY_SIZE;
        if (index >= start && index < end) {{
            return MRDB_MAKE_ROM_ID(sp, index - start);
        }}
    }}
    return MRDB_NULL_ID_24;
}}

// ============================================================================
// Machine lookup functions
// ============================================================================

#define MRDB_MACHINE_ROM_ENTRY_SIZE 9  // machine_id(3) + rom_id(3) + name_id(3)

/**
 * Result structure for machine lookup.
 */
typedef struct {{
    uint32_t machine_id;       // Machine ID
    uint32_t rom_name_id;      // ROM filename ID for this machine
}} MrdbMachineResult;

/**
 * Get all machines associated with a ROM.
 *
 * @param db          Pointer to the database
 * @param rom         Pointer to the ROM entry (from mrdb_find_rom_by_sha1)
 * @param size_pow2   Size of the ROM (power of 2)
 * @param results     Array to store results
 * @param max_results Maximum number of results to return
 * @return            Number of machines found (may be > max_results if truncated)
 */
uint32_t mrdb_get_machines_for_rom(const uint8_t* db, const MrdbRom* rom,
                                    uint8_t size_pow2, MrdbMachineResult* results,
                                    uint32_t max_results);

/**
 * Get machine name.
 */
const char* mrdb_get_machine_name(const uint8_t* db, uint32_t machine_id);

/**
 * Get machine description (decompresses zlib data).
 *
 * @param db          Pointer to the database
 * @param machine_id  Machine ID
 * @param buffer      Output buffer for decompressed description
 * @param buffer_size Size of output buffer
 * @return            Length of description, or 0 if failed/empty
 */
uint32_t mrdb_get_machine_description(const uint8_t* db, uint32_t machine_id,
                                       char* buffer, uint32_t buffer_size);

/**
 * Get machine cloneof ID.
 */
static inline uint32_t mrdb_get_machine_cloneof(const uint8_t* db, uint32_t machine_id) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    if (machine_id >= hdr->machines_count) {{
        return MRDB_NULL_ID_24;
    }}
    const MrdbMachine* machine = &mrdb_get_machines(db)[machine_id];
    return MRDB_READ_UINT24(machine->cloneof_id);
}}

/**
 * Get machine romof ID.
 */
static inline uint32_t mrdb_get_machine_romof(const uint8_t* db, uint32_t machine_id) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    if (machine_id >= hdr->machines_count) {{
        return MRDB_NULL_ID_24;
    }}
    const MrdbMachine* machine = &mrdb_get_machines(db)[machine_id];
    return MRDB_READ_UINT24(machine->romof_id);
}}

/**
 * Get machine year.
 */
static inline uint16_t mrdb_get_machine_year(const uint8_t* db, uint32_t machine_id) {{
    const MrdbHeader* hdr = mrdb_get_header(db);
    if (machine_id >= hdr->machines_count) {{
        return 0;
    }}
    const MrdbMachine* machine = &mrdb_get_machines(db)[machine_id];
    return machine->year;
}}

/**
 * Get ROM filename.
 */
const char* mrdb_get_rom_name(const uint8_t* db, uint32_t name_id);

#ifdef __cplusplus
}}
#endif

#endif // MAME_ROM_DATABASE_H
'''

    with open(output_path, 'w') as f:
        f.write(header_content)


def generate_data_file(binary_data, output_path):
    """Generate C source file with embedded binary data and search function."""
    print(f"Generating C data file: {output_path}")

    lines = [
        '// Auto-generated MAME ROM database binary data',
        '// Generated by generate_embedded_database.py',
        '// DO NOT EDIT MANUALLY',
        '',
        '#include <stdint.h>',
        '#include <string.h>',
        '',
        '// Use miniz for decompression (lighter than zlib)',
        '#define MINIZ_NO_STDIO',
        '#define MINIZ_NO_TIME',
        '#define MINIZ_NO_ZLIB_APIS',
        '#define MINIZ_NO_MALLOC',
        '#include "miniz.h"',
        '',
        '#include "mame_rom_database.h"',
        '',
        f'// Database size: {len(binary_data)} bytes ({len(binary_data) / 1024 / 1024:.2f} MB)',
        '',
        'const uint8_t mame_rom_database[] = {',
    ]

    # Write data in rows of 16 bytes
    for i in range(0, len(binary_data), 16):
        chunk = binary_data[i:i+16]
        hex_values = ', '.join(f'0x{b:02x}' for b in chunk)
        lines.append(f'    {hex_values},')

    lines.append('};')
    lines.append('')
    lines.append('const uint8_t* mrdb_get_data(void) {')
    lines.append('    return mame_rom_database;')
    lines.append('}')
    lines.append('')

    # Add binary search function
    binary_search_code = '''
// ============================================================================
// Binary search for ROM by size and SHA1
// ============================================================================

const MrdbRom* mrdb_find_rom_by_sha1(const uint8_t* db, uint8_t size_pow2, const uint8_t* sha1) {
    const MrdbHeader* hdr = mrdb_get_header(db);

    // Validate size_pow2 range
    if (size_pow2 < hdr->min_size_pow2 || size_pow2 > hdr->max_size_pow2) {
        return NULL;
    }

    // Get size index entry for this size
    const MrdbSizeIndex* size_index = mrdb_get_size_index(db);
    uint32_t idx = size_pow2 - hdr->min_size_pow2;

    uint32_t start_offset = size_index[idx].start_offset;
    uint32_t end_offset = size_index[idx].end_offset;

    // Empty range check
    if (start_offset >= end_offset) {
        return NULL;
    }

    // Get pointer to ROMs table base
    const uint8_t* roms_base = db + hdr->roms_offset;

    // Binary search within the range
    // Offsets are in bytes, ROM entry size is 23 bytes
    const uint8_t* left = roms_base + start_offset;
    const uint8_t* right = roms_base + end_offset - MRDB_ROM_ENTRY_SIZE;

    while (left <= right) {
        // Calculate middle pointer (aligned to ROM entry boundary)
        size_t left_idx = (left - roms_base) / MRDB_ROM_ENTRY_SIZE;
        size_t right_idx = (right - roms_base) / MRDB_ROM_ENTRY_SIZE;
        size_t mid_idx = left_idx + (right_idx - left_idx) / 2;
        const uint8_t* mid = roms_base + mid_idx * MRDB_ROM_ENTRY_SIZE;

        // Compare SHA1 (first 20 bytes of ROM entry)
        int cmp = memcmp(mid, sha1, 20);

        if (cmp == 0) {
            // Found it
            return (const MrdbRom*)mid;
        } else if (cmp < 0) {
            // mid < sha1, search right half
            left = mid + MRDB_ROM_ENTRY_SIZE;
        } else {
            // mid > sha1, search left half
            right = mid - MRDB_ROM_ENTRY_SIZE;
        }
    }

    // Not found
    return NULL;
}

// ============================================================================
// Machine lookup functions
// ============================================================================

#define MRDB_MACHINE_ROM_ENTRY_SIZE 9  // machine_id(3) + rom_id(3) + name_id(3)

/**
 * Find first machine-ROM mapping entry for a given ROM ID.
 * Uses binary search on the machine_roms table (sorted by rom_id).
 *
 * @param db     Pointer to the database
 * @param rom_id 24-bit ROM ID to search for
 * @return       Pointer to first MrdbMachineRom entry, or NULL if not found
 */
static const MrdbMachineRom* mrdb_find_first_machine_rom(const uint8_t* db, uint32_t rom_id) {
    const MrdbHeader* hdr = mrdb_get_header(db);
    const uint8_t* base = db + hdr->machine_roms_offset;

    uint32_t left = 0;
    uint32_t right = hdr->machine_roms_count;
    uint32_t result = hdr->machine_roms_count;  // Invalid index

    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        const uint8_t* entry = base + mid * MRDB_MACHINE_ROM_ENTRY_SIZE;
        uint32_t mid_rom_id = MRDB_READ_UINT24(entry + 3);

        if (mid_rom_id >= rom_id) {
            if (mid_rom_id == rom_id) {
                result = mid;
            }
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    if (result >= hdr->machine_roms_count) {
        return NULL;
    }

    return (const MrdbMachineRom*)(base + result * MRDB_MACHINE_ROM_ENTRY_SIZE);
}

/**
 * Get all machines associated with a ROM.
 *
 * @param db          Pointer to the database
 * @param rom         Pointer to the ROM entry (from mrdb_find_rom_by_sha1)
 * @param size_pow2   Size of the ROM (power of 2)
 * @param results     Array to store results
 * @param max_results Maximum number of results to return
 * @return            Number of machines found (may be > max_results if truncated)
 */
uint32_t mrdb_get_machines_for_rom(const uint8_t* db, const MrdbRom* rom,
                                    uint8_t size_pow2, MrdbMachineResult* results,
                                    uint32_t max_results) {
    const MrdbHeader* hdr = mrdb_get_header(db);

    // Calculate ROM ID from pointer
    const MrdbRom* roms_base = mrdb_get_roms(db);
    const MrdbSizeIndex* size_idx = mrdb_get_size_index(db);
    uint32_t size_offset = size_idx[size_pow2 - hdr->min_size_pow2].start_offset;
    uint32_t rom_index = ((const uint8_t*)rom - (const uint8_t*)roms_base - size_offset) / MRDB_ROM_ENTRY_SIZE;
    uint32_t rom_id = MRDB_MAKE_ROM_ID(size_pow2, rom_index);

    // Find first machine-ROM entry
    const MrdbMachineRom* mr = mrdb_find_first_machine_rom(db, rom_id);
    if (!mr) {
        return 0;
    }

    // Collect all machines with this ROM
    const uint8_t* base = db + hdr->machine_roms_offset;
    const uint8_t* end = base + hdr->machine_roms_count * MRDB_MACHINE_ROM_ENTRY_SIZE;
    const uint8_t* ptr = (const uint8_t*)mr;

    uint32_t count = 0;
    while (ptr < end) {
        uint32_t entry_rom_id = MRDB_READ_UINT24(ptr + 3);
        if (entry_rom_id != rom_id) {
            break;
        }

        if (count < max_results && results) {
            results[count].machine_id = MRDB_READ_UINT24(ptr);
            results[count].rom_name_id = MRDB_READ_UINT24(ptr + 6);
        }
        count++;
        ptr += MRDB_MACHINE_ROM_ENTRY_SIZE;
    }

    return count;
}

/**
 * Get machine name.
 *
 * @param db         Pointer to the database
 * @param machine_id Machine ID
 * @return           Machine name string, or NULL if invalid ID
 */
const char* mrdb_get_machine_name(const uint8_t* db, uint32_t machine_id) {
    const MrdbHeader* hdr = mrdb_get_header(db);
    if (machine_id >= hdr->machines_count) {
        return NULL;
    }
    const MrdbMachine* machine = &mrdb_get_machines(db)[machine_id];
    return mrdb_get_string(db, machine->name_offset);
}

/**
 * Get machine description (decompresses zlib data).
 *
 * @param db         Pointer to the database
 * @param machine_id Machine ID
 * @param buffer     Output buffer for decompressed description
 * @param buffer_size Size of output buffer
 * @return           Length of description, or 0 if failed/empty
 */
/**
 * Get ROM filename for a machine.
 *
 * @param db      Pointer to the database
 * @param name_id ROM name ID (from MrdbMachineResult.rom_name_id)
 * @return        ROM filename string, or NULL if invalid
 */
const char* mrdb_get_rom_name(const uint8_t* db, uint32_t name_id) {
    const MrdbHeader* hdr = mrdb_get_header(db);
    if (name_id >= hdr->rom_names_count || name_id == MRDB_NULL_ID_24) {
        return NULL;
    }
    const MrdbRomName* rom_name = &mrdb_get_rom_names(db)[name_id];
    return mrdb_get_string(db, rom_name->name_offset);
}

/**
 * Get machine description (decompresses zlib data using tinfl).
 *
 * @param db          Pointer to the database
 * @param machine_id  Machine ID
 * @param buffer      Output buffer for decompressed description
 * @param buffer_size Size of output buffer
 * @return            Length of description (excluding null terminator), or 0 if failed/empty
 */
uint32_t mrdb_get_machine_description(const uint8_t* db, uint32_t machine_id,
                                       char* buffer, uint32_t buffer_size) {
    const MrdbHeader* hdr = mrdb_get_header(db);

    if (machine_id >= hdr->machines_count || !buffer || buffer_size == 0) {
        return 0;
    }

    const MrdbMachine* machine = &mrdb_get_machines(db)[machine_id];

    // Check if description exists
    if (machine->desc_length == 0) {
        buffer[0] = '\\0';
        return 0;
    }

    // Get compressed data (zlib format: 2-byte header + deflate data + 4-byte checksum)
    const uint8_t* compressed = mrdb_get_description(db, machine->desc_offset);
    uint16_t comp_len = machine->desc_length;

    // Skip zlib header (2 bytes) and checksum (4 bytes)
    if (comp_len <= 6) {
        buffer[0] = '\\0';
        return 0;
    }
    const uint8_t* deflate_data = compressed + 2;
    size_t deflate_len = comp_len - 6;

    // Decompress using tinfl
    size_t out_len = buffer_size - 1;
    int status = tinfl_decompress_mem_to_mem(buffer, out_len, deflate_data, deflate_len,
                                              TINFL_FLAG_PARSE_ZLIB_HEADER);

    if (status < 0) {
        // Try without skipping header (raw deflate)
        status = tinfl_decompress_mem_to_mem(buffer, out_len, compressed + 2, comp_len - 6, 0);
        if (status < 0) {
            buffer[0] = '\\0';
            return 0;
        }
    }

    buffer[status] = '\\0';
    return (uint32_t)status;
}
'''
    lines.append(binary_search_code)

    with open(output_path, 'w') as f:
        f.write('\n'.join(lines))


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate embedded ROM database for Raspberry Pi Pico 2"
    )
    parser.add_argument('--database', '-d', type=str, default='mame_roms.db',
                        help="Input SQLite database path")
    parser.add_argument('--output', '-o', type=str, default='mame_rom_database',
                        help="Output base name (generates .bin, .h, .c files)")
    parser.add_argument('--min-size', type=int, default=DEFAULT_MIN_SIZE_POW2,
                        help=f"Minimum ROM size as power of 2 (default: {DEFAULT_MIN_SIZE_POW2})")
    parser.add_argument('--max-size', type=int, default=DEFAULT_MAX_SIZE_POW2,
                        help=f"Maximum ROM size as power of 2 (default: {DEFAULT_MAX_SIZE_POW2})")
    parser.add_argument('--bin-only', action='store_true',
                        help="Only generate binary file, no C headers")
    args = parser.parse_args()

    min_size_pow2 = args.min_size
    max_size_pow2 = args.max_size

    print(f"ROM size range: 2^{min_size_pow2} ({2**min_size_pow2} bytes) to "
          f"2^{max_size_pow2} ({2**max_size_pow2} bytes)")

    script_dir = Path(__file__).parent
    db_path = script_dir / args.database

    if not db_path.exists():
        print(f"Error: Database not found: {db_path}")
        sys.exit(1)

    # Load data
    data = load_database(str(db_path), min_size_pow2, max_size_pow2)

    # Remap IDs
    data = remap_ids(data, min_size_pow2, max_size_pow2)

    # Build strings pool
    strings_pool, string_offsets = build_strings_pool(data)

    # Build descriptions pool
    desc_pool, desc_info = build_descriptions_pool(data)

    # Generate binary
    binary_data = generate_binary(data, strings_pool, string_offsets, desc_pool, desc_info,
                                  min_size_pow2, max_size_pow2)

    # Output files
    bin_path = script_dir / f"{args.output}.bin"
    with open(bin_path, 'wb') as f:
        f.write(binary_data)
    print(f"\nBinary file: {bin_path} ({len(binary_data)} bytes)")

    if not args.bin_only:
        # Generate C header
        header_path = script_dir / "include" / f"{args.output}.h"
        header_path.parent.mkdir(exist_ok=True)
        generate_header(data, binary_data, str(header_path), min_size_pow2, max_size_pow2)

        # Generate C data file
        data_path = script_dir / "src" / f"{args.output}.c"
        data_path.parent.mkdir(exist_ok=True)
        generate_data_file(binary_data, str(data_path))

        print(f"C header: {header_path}")
        print(f"C source: {data_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
