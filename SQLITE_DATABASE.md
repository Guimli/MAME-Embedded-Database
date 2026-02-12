# MAME ROM Database Builder

Python script to download MAME DAT files and create an optimized SQLite database for arcade ROM identification.

## Requirements

- Python 3.6+
- `py7zr` module for 7z extraction: `pip install py7zr`
- Internet connection (for automatic download)

## Installation

```bash
pip install py7zr
```

## Usage

### Automatic download (recommended)

```bash
python3 build_mame_database.py
```

The script automatically downloads the latest MAME DAT archive from [ProgettoSnaps](https://www.progettosnaps.net/dats/MAME/) and processes **all DAT files** contained in it.

### With a local DAT file

```bash
python3 build_mame_database.py --dat-file "path/to/file.dat"
```

### Add a DAT file to an existing database

```bash
python3 build_mame_database.py --add-dat --dat-file "path/to/extra.dat"
```

This option allows you to enrich an existing database with additional entries from another DAT file. Machines already present are ignored (first processed file takes priority).

### Use local DAT files (no download)

```bash
python3 build_mame_database.py --no-download
```

Searches for all `.dat` files in the `ROM/DATs/` directory or the current directory.

### Options

| Option | Description |
|--------|-------------|
| `--dat-file FILE` | Use a specific DAT file |
| `--add-dat` | Add DAT to an existing database (requires `--dat-file`) |
| `--no-download` | Don't download, use local DAT files |
| `--output FILE` | Output file path (default: `mame_roms.db`) |
| `--keep-temp` | Keep downloaded temporary files |

## Applied Filters

The script applies several filters to optimize the database:

### ROM Filters

| Criteria | Value |
|----------|-------|
| Minimum size | 2 KB (2048 bytes) |
| Maximum size | 8 MB (8388608 bytes) |
| Size | Must be a power of 2 |
| SHA1 | Required ("nodump" ROMs are ignored) |

### Optimizations

- **SHA1**: Stored as binary (20 bytes instead of 40 characters)
- **CRC**: Stored as binary (4 bytes instead of 8 characters)
- **Size**: Stored as power of 2 exponent (1 byte)
- **Description**: Compressed with zlib
- **References**: `cloneof`, `romof` stored as numeric IDs
- **Neo-Geo BIOS**: Grouped into a separate `neogeo_bios` entry

### Ignored Fields

The following DAT file fields are not imported:

- `sourcefile`, `isbios`, `isdevice`, `ismechanical`, `runnable`
- `sampleof`, `bios`, `region`, `offset`, `merge`
- `status`, `optional`
- `device_ref`, `softwarelist`, `driver`, `biosset`, `sample`, `disk`

## Database Structure

### Relational Schema

```
manufacturers (1) ─────< machines (N)
                              │
                              └────< machine_roms (N) >──── roms (N)
                                          │                    │
                                          └───> rom_names <────┘
```

### Table `manufacturers`

Arcade game manufacturers.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `name` | TEXT | Manufacturer name (unique) |

### Table `machines`

Arcade machines/games.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `name` | TEXT | Short machine name (unique, e.g., "pacman") |
| `cloneof_id` | INTEGER | FK to parent machine (clone) |
| `romof_id` | INTEGER | FK to ROM source machine |
| `description` | BLOB | Full description (zlib compressed) |
| `year` | INTEGER | Release year (0-65535) |
| `manufacturer_id` | INTEGER | FK to manufacturers |

### Table `roms`

Unique ROMs identified by their SHA1.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `sha1` | BLOB | SHA1 hash (20 binary bytes) |
| `crc` | BLOB | CRC32 (4 binary bytes) |
| `size_pow2` | INTEGER | Size = 2^size_pow2 bytes |
| `name_id` | INTEGER | FK to rom_names (reference name) |

### Table `rom_names`

Unique ROM filenames.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `name` | TEXT | Filename (unique) |

### Table `machine_roms`

Junction table between machines and ROMs.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `machine_id` | INTEGER | FK to machines |
| `rom_id` | INTEGER | FK to roms |
| `name_id` | INTEGER | FK to rom_names (name in this machine) |

## Query Examples

### Find a ROM by SHA1

```sql
SELECT m.name, m.description, rn.name as rom_file
FROM machines m
JOIN machine_roms mr ON mr.machine_id = m.id
JOIN roms r ON mr.rom_id = r.id
JOIN rom_names rn ON mr.name_id = rn.id
WHERE r.sha1 = X'<sha1_hex>'
```

### List ROMs for a machine

```sql
SELECT rn.name, (1 << r.size_pow2) as size, hex(r.sha1), hex(r.crc)
FROM machine_roms mr
JOIN roms r ON mr.rom_id = r.id
JOIN rom_names rn ON mr.name_id = rn.id
JOIN machines m ON mr.machine_id = m.id
WHERE m.name = 'pacman'
```

### Decompress description (Python)

```python
import zlib
description = zlib.decompress(row['description']).decode('utf-8')
```

### Convert binary SHA1 to hexadecimal (SQL)

```sql
SELECT hex(sha1) FROM roms
```

### Calculate actual ROM size

```sql
SELECT (1 << size_pow2) as size_bytes FROM roms
-- or in Python: size = 2 ** size_pow2
```

### Find clones of a machine

```sql
SELECT m.name, m.description
FROM machines m
JOIN machines parent ON m.cloneof_id = parent.id
WHERE parent.name = 'pacman'
```

## Neo-Geo BIOS

BIOS ROMs shared by many Neo-Geo games (>4000 machines) are automatically extracted and grouped into a special entry:

- **Machine**: `neogeo_bios`
- **Manufacturer**: SNK
- **Year**: 1990

This avoids massive duplication of links in `machine_roms` and significantly reduces database size.

## Multiple File Handling

When processing multiple DAT files (automatic download or `--no-download`):

- Files are sorted to process the main MAME DAT first
- Machines already in the database are ignored (first file = priority)
- Identical ROMs (same SHA1) are automatically deduplicated
- New ROMs and manufacturers are added to existing tables

## Typical Statistics

For a complete MAME DAT (version 0.284):

| Table | Entries |
|-------|---------|
| manufacturers | ~2,000-4,000 |
| rom_names | ~130,000-165,000 |
| roms | ~130,000-160,000 |
| machines | ~35,000-50,000 |
| machine_roms | ~300,000-500,000 |

**Database size**: 35-55 MB (depending on DAT sources used)
