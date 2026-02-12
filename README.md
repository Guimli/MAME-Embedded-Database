# MAME Embedded Database

Optimized binary database for MAME arcade ROM identification, designed to be embedded in C on PC (Linux) or microcontroller (Raspberry Pi Pico 2).

## Overview

This project transforms official MAME DAT files into a compact database (~12.5 MB) directly embedded in a C binary. The pipeline has 3 steps:

```
MAME DAT files
       |
       v
 [Step 1] build_mame_database.py
       |
       v
  SQLite database (mame_roms.db, ~40 MB)
       |
       v
 [Step 2] generate_embedded_database.py
       |
       v
  C source files + binary (src/ + include/)
       |
       v
 [Step 3] gcc compilation
       |
       v
  Executable with embedded database
```

## Step 1: Build the SQLite database

The `build_mame_database.py` script downloads MAME DAT files from [ProgettoSnaps](https://www.progettosnaps.net/dats/MAME/) and builds an optimized relational SQLite database.

```bash
pip install py7zr
python3 build_mame_database.py
```

Output: `mame_roms.db`

Full documentation: [SQLITE_DATABASE.md](SQLITE_DATABASE.md)

## Step 2: Generate the embedded database

The `generate_embedded_database.py` script converts the SQLite database into a compact binary format with corresponding C files.

```bash
python3 generate_embedded_database.py
```

Output:
- `mame_rom_database.bin` — raw binary data
- `include/mame_rom_database.h` — C structures, macros and API
- `src/mame_rom_database.c` — C array containing the binary data

Full documentation: [EMBEDDED_DATABASE.md](EMBEDDED_DATABASE.md)

## Step 3: Compilation and usage

### PC / Linux

```bash
gcc -I include -I external/miniz -o example example.c src/mame_rom_database.c \
    external/miniz/miniz.c external/miniz/miniz_tinfl.c external/miniz/miniz_tdef.c
```

### Raspberry Pi Pico 2

Add `src/mame_rom_database.c` and the miniz files to your `CMakeLists.txt`.

### Example

The `example.c` file demonstrates how to:

1. Access the embedded database and verify the header
2. Search for a ROM by its SHA1 hash and size (binary search)
3. List all machines using that ROM
4. Navigate between machines (next / previous)

```
$ ./example
MAME Embedded Database v1
  ROMs: 157561 | Machines: 49537 | Manufacturers: 4233

Searching for SHA1: 48055822E0CEA228CDECF3D05AC24E50979B6F4D
ROM size: 2 MB (2097152 bytes)

ROM found!
This ROM is used by 8 machine(s):
  kaiserkn, kaiserknj, dankuga, gblchmp, ...
```

## Project structure

```
mame_embedded_database/
├── build_mame_database.py        # Step 1: DAT -> SQLite
├── generate_embedded_database.py # Step 2: SQLite -> C binary
├── example.c                     # C API usage example
├── mame_roms.db                  # Generated SQLite database
├── mame_rom_database.bin         # Generated binary database
├── include/
│   └── mame_rom_database.h       # C header (structures, macros, API)
├── src/
│   └── mame_rom_database.c       # Embedded binary data as C array
├── external/
│   └── miniz/                    # zlib decompression library
├── SQLITE_DATABASE.md            # Step 1 documentation
├── EMBEDDED_DATABASE.md          # Step 2 documentation
└── README.md                     # This file
```

## C API

The API is defined in `include/mame_rom_database.h`. Main functions:

| Function | Description |
|----------|-------------|
| `mrdb_get_data()` | Pointer to the embedded database |
| `mrdb_get_header()` | Access the header (magic, counters, offsets) |
| `mrdb_find_rom_by_sha1()` | Binary search for a ROM by size + SHA1 |
| `mrdb_get_machines_for_rom()` | List all machines using a ROM |
| `mrdb_get_machine_name()` | Short machine name (e.g. "pacman") |
| `mrdb_get_machine_description()` | Full description (zlib decompression) |
| `mrdb_get_machine_year()` | Release year |
| `mrdb_get_rom_name()` | ROM filename |

## Statistics

| Element | Value |
|---------|-------|
| ROMs | 157,561 |
| Machines | 49,537 |
| Manufacturers | 4,233 |
| Embedded database size | ~12.5 MB |
