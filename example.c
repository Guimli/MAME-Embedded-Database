/**
 * example.c - MAME Embedded Database usage example
 *
 * Demonstrates how to:
 *   1. Search for a ROM by its SHA1 hash and size
 *   2. Display machine name, ROM filename, and ROM size
 *   3. Navigate between machines when a ROM is shared by multiple machines
 *
 * Example ROM used:
 *   SHA1: 48055822E0CEA228CDECF3D05AC24E50979B6F4D
 *   File: d84-01.rom (2 MB)
 *   Shared by: kaiserkn, kaiserknj, dankuga, gblchmp, etc. (8 machines)
 *
 * Build (PC/Linux):
 *   gcc -I include -I external/miniz -o example example.c src/mame_rom_database.c \
 *       external/miniz/miniz.c external/miniz/miniz_tinfl.c external/miniz/miniz_tdef.c
 *
 * Build (Raspberry Pi Pico 2):
 *   Add src/mame_rom_database.c and external/miniz/miniz.c to your CMakeLists.txt
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "mame_rom_database.h"

// ----------------------------------------------------------------------------
// Helper: convert a hex string to a 20-byte SHA1 binary array
// ----------------------------------------------------------------------------
static int hex_to_sha1(const char* hex, uint8_t sha1[20]) {
    if (strlen(hex) != 40) return -1;

    for (int i = 0; i < 20; i++) {
        unsigned int byte;
        if (sscanf(&hex[i * 2], "%02x", &byte) != 1) return -1;
        sha1[i] = (uint8_t)byte;
    }
    return 0;
}

// ----------------------------------------------------------------------------
// Helper: format ROM size as a human-readable string
// ----------------------------------------------------------------------------
static const char* format_size(uint8_t size_pow2) {
    static char buf[32];
    uint32_t bytes = 1u << size_pow2;

    if (bytes >= 1024 * 1024)
        snprintf(buf, sizeof(buf), "%u MB (%u bytes)", bytes / (1024 * 1024), bytes);
    else if (bytes >= 1024)
        snprintf(buf, sizeof(buf), "%u KB (%u bytes)", bytes / 1024, bytes);
    else
        snprintf(buf, sizeof(buf), "%u bytes", bytes);

    return buf;
}

// ----------------------------------------------------------------------------
// Display one machine result
// ----------------------------------------------------------------------------
static void print_machine_info(const uint8_t* db, const MrdbMachineResult* result,
                               uint32_t index, uint32_t total, uint8_t size_pow2) {
    const char* machine_name = mrdb_get_machine_name(db, result->machine_id);
    const char* rom_name     = mrdb_get_rom_name(db, result->rom_name_id);
    uint16_t    year         = mrdb_get_machine_year(db, result->machine_id);

    char desc_buf[256];
    uint32_t desc_len = mrdb_get_machine_description(db, result->machine_id,
                                                     desc_buf, sizeof(desc_buf));

    printf("  ┌─ Machine %u / %u ─────────────────────────────\n", index + 1, total);
    printf("  │ Machine name : %s\n", machine_name ? machine_name : "(unknown)");

    if (desc_len > 0)
        printf("  │ Description  : %s\n", desc_buf);

    printf("  │ Year         : %u\n", year);
    printf("  │ ROM filename : %s\n", rom_name ? rom_name : "(unknown)");
    printf("  │ ROM size     : %s (2^%u)\n", format_size(size_pow2), size_pow2);
    printf("  └──────────────────────────────────────────────\n");
}

// ============================================================================
// Main
// ============================================================================
int main(void) {
    // Get pointer to the embedded database
    const uint8_t* db = mrdb_get_data();

    // Verify database header
    const MrdbHeader* hdr = mrdb_get_header(db);
    if (hdr->magic != MRDB_MAGIC) {
        printf("Error: invalid database magic\n");
        return 1;
    }
    printf("MAME Embedded Database v%u\n", hdr->version);
    printf("  ROMs: %u | Machines: %u | Manufacturers: %u\n\n",
           hdr->roms_count, hdr->machines_count, hdr->manufacturers_count);

    // ========================================================================
    // Step 1: Search for a ROM by SHA1 and size
    // ========================================================================

    // SHA1 of "d84-01.rom" from Kaiser Knuckle / Dan-Ku-Ga / Global Champion
    const char* sha1_hex = "48055822E0CEA228CDECF3D05AC24E50979B6F4D";
    uint8_t size_pow2 = 21;  // 2^21 = 2 MB

    uint8_t sha1[20];
    if (hex_to_sha1(sha1_hex, sha1) != 0) {
        printf("Error: invalid SHA1 hex string\n");
        return 1;
    }

    printf("Searching for SHA1: %s\n", sha1_hex);
    printf("ROM size: %s\n\n", format_size(size_pow2));

    // Binary search in the database
    const MrdbRom* rom = mrdb_find_rom_by_sha1(db, size_pow2, sha1);

    if (!rom) {
        printf("ROM not found in database.\n");
        return 0;
    }

    printf("ROM found!\n\n");

    // ========================================================================
    // Step 2: Get all machines that use this ROM
    // ========================================================================

    #define MAX_MACHINES 16
    MrdbMachineResult results[MAX_MACHINES];

    uint32_t total = mrdb_get_machines_for_rom(db, rom, size_pow2,
                                                results, MAX_MACHINES);

    printf("This ROM is used by %u machine(s):\n\n", total);

    // Display all machines
    uint32_t display_count = (total < MAX_MACHINES) ? total : MAX_MACHINES;
    for (uint32_t i = 0; i < display_count; i++) {
        print_machine_info(db, &results[i], i, total, size_pow2);
    }

    // ========================================================================
    // Step 3: Navigate between machines (next / previous)
    //
    // In a real application (e.g. on Pico 2 with buttons), you would keep
    // a "current_index" and increment/decrement it on button press.
    // ========================================================================

    printf("\n--- Navigation demo ---\n\n");

    if (total <= 1) {
        printf("Only one machine, no navigation needed.\n");
        return 0;
    }

    uint32_t current = 0;  // Start at first machine

    // Simulate: show current, then NEXT, NEXT, PREVIOUS
    const char* actions[] = { "Initial", "NEXT", "NEXT", "PREVIOUS" };
    int deltas[]          = {  0,         +1,     +1,     -1         };

    for (int step = 0; step < 4; step++) {
        // Apply delta with wraparound
        current = (current + deltas[step] + total) % total;

        printf("[%s] -> Machine %u/%u:\n", actions[step], current + 1, total);
        print_machine_info(db, &results[current], current, total, size_pow2);
        printf("\n");
    }

    // ========================================================================
    // Typical Pico 2 button loop (pseudo-code)
    // ========================================================================
    /*
    uint32_t current_machine = 0;

    while (1) {
        // Display current machine on OLED
        const char* name = mrdb_get_machine_name(db, results[current_machine].machine_id);
        const char* rom  = mrdb_get_rom_name(db, results[current_machine].rom_name_id);
        oled_display(name, rom, current_machine + 1, total);

        // Wait for button press
        if (button_next_pressed()) {
            current_machine = (current_machine + 1) % total;
        }
        if (button_prev_pressed()) {
            current_machine = (current_machine + total - 1) % total;
        }
    }
    */

    return 0;
}
