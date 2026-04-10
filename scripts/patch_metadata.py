#!/usr/bin/env python3
"""
Patch DuckDB WASM extension metadata footer.

DuckDB extensions have a 512-byte footer at the end of the file:
  - First 256 bytes: 8 × 32-byte metadata fields (read then REVERSED by parser)
  - Last 256 bytes: RSA signature (ignored with allowUnsignedExtensions)

After reversal, field mapping:
  [0] = magic ("4" + 31 null bytes)
  [1] = platform (e.g., "wasm_eh")
  [2] = duckdb_version (e.g., "v1.4.3")
  [3] = extension_version
  [4] = ABI type ("CPP", "C_STRUCT", or "C_STRUCT_UNSTABLE")

Pre-reversal byte offsets (within the first 256 bytes of the footer):
  offset 224 → field[0] after reversal (magic)
  offset 192 → field[1] after reversal (platform)
  offset 160 → field[2] after reversal (duckdb_version)
  offset 128 → field[3] after reversal (extension_version)
  offset  96 → field[4] after reversal (ABI type)
  offsets 0-95 → fields[5-7] (unused, zeros)

Usage:
  python3 patch_metadata.py <extension.wasm> [--platform wasm_eh] [--version v1.4.3] [--abi CPP]
  python3 patch_metadata.py aggjoin.duckdb_extension.wasm
  python3 patch_metadata.py aggjoin.duckdb_extension.wasm --version v1.5.1
"""

import argparse
import sys


def patch_extension_metadata(filepath, platform="wasm_eh", version="v1.4.3",
                              ext_version=None, abi_type="CPP"):
    if ext_version is None:
        ext_version = version

    with open(filepath, "rb") as f:
        data = bytearray(f.read())

    if len(data) < 512:
        print(f"ERROR: File is only {len(data)} bytes, need at least 512", file=sys.stderr)
        sys.exit(1)

    # Verify WASM magic
    if data[:4] != bytes([0x00, 0x61, 0x73, 0x6d]):
        print("WARNING: File does not start with WASM magic bytes", file=sys.stderr)

    # Metadata is in the FIRST 256 bytes of the last 512 bytes
    meta_start = len(data) - 512

    def write_field(offset, value):
        """Write a null-padded 32-byte field at the given offset within metadata."""
        encoded = value.encode("ascii")
        if len(encoded) > 32:
            print(f"ERROR: Field value '{value}' exceeds 32 bytes", file=sys.stderr)
            sys.exit(1)
        field = encoded + b"\x00" * (32 - len(encoded))
        data[meta_start + offset : meta_start + offset + 32] = field

    # Clear the metadata area (first 256 bytes of footer)
    for i in range(256):
        data[meta_start + i] = 0

    # Write fields at their pre-reversal offsets
    write_field(224, "4")            # magic (field[0] after reversal)
    write_field(192, platform)       # platform (field[1] after reversal)
    write_field(160, version)        # duckdb_version (field[2] after reversal)
    write_field(128, ext_version)    # extension_version (field[3] after reversal)
    write_field(96, abi_type)        # ABI type (field[4] after reversal)

    with open(filepath, "wb") as f:
        f.write(data)

    # Verify by reading back
    meta = bytes(data[meta_start : meta_start + 256])
    fields = [meta[i * 32 : (i + 1) * 32] for i in range(8)]
    fields.reverse()

    names = ["magic", "platform", "version", "ext_ver", "abi_type"]
    print(f"Patched {filepath} ({len(data)} bytes):")
    for i, name in enumerate(names):
        val = fields[i].rstrip(b"\x00").decode("ascii")
        print(f"  {name:12s}: {val}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patch DuckDB WASM extension metadata")
    parser.add_argument("file", help="Path to .duckdb_extension.wasm file")
    parser.add_argument("--platform", default="wasm_eh", help="Platform (default: wasm_eh)")
    parser.add_argument("--version", default="v1.4.3", help="DuckDB version (default: v1.4.3)")
    parser.add_argument("--ext-version", default=None, help="Extension version (default: same as --version)")
    parser.add_argument("--abi", default="CPP", choices=["CPP", "C_STRUCT", "C_STRUCT_UNSTABLE"],
                        help="ABI type (default: CPP)")
    args = parser.parse_args()

    patch_extension_metadata(args.file, args.platform, args.version, args.ext_version, args.abi)
