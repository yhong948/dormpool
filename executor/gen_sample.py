"""
executor/gen_sample.py  —  deterministic synthetic sample generator
===================================================================
Owner: Person C.

Creates a flat zip of deterministic synthetic PNGs, used by the gates and by
anyone who wants a reproducible input without shipping binary fixtures.

  python executor/gen_sample.py --count 12 --out executor/samples/sample12.zip

Same arguments always produce byte-identical bytes: the RNG is seeded from the
image index (never from time), and no timestamps are written into the archive.
"""

from __future__ import annotations

import argparse
import io
import os
import random
import zipfile

from PIL import Image

SIZES = [(320, 240), (200, 300), (128, 128), (512, 64), (256, 256)]
FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)


def _make_image(index: int) -> Image.Image:
    rng = random.Random(20260101 + index)      # seeded per index, never by clock
    width, height = SIZES[index % len(SIZES)]
    img = Image.new("RGB", (width, height), (index * 17 % 256, index * 53 % 256, index * 97 % 256))
    pixels = img.load()
    step = max(1, min(width, height) // 16)
    for x in range(0, width, step):
        for y in range(0, height, step):
            pixels[x, y] = (x * 7 % 256, y * 13 % 256, (x * y) % 256)
    # Sprinkle a few seeded points so the two machines' outputs differ from a flat fill.
    for _ in range(24):
        px = rng.randrange(width)
        py = rng.randrange(height)
        pixels[px, py] = (rng.randrange(256), rng.randrange(256), rng.randrange(256))
    return img


def _zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=FIXED_DATE_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o600 << 16
    info.create_system = 0
    return info


def build(count: int, out_path: str) -> str:
    parent = os.path.dirname(os.path.abspath(out_path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for i in range(count):
            img = _make_image(i)
            buf = io.BytesIO()
            img.save(buf, format="PNG", pnginfo=None)
            zf.writestr(_zip_info(f"img_{i:03d}.png"), buf.getvalue())
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a deterministic sample zip")
    parser.add_argument("--count", type=int, default=12, help="number of images")
    parser.add_argument("--out", default="executor/samples/sample.zip", help="output zip path")
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be >= 1")

    path = build(args.count, args.out)
    print(f"wrote {path} with {args.count} images")


if __name__ == "__main__":
    raise SystemExit(main())
