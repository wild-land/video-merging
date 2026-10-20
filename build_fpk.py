#!/usr/bin/env python3
"""Build native fnOS packages for x86 and ARM from the official fnpack template."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "fpk" / "easynvr-merger"
DIST = ROOT / "dist"
VERSION = "1.0.5"
WHEELS = {
    "x86": (
        "manylinux2014_x86_64",
        "imageio_ffmpeg-0.6.0-py3-none-manylinux2014_x86_64.whl",
        "c7e46fcec401dd990405049d2e2f475e2b397779df2519b544b8aab515195282",
        "imageio_ffmpeg/binaries/ffmpeg-linux-x86_64-v7.0.2",
    ),
    "arm": (
        "manylinux2014_aarch64",
        "imageio_ffmpeg-0.6.0-py3-none-manylinux2014_aarch64.whl",
        "1d47bebd83d2c5fc770720d211855f208af8a596c82d17730aa51e815cdee6dc",
        "imageio_ffmpeg/binaries/ffmpeg-linux-aarch64-v7.0.2",
    ),
}


def wheel_for(arch: str, cache: Path) -> tuple[Path, str]:
    platform, filename, expected, binary = WHEELS[arch]
    cache.mkdir(parents=True, exist_ok=True)
    wheel = cache / filename
    if not wheel.exists():
        subprocess.run(
            [sys.executable, "-m", "pip", "download", "--no-deps",
             "--only-binary=:all:", "--platform", platform,
             "--dest", str(cache), "imageio-ffmpeg==0.6.0"],
            check=True,
        )
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    if digest != expected:
        raise RuntimeError(f"FFmpeg wheel 校验失败：{wheel}（SHA-256 {digest}）")
    return wheel, binary


def build(arch: str, fnpack: str, cache: Path) -> Path:
    wheel, binary = wheel_for(arch, cache)
    with tempfile.TemporaryDirectory(prefix=f"easynvr-fpk-{arch}-") as temporary:
        stage = Path(temporary) / "easynvr-merger"
        shutil.copytree(TEMPLATE, stage)
        for junk in stage.rglob(".DS_Store"):
            junk.unlink()
        server = stage / "app" / "server"
        server.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "app.py", server / "app.py")
        shutil.copytree(ROOT / "static", server / "static")
        manifest = stage / "manifest"
        manifest.write_text(
            re.sub(r"(?m)^platform\s*=.*$", f"platform              = {arch}",
                   manifest.read_text(encoding="utf-8")),
            encoding="utf-8",
        )
        with ZipFile(wheel) as package:
            ffmpeg = stage / "app" / "bin" / "ffmpeg"
            ffmpeg.parent.mkdir(parents=True)
            ffmpeg.write_bytes(package.read(binary))
            ffmpeg.chmod(0o755)
            licenses = stage / "app" / "licenses"
            licenses.mkdir(parents=True, exist_ok=True)
            (licenses / "imageio-ffmpeg-LICENSE").write_bytes(
                package.read("imageio_ffmpeg-0.6.0.dist-info/LICENSE"))
        subprocess.run([fnpack, "build", "--directory", str(stage)],
                       cwd=stage, check=True)
        candidates = list(stage.glob("*.fpk"))
        if len(candidates) != 1:
            raise RuntimeError(f"未找到唯一的 fpk 构建产物：{candidates}")
        DIST.mkdir(exist_ok=True)
        output = DIST / f"easynvr-merger-{VERSION}-{arch}.fpk"
        shutil.copy2(candidates[0], output)
        return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arches", nargs="*", choices=WHEELS.keys(),
                        default=list(WHEELS))
    args = parser.parse_args()
    fnpack = os.environ.get("FNPACK_BIN", "fnpack")
    cache = Path(os.environ.get("WHEEL_DIR", str(Path(tempfile.gettempdir()) / "easynvr-wheels")))
    for arch in args.arches:
        output = build(arch, fnpack, cache)
        print(f"{arch}: {output} ({output.stat().st_size / 1024 / 1024:.1f} MiB)")


if __name__ == "__main__":
    main()
