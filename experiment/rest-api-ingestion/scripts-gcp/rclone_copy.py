"""Rclone copy runner for Dataproc batch execution.

This script copies data from Azure Blob Storage to GCS using rclone.
All rclone remote configuration is supplied via environment variables
(RCLONE_CONFIG_<REMOTE>_<KEY>) — no rclone.conf is needed on the worker.

The script will attempt to install rclone if it is not already on PATH,
making it safe to run on vanilla Dataproc images.

Required environment variables:
    RCLONE_CONFIG_AZURE_TYPE     azureblob
    RCLONE_CONFIG_AZURE_ACCOUNT  Azure storage account name
    RCLONE_CONFIG_AZURE_KEY      Azure storage account key

The GCS remote relies on Application Default Credentials (Dataproc service
account), so no explicit RCLONE_CONFIG_GOOGLE_* vars are required for auth.

Usage (submitted as Dataproc PySpatch main or run locally):
    python rclone_copy.py \\
        --source      azure:bronze/POPUS/ \\
        --destination google:my-bucket/v3/data \\
        --transfers   16 \\
        --checkers    32
"""

from __future__ import annotations

import argparse
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rclone_copy")

RCLONE_VERSION = "v1.67.0"


# ── rclone installation ────────────────────────────────────────────────────────

def _rclone_binary() -> str:
    """Return the path to the rclone binary, installing it if necessary.

    Returns:
        Absolute path to the rclone executable.

    Raises:
        RuntimeError: If installation fails.
    """
    path = shutil.which("rclone")
    if path:
        logger.info("rclone found at %s", path)
        return path

    logger.info("rclone not on PATH — installing %s", RCLONE_VERSION)

    system = platform.system().lower()
    machine = platform.machine().lower()

    arch = "amd64"
    if machine in ("aarch64", "arm64"):
        arch = "arm64"

    if system == "linux":
        os_name = "linux"
    elif system == "darwin":
        os_name = "osx"
    else:
        raise RuntimeError(f"Unsupported OS for auto-install: {system}")

    filename = f"rclone-{RCLONE_VERSION}-{os_name}-{arch}.zip"
    url = f"https://github.com/rclone/rclone/releases/download/{RCLONE_VERSION}/{filename}"

    install_dir = tempfile.mkdtemp(prefix="rclone_")
    zip_path = os.path.join(install_dir, filename)

    logger.info("Downloading %s", url)
    urllib.request.urlretrieve(url, zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(install_dir)

    binary = os.path.join(
        install_dir,
        f"rclone-{RCLONE_VERSION}-{os_name}-{arch}",
        "rclone",
    )
    os.chmod(binary, 0o755)
    logger.info("rclone installed at %s", binary)
    return binary


# ── Config validation ──────────────────────────────────────────────────────────

def _validate_env() -> None:
    """Assert that required rclone Azure environment variables are set.

    Raises:
        EnvironmentError: If any required variable is missing.
    """
    required = [
        "RCLONE_CONFIG_AZURE_TYPE",
        "RCLONE_CONFIG_AZURE_ACCOUNT",
        "RCLONE_CONFIG_AZURE_KEY",
    ]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            "Set RCLONE_CONFIG_AZURE_TYPE, RCLONE_CONFIG_AZURE_ACCOUNT, "
            "and RCLONE_CONFIG_AZURE_KEY before running this script."
        )


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="rclone copy: Azure Blob → GCS")

    parser.add_argument(
        "--source",
        required=True,
        help="Rclone source path (e.g. azure:container/path/)",
    )
    parser.add_argument(
        "--destination",
        required=True,
        help="Rclone destination path (e.g. google:bucket/path)",
    )
    parser.add_argument(
        "--transfers",
        type=int,
        default=16,
        help="Number of parallel file transfers (default: 16)",
    )
    parser.add_argument(
        "--checkers",
        type=int,
        default=32,
        help="Number of parallel checkers (default: 32)",
    )
    parser.add_argument(
        "--bwlimit",
        type=str,
        default="",
        help="Bandwidth limit (e.g. '100M'). Empty = unlimited.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be copied without copying them",
    )
    return parser.parse_args()


# ── rclone execution ───────────────────────────────────────────────────────────

def build_command(binary: str, args: argparse.Namespace) -> list[str]:
    """Assemble the rclone copy command.

    Args:
        binary: Path to the rclone executable.
        args: Parsed CLI arguments.

    Returns:
        List of command tokens.
    """
    cmd = [
        binary, "copy",
        args.source,
        args.destination,
        "--progress",
        "--metadata",
        "--use-server-modtime",
        "--fast-list",
        "--transfers", str(args.transfers),
        "--checkers", str(args.checkers),
        "--log-level", "INFO",
        "--stats", "30s",
        "--retries", "3",
        "--low-level-retries", "10",
    ]

    if args.bwlimit:
        cmd += ["--bwlimit", args.bwlimit]

    if args.dry_run:
        cmd.append("--dry-run")

    return cmd


def run_rclone(cmd: list[str]) -> int:
    """Execute rclone and stream its output to the logger.

    Args:
        cmd: Full command as a list of tokens.

    Returns:
        rclone process exit code.
    """
    logger.info("Executing: %s", " ".join(cmd))

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    for line in process.stdout:
        stripped = line.rstrip()
        if stripped:
            logger.info("[rclone] %s", stripped)

    process.wait()
    return process.returncode


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 = success).
    """
    args = parse_arguments()

    logger.info("=== rclone copy started ===")
    logger.info("Source      : %s", args.source)
    logger.info("Destination : %s", args.destination)
    logger.info("Transfers   : %d", args.transfers)
    logger.info("Checkers    : %d", args.checkers)
    logger.info("Dry run     : %s", args.dry_run)

    _validate_env()

    try:
        binary = _rclone_binary()
    except RuntimeError as exc:
        logger.error("Failed to obtain rclone binary: %s", exc)
        return 1

    cmd = build_command(binary, args)
    exit_code = run_rclone(cmd)

    if exit_code == 0:
        logger.info("=== rclone copy completed successfully ===")
    else:
        logger.error("=== rclone copy FAILED (exit code %d) ===", exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
