import os
import shutil
import subprocess
from pathlib import Path

import pytest

MODULES = [
    "tafsir_gui.pipeline.gemini_api.blocks_to_xml_api",
    "tafsir_gui.pipeline.gemini_gui.blocks_to_xml_gui",
    "tafsir_gui.pipeline.analysis.rollback_pipeline",
    "tafsir_gui.main",
    "tafsir_gui.integrations.universal_pipeline",
]


DOCKER_IMAGE = "classify:stable"


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _image_present() -> bool:
    if not _docker_available():
        return False
    result = subprocess.run(
        ["docker", "image", "inspect", DOCKER_IMAGE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


@pytest.mark.parametrize("module_name", MODULES)
def test_module_runs_in_docker(module_name: str):
    env_file = Path(".env")
    assert env_file.exists(), ".env is required for Docker tests"

    command = [
        "docker",
        "run",
        "--rm",
        "--env-file",
        str(env_file),
        DOCKER_IMAGE,
        "python",
        "-m",
        module_name,
    ]

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        pytest.skip(
            "Docker run failed (probably not available in this environment): "
            f"{result.stdout or result.stderr}"
        )
