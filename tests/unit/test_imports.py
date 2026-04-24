import importlib
import os
import subprocess
import sys


MODULES = [
    "tafsir_gui.main",
    "tafsir_gui.core.runner",
    "tafsir_gui.core.preflight",
    "tafsir_gui.core.scheduler",
    "tafsir_gui.core.events",
    "tafsir_gui.core.state",
    "tafsir_gui.core.adapters",
    "tafsir_gui.integrations.gemini",
    "tafsir_gui.utils.env",
    "tafsir_gui.utils.logging",
    "tafsir_gui.ui.pages.gcp_setup",
    "tafsir_gui.ui.pages.run",
    "tafsir_gui.ui.pages.artifacts",
    "tafsir_gui.utils.metadata",
]


def test_import_modules():
    for mod in MODULES:
        importlib.import_module(mod)


def test_module_entrypoint_invocation():
    env = os.environ.copy()
    env.pop("PYTEST_CURRENT_TEST", None)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "tafsir_gui.main", "--test-mode"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired as exc:
        assert "Traceback" not in (exc.stderr or "")
    else:
        assert result.returncode == 0 or "NiceGUI ready" in result.stdout
