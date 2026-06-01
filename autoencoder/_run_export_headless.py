"""
Headless runner for the autoencoder export pipeline.
Neutralizes matplotlib's TkAgg backend + blocking plt.show() so the
export can run unattended (e.g. when regenerating autoencoder_results.json).
Usage:  python autoencoder/_run_export_headless.py
"""
import os
import sys
import importlib.util

os.environ["MPLBACKEND"] = "Agg"

import matplotlib
matplotlib.use("Agg", force=True)
# Neutralize later matplotlib.use("TkAgg") calls inside step modules
matplotlib.use = lambda *a, **k: None

import matplotlib.pyplot as plt
plt.show = lambda *a, **k: None          # never block on a GUI window
plt.pause = lambda *a, **k: None

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

sys.argv = ["export_results.py"]

spec = importlib.util.spec_from_file_location(
    "ae_export", os.path.join(HERE, "export_results.py")
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.main()
