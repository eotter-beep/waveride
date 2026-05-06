#!/usr/bin/env python3
"""Simple GUI installer helper for Ardour developer builds.

This tool is intended to be bundled with PyInstaller and gives users a GUI to:
1) Check whether required external packages/tools exist.
2) Install missing packages using the detected package manager.
3) Run project configure/build/install commands.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import messagebox, ttk

REQUIRED_TOOLS = [
    "python3",
    "pkg-config",
    "gcc",
    "g++",
    "make",
]

OPTIONAL_TOOLS = [
    "ninja",
    "ccache",
]

PKG_MANAGERS = [
    ("apt-get", ["sudo", "apt-get", "update"], ["sudo", "apt-get", "install", "-y"]),
    ("dnf", ["sudo", "dnf", "makecache"], ["sudo", "dnf", "install", "-y"]),
    ("yum", ["sudo", "yum", "makecache"], ["sudo", "yum", "install", "-y"]),
    ("pacman", ["sudo", "pacman", "-Sy"], ["sudo", "pacman", "-S", "--needed", "--noconfirm"]),
    ("zypper", ["sudo", "zypper", "refresh"], ["sudo", "zypper", "install", "-y"]),
]


def detect_package_manager():
    for name, refresh_cmd, install_cmd in PKG_MANAGERS:
        if shutil.which(name):
            return name, refresh_cmd, install_cmd
    return None


class InstallerGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Ardour GUI Installer")
        self.root.geometry("860x560")

        self.status = tk.StringVar(value="Ready")
        self.pm_text = tk.StringVar(value="Package manager: detecting...")

        self._build_ui()
        self._show_package_manager()

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        top = ttk.Frame(main)
        top.pack(fill=tk.X)

        ttk.Label(top, textvariable=self.pm_text).pack(side=tk.LEFT)
        ttk.Button(top, text="Re-check", command=self.check_packages).pack(side=tk.RIGHT)

        cols = ("tool", "state", "path")
        self.tree = ttk.Treeview(main, columns=cols, show="headings", height=12)
        self.tree.heading("tool", text="Tool")
        self.tree.heading("state", text="Status")
        self.tree.heading("path", text="Resolved path")
        self.tree.column("tool", width=180)
        self.tree.column("state", width=110)
        self.tree.column("path", width=520)
        self.tree.pack(fill=tk.BOTH, expand=True, pady=(10, 8))

        btns = ttk.Frame(main)
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="Check packages", command=self.check_packages).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btns, text="Install missing", command=self.install_missing).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btns, text="Configure + Build", command=self.configure_and_build).pack(side=tk.LEFT)

        self.log = tk.Text(main, wrap=tk.WORD, height=12)
        self.log.pack(fill=tk.BOTH, expand=True, pady=(10, 8))

        ttk.Label(main, textvariable=self.status).pack(anchor="w")

        self.check_packages()

    def _show_package_manager(self) -> None:
        pm = detect_package_manager()
        if pm:
            self.pm_text.set(f"Package manager: {pm[0]}")
        else:
            self.pm_text.set("Package manager: not detected")

    def _append_log(self, text: str) -> None:
        self.log.insert(tk.END, text + "\n")
        self.log.see(tk.END)

    def _run(self, cmd: list[str]) -> int:
        self._append_log("$ " + " ".join(cmd))
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert proc.stdout is not None
        for line in proc.stdout:
            self._append_log(line.rstrip())
        return proc.wait()

    def _run_threaded(self, title: str, fn):
        def wrapped():
            try:
                self.status.set(title)
                fn()
                self.status.set("Done")
            except Exception as exc:  # noqa: BLE001
                self.status.set("Failed")
                self._append_log(f"ERROR: {exc}")
        threading.Thread(target=wrapped, daemon=True).start()

    def check_packages(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for tool in REQUIRED_TOOLS + OPTIONAL_TOOLS:
            path = shutil.which(tool)
            state = "OK" if path else ("Missing" if tool in REQUIRED_TOOLS else "Optional")
            self.tree.insert("", tk.END, values=(tool, state, path or ""))
        self.status.set("Package check complete")

    def _missing_required(self) -> list[str]:
        return [t for t in REQUIRED_TOOLS if shutil.which(t) is None]

    def install_missing(self) -> None:
        missing = self._missing_required()
        if not missing:
            messagebox.showinfo("Installer", "No required packages are missing.")
            return

        pm = detect_package_manager()
        if not pm:
            messagebox.showerror("Installer", "No supported package manager detected.")
            return

        name, refresh_cmd, install_cmd = pm
        if not messagebox.askyesno("Confirm install", f"Install missing packages with {name}?\n\n{', '.join(missing)}"):
            return

        def work():
            rc = self._run(refresh_cmd)
            if rc != 0:
                raise RuntimeError(f"Failed refreshing package metadata via {name}")
            rc = self._run(install_cmd + missing)
            if rc != 0:
                raise RuntimeError(f"Failed installing packages via {name}")
            self.check_packages()

        self._run_threaded("Installing missing packages...", work)

    def configure_and_build(self) -> None:
        def work():
            cmds = [
                ["./waf", "configure"],
                ["./waf"],
                ["sudo", "./waf", "install"],
            ]
            for cmd in cmds:
                rc = self._run(cmd)
                if rc != 0:
                    raise RuntimeError(f"Command failed: {' '.join(cmd)}")

        self._run_threaded("Running configure/build/install...", work)


def main() -> int:
    root = tk.Tk()
    app = InstallerGUI(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
