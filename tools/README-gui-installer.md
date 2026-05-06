# GUI Installer (PyInstaller)

This repository includes `tools/gui_installer.py`, a simple desktop installer assistant that can:

- Check for required build tools.
- Install missing tools with the detected package manager.
- Run `./waf configure`, `./waf`, and `sudo ./waf install`.

## Run directly

```bash
python3 tools/gui_installer.py
```

## Build standalone app with PyInstaller

```bash
pip install pyinstaller
pyinstaller tools/gui_installer.spec
```

Output binary:

- `dist/ardour-gui-installer`

## Notes

- The installer executes package-manager commands via `sudo`.
- Package names currently match generic tool names; distro-specific package mapping may need customization.
- Intended as a convenience helper, not a replacement for official distro packaging.
