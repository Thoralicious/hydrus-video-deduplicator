# -*- mode: python ; coding: utf-8 -*-


from PyInstaller.utils.hooks import collect_data_files
from hydrusvideodeduplicator import _native

a = Analysis(
    ['..\\..\\src\\hydrusvideodeduplicator\\cli_exe_entrypoint.py'],
    pathex=[],
    # Editable/source checkouts can keep the extension outside the source package.
    binaries=[(_native.__file__, "hydrusvideodeduplicator")],
    datas=collect_data_files("hydrusvideodeduplicator", includes=["_licenses/*"]),
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='hydrusvideodeduplicator-windows-x86_64',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['icon.ico'],
)
