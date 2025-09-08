# -*- mode: python ; coding: utf-8 -*-

"""
PyInstaller spec file for the Contextually Shell (`cx`).
This is configured for a true single-file executable build.
"""

added_files = [
    ('src/cx_shell/assets', 'cx_shell/assets'),
    ('src/cx_shell/interactive/grammar', 'cx_shell/interactive/grammar')
]

block_cipher = None

a = Analysis(
    ['src/cx_shell/main.py'],
    pathex=[],
    binaries=[],
    datas=added_files,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# The EXE object is now the final output of the build process.
exe = EXE(
    pyz,
    a.scripts,
    a.binaries, # We need to include binaries here
    a.zipfiles, # and zipfiles
    a.datas,    # and our data files
    [],
    name='cx',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None, # Creates a temporary directory for assets at runtime
    console=True,
)