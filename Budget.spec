# Budget.spec
# PyInstaller spec — works on both Windows and Mac.
# Run with: pyinstaller Budget.spec --noconfirm

import sys
import os
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# ── Collect pdfplumber and pdfminer data files ────────────────────────────────
datas = []
datas += collect_data_files('pdfplumber')
datas += collect_data_files('pdfminer')

# ── Bundle all read-only assets into MEIPASS ─────────────────────────────────
# These land in sys._MEIPASS (BUNDLE_DIR) at runtime
datas += [
    ('budget.html',  '.'),       # frontend
    ('run.py',       '.'),       # parser entry point (run in-process when frozen)
    ('src',          'src'),     # parsers, mapping, io_utils
]
if os.path.exists('Budget.ico'):
    datas += [('Budget.ico', '.')]

# ── Hidden imports ────────────────────────────────────────────────────────────
hidden_imports = [
    'flask',
    'flask_cors',
    'pdfplumber',
    'pdfminer',
    'pdfminer.high_level',
    'pdfminer.layout',
    'pdfminer.pdfinterp',
    'pdfminer.converter',
    'pdfminer.pdfpage',
    'pandas',
    'pandas._libs.tslibs.np_datetime',
    'pandas._libs.tslibs.nattype',
    'pandas._libs.tslibs.timedeltas',
    'numpy',
    'yfinance',
    'pkg_resources',
    'charset_normalizer',
    'charset_normalizer.md__mypyc',
    'werkzeug',
    'werkzeug.serving',
    'jinja2',
    'click',
    'itsdangerous',
    'runpy',
]
hidden_imports += collect_submodules('pdfminer')
hidden_imports += collect_submodules('flask')

a = Analysis(
    ['app.py'],
    pathex=['.'],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'PIL', 'PyQt5', 'wx', 'IPython'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='Budget',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='Budget.ico' if os.path.exists('Budget.ico') else None,
)

# macOS: also build a .app bundle
app = BUNDLE(
    exe,
    name='Budget.app',
    icon='Budget.icns' if os.path.exists('Budget.icns') else None,
    bundle_identifier='com.budget.app',
    info_plist={
        'NSHighResolutionCapable': True,
        'LSUIElement': False,
        'CFBundleShortVersionString': '1.0.0',
        'NSAppTransportSecurity': {'NSAllowsArbitraryLoads': True},
    },
)
