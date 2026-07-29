# -*- mode: python -*-

import os
import yaml

confpath = os.getenv('TOOLS_CONF', './tools.yaml')
try:
    with open(confpath, 'rb') as conffile:
        toolconf = yaml.safe_load(conffile.read())
except Exception as e:
    print(f'Failed to load {confpath}. {repr(e)}')
    exit(1)

a_kwargs = {
    'noarchive': False,
    'runtime_hooks': ['restore_ld_library_path.py'],
}

for tool, tconf in toolconf['tools'].items():
    print(f'Generating one-file binary for: {tool}')

    a = Analysis(
        [tconf['source']],
        **a_kwargs
    )

    pyz = PYZ(
        a.pure,
        a.zipped_data,
        cipher=None
    )

    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.zipfiles,
        a.datas,
        [],
        name=tool,
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        upx_exclude=[],
        runtime_tmpdir=None,
        console=True
    )
