#!/usr/bin/env python3
# coding: utf-8

import contextlib
from glob import glob
import os
import shutil
import stat
from subprocess import PIPE
from subprocess import run
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile

BUILD = 'executables'
PKGS = BUILD + '/pkgs'
SRC = BUILD + '/src'
TOOL_PYZ = BUILD + '/bead.pyz'
UNIX_TOOL = BUILD + '/bead'
WIN_TOOL = BUILD + '/bead.cmd'


def mkdir(dir):
    if not os.path.isdir(dir):
        print(f'mkdir {dir}')
        os.makedirs(dir)


def pip(*args):
    print(f'pip {" ".join(args)}')
    return run(('pip',) + args, check=True)


def pip_download_source(*args):
    return pip('download', '--no-binary', ':all:', *args)


def rmtree(dir):
    print(f'rm -rf {dir}')
    shutil.rmtree(dir, ignore_errors=True)


def make_executable(file):
    print(f'chmod +x {file}')
    st = os.stat(file)
    os.chmod(file, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


@contextlib.contextmanager
def notification(msg, long_output=False):
    if long_output:
        print(msg + ':')
        print()
        print('-' * 32)
    else:
        print(' * ' + msg)
    try:
        yield
    finally:
        if long_output:
            print('-' * 32)
            print()


def further_output(msg):
    return notification(msg, long_output=True)


progress = notification

# start with no build directory
with further_output('Clean up'):
    rmtree(BUILD)

git_ls_files = run(['git', 'ls-files'], stdout=PIPE, check=True).stdout.decode('utf-8')

PY_SOURCES = [
    line
    for line in git_ls_files.splitlines()
    if line.endswith('.py')
]

with further_output('Copying over our sources'):
    PY_DIRS = sorted({os.path.dirname(file) for file in PY_SOURCES})
    for dir in PY_DIRS:
        mkdir(os.path.join(SRC, dir))
    for file in PY_SOURCES:
        shutil.copy(file, os.path.join(SRC, file))

with further_output('Downloading dependencies'):
    mkdir(PKGS)
    pip_download_source('--dest', PKGS, '--exists-action', 'w', '-r', 'requirements.txt')

with further_output('Unpacking packages'):
    mkdir(SRC)
    for package in glob(PKGS + '/*'):
        pip('install', '--target', SRC, '--no-compile', '--no-deps', package)

# # Technically we do not need these files,
# # however licensing forces us to copy and keep them :(
# for dir in glob(SRC + '/*.egg-info'):
#     rmtree(dir)

with progress(f'Creating .pyz zip archive from the sources ({TOOL_PYZ})'):
    with ZipFile(TOOL_PYZ, mode='w', compression=ZIP_DEFLATED) as zip:
        for realroot, dirs, files in os.walk(SRC):
            ziproot = os.path.relpath(realroot, SRC)
            for file_name in files:
                zip.write(
                    os.path.join(realroot, file_name),
                    os.path.join(ziproot, file_name))


def make_tool(tool_file_name, runner):
    with open(tool_file_name, 'wb') as f:
        f.write(runner)
        with open(TOOL_PYZ, 'rb') as pyz:
            f.write(pyz.read())


with progress(f'Creating unix tool ({UNIX_TOOL})'):
    UNIX_RUNNER = b'#!/usr/bin/env python3\n# PYTHON_ARGCOMPLETE_OK\n'

    make_tool(UNIX_TOOL, UNIX_RUNNER)
    make_executable(UNIX_TOOL)

with progress(f'Creating windows tool ({WIN_TOOL})'):
    WINDOWS_RUNNER = b'\r\n'.join((
        b'@echo off',
        b'python3.exe "%~f0" %*',
        b'exit /b %errorlevel%',
        b''))

    make_tool(WIN_TOOL, WINDOWS_RUNNER)

print('Done.')
