import pathlib
import os


def reformat_value(val: str):
    return val \
        .replace('(', '') \
        .replace(')', '') \
        .replace('/', '_') \
        .replace('.', '_') \
        .replace(' ', '_') \
        .replace('-', '_') \
        .replace('&', '') \
        .replace('%', '') \
        .replace("'", '')


def reformat_dict(row: dict):
    pairs = []
    nrow = {}

    for k, v in row.items():
        new_key = reformat_value(k)
        pairs.append([new_key, v])
    for k, v in pairs:
        nrow[k] = v

    return nrow


def currentDir(_file_, path):
    return str(pathlib.PurePath(_file_).parent.joinpath(path))


def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))

