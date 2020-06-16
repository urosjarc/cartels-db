import pathlib

def currentDir(_file_, path):
    return str(pathlib.PurePath(_file_).parent.joinpath(path))
