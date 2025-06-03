import os


def ensure_directories(*dirs):
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
