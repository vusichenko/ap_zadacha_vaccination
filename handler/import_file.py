from typing import Union
from pathlib import Path

import pandas as pd


class FileImporter:
    def __init__(self, file: Union[str, Path], chunk_size: int = None):
        self.path = Path(file)
        if not self.path.is_file():
            raise FileExistsError(f"File {file} does not exists!")
        self.chunk_size = chunk_size if chunk_size is not None else 10 ** 4

    def read_csv(self, concat: bool = False, **kwargs):
        chunks = []
        print(f"IMP: Reading file {self.path}")

        with pd.read_csv(self.path, chunksize=self.chunk_size, **kwargs) as parser:
            for chunk in parser:
                chunks.append(chunk)

        if concat:
            return pd.concat(chunks)
        return chunks
