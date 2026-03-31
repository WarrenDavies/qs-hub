from pathlib import Path

import pandas as pd
import os

class StorageManager():

    def __init__(self):
        self.read_funcs = {
            "csv": self.read_csv,
            "parquet": self.read_parquet
        }
        self.write_funcs = {
            "csv": self.write_csv,
            "parquet": self.write_parquet
        }


    def read(self, path, **kwargs):
        path = Path(path)
        extension = path.suffix.lstrip('.').lower()
        read_func = self.read_funcs.get(extension)
        if not read_func:
            raise ValueError(f"Unsupported file format: {extension}")
        df = read_func(path, **kwargs)
        return df


    def read_csv(self, path, **kwargs):
        return pd.read_csv(path)


    def read_parquet(self, path, **kwargs):
        return pd.read_parquet(path)


    def write(self, path, df, **kwargs):
        path = Path(path)
        extension = path.suffix.lstrip('.').lower()
        write_func = self.write_funcs.get(extension)
        if not write_func:
            raise ValueError(f"Unsupported file format: {extension}")

        if not self.exists(path):
            directory_path = path.parent
            directory_path.mkdir(parents=True, exist_ok=True)
        
        df = write_func(path, df, **kwargs)
        return df


    def write_csv(self, path, df, **kwargs):
        df.to_csv(path, index=False)


    def write_parquet(self, path, df, **kwargs):
        df.to_parquet(path, index=False)


    def exists(self, path):
        return os.path.exists(path)