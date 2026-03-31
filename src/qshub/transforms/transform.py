from dataclasses import dataclass
from typing import Callable, Dict, Any

import pandas as pd

@dataclass
class Transform:
    func: Callable[[pd.DataFrame, Dict[str, Any]], pd.DataFrame]
    params: Dict[str, Any] = None
    name: str = None

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.func(df, **(self.params or {}))
