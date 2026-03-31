from abc import ABC
from typing import List
import pandas as pd

from qshub.transforms.transform import Transform

class BasePipeline(ABC):
    
    def __init__(self, steps: List[Transform] = None):
        self.steps = steps or []


    def add_step(self, step: Transform):
        self.steps.append(step)


    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            df = step.run(df)
        return df