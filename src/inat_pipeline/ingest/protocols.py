from typing import Protocol

import pandas as pd


class DataSource(Protocol):
    def read(self) -> pd.DataFrame:
        pass
