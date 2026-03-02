import duckdb
from pathlib import Path


ROOT_FOLDER = Path(__file__).parents[4]
DATA_FOLDER = ROOT_FOLDER / 'data' / 'raw' / 'downloads'


csv_files = list(DATA_FOLDER.glob('*.csv'))


