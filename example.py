from qshub.main import main
from qshub.utils import utils
from qshub.ingestion.ingestion_manager import IngestionManager

ingestion_manager = IngestionManager()
ingestion_manager.ingest_all()
