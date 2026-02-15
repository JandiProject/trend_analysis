from models.db_models import *
from services.service_db import engine, Base
Base.metadata.create_all(bind=engine)