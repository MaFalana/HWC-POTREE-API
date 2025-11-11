import os
import tempfile
import shutil
from datetime import datetime, timedelta

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import uvicorn

from azure.storage.blob import (BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions)
from pymongo import MongoClient
from dotenv import load_dotenv

from storage.az import AzureStorageManager
from storage.db import DatabaseManager

load_dotenv()

print("Environment variables loaded.\n")

DB = DatabaseManager()
#AZ = AzureStorageManager(DB.name)

