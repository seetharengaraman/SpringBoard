#!/usr/bin/env python
"""
This module is to setup cloud storage bucket and use it to ingest and process stock
market data
"""
from google.cloud import storage
from google.oauth2 import service_account
import logging

class CloudSetup:
    def __init__(self,bucket_name):
        self.credentials = service_account.Credentials.from_service_account_file(
    'token.json')
        self.storage_client = storage.Client(credentials=self.credentials)
        self.bucket_name = bucket_name
        log_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
        logging.basicConfig(level=logging.INFO,
                        filename='Trade_Analysis.logs',
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
        self.create_gcp_bucket()
    
    def create_gcp_bucket(self):
        try:
            bucket = self.storage_client.get_bucket(self.bucket_name)
            logging.info(f"Bucket {bucket} present")
        except Exception as e:
            bucket = self.storage_client.create_bucket(self.bucket_name,location='us')
            logging.info(f"Bucket {bucket} created")
    
    def gcs_check_file_exists(self,folder_position,folder_suffix,previous_folder='trade'):
        self.object_exists = False
        blobs = self.storage_client.list_blobs(self.bucket_name)
        for blob in blobs:
            parts = blob.name.split('/')
            if ((parts[folder_position-2] == previous_folder) and (parts[folder_position-1].endswith(folder_suffix))):
                logging.info(f"Object {blob.name} successfully created")
                break