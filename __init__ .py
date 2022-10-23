import logging
import azure.functions as func 
from azure.storage.blob import ContainerClient
import sys
import os
from sys import path
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
from azure.storage.blob import BlobServiceClient
from azurefunctioncustom import *


def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')
    file = req.params.get('file')
    if not file:

        return func.HttpResponse(

            "Warning!, missing parameter",

            status_code=200

        )

    if file:

        result = clean(file)
        
        return func.HttpResponse(result, status_code=200)


        

        
