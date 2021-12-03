#import os
import requests
import json
from urllib.parse import urlparse
import boto3
from PyPDF2 import PdfFileReader
from io import BytesIO
from docx import Document

#os.environ['APIKEY'] = '8511cad6-5ab3-4e4c-baa6-ecde8c4c7cbf'

print('Loading function')

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='AKIAYBIRKH3K2RGNYKE3',
    aws_secret_access_key='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'
)

import requests
import threading
import time
from flask import Flask
app = Flask(__name__)

@app.before_first_request
def activate_job():
    def run_job():
        for obj in s3.Bucket('duke-sandbox-bucket').objects.all(): # goes through each document in the s3 bucket
            # print(obj)
            #os.environ['URL'] = 'https://uesu3rp516.execute-api.us-east-1.amazonaws.com/api/utility/documenttext/processed?file='+obj.key+'&bucket='+obj.bucket_name
            try:
                # get object from object summary
                object = s3.Bucket(obj.bucket_name).Object(obj.key).get()
                print(object["ContentType"])
                fs = object['Body'].read()
                ret = ""
                # sends get request to endpoint to check if document has already been processed
                url = 'https://uesu3rp516.execute-api.us-east-1.amazonaws.com/api/utility/documenttext/processed?file='+obj.key+'&bucket='+obj.bucket_name
                headers = {
                    'apikey': '8511cad6-5ab3-4e4c-baa6-ecde8c4c7cbf',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
                }
                getR = requests.get(url, headers = headers)
                print(getR.json())
                if(not getR):
                    if object["ContentType"] == "application/pdf":
                        # if so, uses PyPDF2 to read each page and concats into single string
                        pdf = PdfFileReader(BytesIO(fs))
                        for i in range(0, pdf.numPages):
                            ret = ret + pdf.getPage(i).extractText().replace('\n','')
                    elif object["ContentType"]=="application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                        doc = Document(BytesIO(fs))
                        for para in doc.paragraphs:
                            ret = ret + para.text.replace('\n','')
                    print(ret)
                    body = {
                       "bucket": obj.bucket_name,
                       "fileName": obj.key,
                       "text": ret
                    }
                    obj = json.dumps(body)
                    postR = requests.post('https://uesu3rp516.execute-api.us-east-1.amazonaws.com/api/utility/documenttext', headers = {'apikey': os.environ.get('APIKEY')}, json = obj)
                    postR.json()
            except:
                pass
    thread = threading.Thread(target=run_job)
    thread.start()

@app.route("/")
def hello():
    return "Hello World!"


def start_runner():
    def start_loop():
        not_started = True
        while not_started:
            print('In start loop')
            try:
                r = requests.get('http://127.0.0.1:5000/')
                if r.status_code == 200:
                    print('Server started, quiting start_loop')
                    not_started = False
                print(r.status_code)
            except:
                print('Server not yet started')
            time.sleep(2)

    print('Started runner')
    thread = threading.Thread(target=start_loop)
    thread.start()

if __name__ == "__main__":
    start_runner()
    app.run()
