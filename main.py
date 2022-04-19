import csv
import traceback
import requests
from bs4 import BeautifulSoup
import wget
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import boto.s3.connection
from io import StringIO
import boto3
import pandas as pd
import json
import logging

logging.basicConfig(filename='StellEye.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def getXML(link):  # Ex1
    try:
        response = requests.get(link)  # Code based on this Website https://pybit.es/articles/download-xml-file/
    except Exception:
        logging.exception("Error: ")
        raise
    with open('data.xml', 'wb') as file:
        file.write(response.content)
    return logging.info("Ex1: XML download")


def find_download(filepath):  # Ex2
    try:
        tree = ET.parse(filepath)
    except Exception:
        logging.exception("Error: ")
        raise
    root = tree.getroot()

    for i in root.findall("./result/doc"):
        if i.find("str[@name='file_type']").text == 'DLTINS':
            download_link_text=i.find("str[@name='download_link']").text
            break
    try:
        wget.download(download_link_text, "important_file.zip")
    except Exception:
        logging.exception("Error: ")
        raise
    return logging.info("Ex2: Zip downloaded")


def unzip_download(file):  # Ex3
    try:
        with ZipFile(file, "r") as zip_ref:
            zip_ref.extractall()
    except Exception:
        logging.exception("Error: ")
        raise
    return logging.info("Ex3: Zip extracted")


def write_xml_to_csv(xml_file):  # Ex4
    try:
        tree = ET.parse(xml_file)
    except Exception:
        logging.exception("Error: ")
        raise
    root = tree.getroot()
    # CREATE CSV FILE
    try:
        csvfile = open("data.csv", 'w', encoding='utf-8', newline='')
    except Exception:
        logging.exception("Error: ")
        raise
    csvfile_writer = csv.writer(csvfile)
    csvfile_writer.writerow(["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp",
                             "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"])

    for entry in root.iter('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}TermntdRcrd'):
        try:
            Id = entry.find(
                './{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id').text
            FullNm = entry.find(
                './{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm').text
            ClssfctnTp = entry.find(
                './{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp').text
            CmmdtyDerivInd = entry.find(
                './{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd').text
            NtnlCcy = entry.find(
                './{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy').text
            Issr = entry.find('./{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr').text
            csv_line = [Id, FullNm, ClssfctnTp, CmmdtyDerivInd, NtnlCcy, Issr]
            csvfile_writer.writerow(csv_line)
        except Exception:
            logging.exception("Error: ")
            raise
    csvfile.close()
    return logging.info("Ex4: Csv created")


def aws_s3_bucket(csv_file):  # Ex5
    AWSAccessKeyId = "AKIAWQD6BQBNGNIKGSNF"  # code was based on this video content
    AWSSecretKey = "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"

    try:
        conn = boto.connect_s3(
            aws_access_key_id=AWSAccessKeyId,
            aws_secret_access_key=AWSSecretKey
        )

        conn.create_bucket("steeleyeex5")
    except Exception:
        logging.exception("Error: ")
        raise

    data = pd.read_csv(csv_file)

    s3 = boto3.client('s3', aws_access_key_id=AWSAccessKeyId,
                      aws_secret_access_key=AWSSecretKey
                      )
    csv_buf = StringIO()
    data.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    try:
        s3.put_object(Bucket="steeleyeex5", Body=csv_buf.getvalue(), Key="steeleyeex5.csv")
    except Exception:
        logging.exception("Error: ")
        raise
    return logging.info("Ex5: Csv stored in AWS S3 bucket")



if __name__ == '__main__':
    getXML(r'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100')
    find_download("data.xml")
    unzip_download("important_file.zip")
    write_xml_to_csv('DLTINS_20210117_01of01.xml')
    aws_s3_bucket("data.csv")
