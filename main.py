import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import gcsfs
from google.cloud import storage
import functions_framework
import os
import pyarrow.parquet as pq

bucket_name = "prediswiss-network"
file_name = "network.parquet"
url = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/Pull"

headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'Authorization': os.environ.get("OPENTRANSPORT_CREDENTIAL"),
    'SOAPAction': 'http://opentransportdata.swiss/TDP/Soap_Datex2/Pull/v1/pullMeasurementSiteTable' 
}

@functions_framework.cloud_event
def ingestion_counter(cloud_event):
    storage_client = storage.Client(project="prediswiss")
    fs_gcs = gcsfs.GCSFileSystem(project='prediswiss')

    try:
        bucket = storage_client.get_bucket(bucket_name)
    except:
        bucket = create_bucket(bucket_name, storage_client)

    data = toParquet(get_data(url=url, headers=headers))
    if fs_gcs.exists(f"{bucket_name}/{file_name}"):
        fs_gcs.rm_file(f"{bucket_name}/{file_name}")
    pq.write_to_dataset(data, root_path="gs://" + bucket_name + "/" + file_name, filesystem=fs_gcs)

def create_bucket(name, client: storage.Client):    
    bucket = client.create_bucket(name, location="us-east1")
    print(f"Bucket {name} created")
    return bucket

def get_data(url, headers):
    filePayload = open("request.xml", "r")
    payload = filePayload.read()

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 404:
        raise UrlException
    if response.status_code == 403:
        raise HeadersException
    if response.status_code != 200:
        raise NotSupportedException

    element = ET.XML(response.text)
    ET.indent(element)

    return ET.tostring(element, encoding='unicode')

def toParquet(data):
    namespaces = {
    'ns0': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns1': 'http://datex2.eu/schema/2/2_0',
    }


    dom = ET.fromstring(data)

    pubDate = dom.find(
        './ns0:Body'
        '/ns1:d2LogicalModel'
        '/ns1:payloadPublication'
        '/ns1:publicationTime',
        namespaces
    )

    sites = dom.findall(
        './ns0:Body'
        '/ns1:d2LogicalModel'
        '/ns1:payloadPublication'
        '/ns1:measurementSiteTable'
        '/ns1:measurementSiteRecord',
        namespaces
    )

    locSitesElem = [(site.find(
        './ns1:measurementSiteLocation'
        '/ns1:pointByCoordinates'
        '/ns1:pointCoordinates'
        '/ns1:latitude',
        namespaces
    ), site.find(
        './ns1:measurementSiteLocation'
        '/ns1:pointByCoordinates'
        '/ns1:pointCoordinates'
        '/ns1:longitude',
        namespaces
    ), site.find(
        './ns1:measurementSiteLocation'
        '/ns1:supplementaryPositionalDescription'
        '/ns1:affectedCarriagewayAndLanes'
        '/ns1:lane',
        namespaces
    ), site.get("id")) for site in sites]

    locSites = []
    for loc in locSitesElem:
        if loc[0] != None:
            if loc[1] != None:
                locSites.append((loc[0].text, loc[1].text, loc[2].text, loc[3]))

    dataframe = pd.DataFrame()

    columns = ["lat", "long", "line", "id"]
    dataframe = pd.concat([pd.DataFrame(data=locSites, columns=columns), dataframe])
    table = pa.Table.from_pandas(dataframe)

    return table


class UrlException(Exception):
    "Raised when url is not correct"
    pass

class HeadersException(Exception):
    "Error in headers"
    pass

class NotSupportedException(Exception):
    "Exception not supported"
    pass