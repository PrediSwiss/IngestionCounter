import pytest
import os

from google.cloud import storage
from main import create_bucket, get_data, UrlException, HeadersException

class TestIngestion:
    url1 = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/Pull"
    url2 = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/asdPull"
    url3 = "https://api.opentransportdata.swiss/TDP/Soap_Datex2/PullThisIsDumb"

    headers1 = {
        'Content-Type': 'text/xml; charset=utf-8',
        'Authorization': os.environ.get("OPENTRANSPORT_CREDENTIAL"),
        'SOAPAction': 'http://opentransportdata.swiss/TDP/Soap_Datex2/Pull/v1/pullMeasurementSiteTable' 
    }

    headers2 = {
        'Content-Type': 'text/xml; charset=utf-8',
        'Authorization': os.environ.get("OPENTRANSPORT_CREDENTIAL") + "sdasda",
        'SOAPAction': 'http://opentransportdata.swiss/TDP/Soap_Datex2/Pull/v1/pullMeasurementSiteTable' 
    }

    def test_get_data_url_error(self):
        with pytest.raises(UrlException):
            get_data(self.url2, self.headers1)

    def test_get_data_correct(self):
        assert get_data(self.url1, self.headers1) != ""

    def test_get_data_after_url_correct(self):
        try:
            get_data(self.url3, self.headers1)
        except UrlException:
            assert False

    def test_get_data_headers_error(self):
        with pytest.raises(HeadersException):
            get_data(self.url1, self.headers2) 

class TestBucket:
    bucket_name = "prediswiss_test_bucket"
    blob_name = "test"
    blob_type = "text/xml"
    blob_data = "<test>test</test>"
    storage_client = storage.Client(project="prediswiss")

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        yield
        try:
            bucket = self.storage_client.get_bucket(self.bucket_name)
            bucket.delete(force=True)
        except:
            print("ok : no bucket for test")

    def test_create_bucket(self):
        create_bucket(self.bucket_name, self.storage_client)
        try:
            self.storage_client.get_bucket(self.bucket_name)
        except Exception:
            assert False