import json
import logging
import urllib
from copy import deepcopy
from dataclasses import dataclass
from typing import List
from urllib.parse import urlparse, urljoin

import requests

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CorganizeClient:
    host: str
    apikey: str

    @property
    def _headers(self):
        return {"apikey": self.apikey}

    def update(self, file):
        url = urljoin(self.host, "/files")
        r = requests.patch(url, json=file, headers=self._headers)
        r.raise_for_status()

    def create(self, files: List[dict]):
        url = urljoin(self.host, "/files/bulk")
        r = requests.post(url, json=files, headers=self._headers)
        r.raise_for_status()
        return r.json()

    def _get_paginated_files(self, url: str, headers: dict = None, limit: int = 1000):
        return_files = list()

        if not headers:
            LOGGER.debug("headers not provided. Using the default dictionary...")
            headers = self._headers

        headers_deepcopy = deepcopy(headers)

        while True:
            r = requests.get(url, headers=headers_deepcopy)
            r.raise_for_status()

            response_json = r.json()

            files = response_json.get("files")
            metadata = response_json.get("metadata")

            return_files += files

            LOGGER.info(f"len(files)={len(files)} len(return_files)={len(return_files)}")

            next_token = metadata.get("nexttoken")
            if not next_token or len(return_files) >= limit:
                break

            headers_deepcopy.update({
                "nexttoken": next_token
            })

            LOGGER.info("next_token found")

        LOGGER.info("End of pagination")

        if len(return_files) > limit:
            LOGGER.info(f"Truncating return_files... limit={limit}")
            return return_files[:limit]

        return return_files

    def get_incomplete_files(self, limit):
        url = urljoin(self.host, "/files/incomplete")
        return self._get_paginated_files(url, limit=limit)

    def get_active_files(self, limit):
        url = urljoin(self.host, "/files/active")
        return self._get_paginated_files(url, limit=limit)

    def delete(self, fileid: str):
        url = urljoin(self.host, "/files")
        r = requests.delete(url, data={"fileid": fileid}, headers=self._headers)
        r.raise_for_status()
