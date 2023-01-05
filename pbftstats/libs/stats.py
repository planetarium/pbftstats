from __future__ import annotations
from typing import Optional
import os
import re
import datetime
import dateutil
import urllib
import requests
import json
import pandas as pd

class Stats:
    def __init__(self, out_path: str, host_url: str, start_block_index: int):
        self.__out_path = out_path
        self.__host_url = urllib.parse.urljoin(host_url, url="graphql/explorer", allow_fragments=True)
        self.__start_block_index = start_block_index

    def run(self):
        fname_wip = self.lookup_wip()
        while(True):
            fname_wip = self.update_wip(fname_wip)


    def lookup_wip(self) -> Optional[str]:
        fn_regex = re.compile(r"^testpbft_\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])_\d{9}_\d{9}_\d{5}_wip.csv$")
        os.makedirs(self.__out_path, exist_ok=True)
        for file in os.scandir(self.__out_path):
            if fn_regex.match(file.name):
                return file.name
        return None

    def query(self, index: int) -> requests.Response:
        body = f"query{{blockQuery{{block(index: {index}){{transactions{{publicKey}}timestamp}}}}}}"
        return requests.post(url=self.__host_url, json={"query": body})

    def update_wip(self, file_name: Optional[str]) -> Optional[str]:
        start_time = datetime.datetime.now()
        if file_name:
            _, date_str, start_str, end_str, _, _ = file_name.split("_")
            file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            start = int(start_str)
            end = int(end_str)
        else:
            end = self.__start_block_index - 1
            start = self.__start_block_index
            file_date = datetime.date.min

        response = self.query(end + 1)

        if response.status_code == 200:
            signers = self.get_tx_signers(response)
            block_date = self.get_timestamp(response)
        else:
            raise ConnectionError("Failed to get response")

        create_new = False
        if block_date > file_date:
            df = pd.DataFrame(columns=["signer", "txn"])
            create_new = True
        else:
            df = pd.read_csv(os.path.join(self.__out_path, file_name), delimiter="\t")

        for signer in signers:
            txn = df.loc[df["signer"] == signer, "txn"]
            if txn.count() == 0:
                df.loc[len(df.index)] = [signer, 1]
            else:
                df.loc[df["signer"] == signer, "txn"] = txn + 1

        if not create_new:
            os.remove(os.path.join(self.__out_path, file_name))
        else:
            if file_name:
                os.rename(file_name, file_name.strip("_wip"))

        block_date_string = block_date.strftime("%Y-%m-%d")
        file_name = f"testpbft_{block_date_string}_{start:09}_{end + 1:09}_{len(df):05}_wip.csv"
        df.to_csv(os.path.join(self.__out_path, file_name), sep="\t", index=False)
        end_time = datetime.datetime.now()
        print(f"processing {file_name}...{int((end_time - start_time).total_seconds() * 1000)}ms per block", end="\r")
        return file_name

    @staticmethod
    def get_tx_signers(response: requests.Response) -> list:
        data = json.loads(response.content)
        if "errors" in data:
            raise IndexError("The block has not been generated yet")
        return [tx["publicKey"] for tx in data["data"]["blockQuery"]["block"]["transactions"]]

    @staticmethod
    def get_timestamp(response: requests.Response) -> datetime.datetime:
        data = json.loads(response.content)
        return dateutil.parser.isoparse(data["data"]["blockQuery"]["block"]["timestamp"]).date()