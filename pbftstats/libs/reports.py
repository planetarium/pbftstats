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
from eth_keys import keys
import codecs

class Reports:
    def __init__(self, log_path: str, report_path: str):
        self.__log_path = log_path
        self.__report_path = report_path

    def read_logs(self) -> Optional[str]:
        logs = []
        fn_regex = re.compile(r"^testpbft_\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])_\d{9}_\d{9}_\d{5}.csv$")
        for file in os.scandir(self.__log_path):
            if fn_regex.match(file.name):
                _, date_str, start_str, end_str, _ = file.name.split("_")
                df = pd.read_csv(file.path, delimiter="\t")
                df["date"] = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                df["signer"] = df["signer"].apply(
                    lambda x: keys.PublicKey.from_compressed_bytes(
                        codecs.decode(bytes(x, encoding="utf-8"), "hex_codec")).to_address())
                logs.append(df)
        return logs

    def run(self):
        logs = self.read_logs()
        concatenated = pd.concat(logs, ignore_index=True)
        report = concatenated.groupby(["date", "signer"]).sum()
        report = pd.pivot_table(report, values="txn", index="signer", columns="date").fillna(0).astype(int)
        print(report)
        os.makedirs(self.__report_path, exist_ok=True)
        report.to_csv(os.path.join(self.__report_path, "report.csv"))