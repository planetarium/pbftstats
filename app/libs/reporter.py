from __future__ import annotations
from typing import Optional
import os
import re
import datetime
import pandas as pd
from eth_keys import keys
import codecs

class Reporter:

    tx_signer_regex = re.compile(r"^tx-signer_\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])_\d{9}_\d{9}_\d{5}.csv$")
    lastcommit_vote_regex = re.compile(r"^lastcommit-vote_\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])_\d{9}_\d{9}_\d{5}.csv$")

    def __init__(self, log_path: str, report_path: str):
        self.__log_path = log_path
        self.__report_path = report_path

    def read_logs_tx_signer(self, regex: re.Pattern) -> Optional[str]:
        logs = []
        for file in os.scandir(self.__log_path):
            if regex.match(file.name):
                _, date_str, _, _, _ = file.name.split("_")
                df = pd.read_csv(file.path)
                df["date"] = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                df["signer"] = df["signer"].apply(
                    lambda x: keys.PublicKey.from_compressed_bytes(
                        codecs.decode(bytes(x, encoding="utf-8"), "hex_codec")).to_address())
                logs.append(df)
        return logs

    def read_logs_lastcommit_vote(self, regex: re.Pattern) -> Optional[str]:
        logs = []
        for file in os.scandir(self.__log_path):
            if regex.match(file.name):
                _, date_str, _, _, _  = file.name.split("_")
                df = pd.read_csv(file.path)
                df["date"] = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                logs.append(df)
        return logs

    def run_tx_signer(self):
        logs = self.read_logs_tx_signer(self.tx_signer_regex)
        concatenated = pd.concat(logs, ignore_index=True)
        report = concatenated.groupby(["date", "signer"]).sum()
        report = pd.pivot_table(report, values="n_tx", index="signer", columns="date").fillna(0).astype(int)
        os.makedirs(self.__report_path, exist_ok=True)
        report.to_csv(os.path.join(self.__report_path, "report_tx_signer.csv"))

    def run_lastcommit_vote(self):
        logs = self.read_logs_lastcommit_vote(self.lastcommit_vote_regex)
        concatenated = pd.concat(logs, ignore_index=True)
        report = concatenated.groupby(["date", "validator"]).sum()
        report = pd.pivot_table(report, values="n_precommit", index="validator", columns="date").fillna(0).astype(int)
        os.makedirs(self.__report_path, exist_ok=True)
        report.to_csv(os.path.join(self.__report_path, "report_lastcommit_vote.csv"))
        