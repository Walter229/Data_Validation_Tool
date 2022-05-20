import pandas as pd
from equity_utils.api import InternalDataAPI
from loguru import logger
import pymysql
import requests
import io
import glob
from datetime import date
import configparser


def fetch_symbology(cash_div_df: pd.DataFrame):
    max_instrument_request = 2000
    ric_list = cash_div_df.drop_duplicates(subset='RIC')['RIC'].to_list()
    all_instruments = []
    temp_gigant_list = [{'ID_RIC': ric} for ric in ric_list]
    body = [temp_gigant_list[i:i + max_instrument_request] for i in
            range(0, len(temp_gigant_list), max_instrument_request)]
    for body_split in body:
        try:
            response = requests.post("http://XYZ/rest/v1/marketData/instruments", json=body_split)
            response.raise_for_status()
            all_instruments += (response.json())
        except requests.HTTPError as err:
            print(f'Request was wrong')
            raise err
    reit_adr_instruments_df = pd.DataFrame(all_instruments)[['ID_RIC', 'SECURITY_TYP_2', 'SECURITY_TYP']].rename(
        columns={'ID_RIC': 'RIC'})
    return reit_adr_instruments_df


class Gigant_Generell_Information:
    def __init__(self):
        config_path = 'config.ini'
        config = configparser.RawConfigParser()
        config.read(config_path)
        self.__instruments_in_gigant = pd.DataFrame(
            Gigant_Generell_Information.__get_gigant_instruments(config['gigant_universe_db']['username'],
                                                                 config['gigant_universe_db']['password'],
                                                                 config['gigant_universe_db']['host'],
                                                                 config['gigant_universe_db']['port'],
                                                                 f"SELECT inst.name, inst.sedol, inst.isin, inst.mic_code, inst.ric, inst.bbg_ticker FROM vnd_data_loader.shares_unique inst WHERE inst.is_in_use = '1'"),
            columns=['Name', 'sedol', 'ISIN', 'ID_MIC', 'RIC', 'TICKER'])

    def get_all_instruments(self):
        """
        general information of gigant in order to add RIC,... with different IDs
        :return: Dataframe with all active instruments
        """
        return self.__instruments_in_gigant

    def map_isin_ticker(self, instruments):
        """
        Mapping Isin and BBG-Ticker to the dataframe which is input
        :param instruments: Dataframe with instruments (needs to contain "RIC"-column)
        :return: Dataframe with Bbgticker and Isin
        """
        instruments_with_ticker = instruments.join(
            self.__instruments_in_gigant[['TICKER', 'RIC']].set_index(['RIC']), how='left', on=['RIC'])
        instruments_with_ticker = instruments_with_ticker.join(
            self.__instruments_in_gigant[['Name', 'RIC']].set_index(['RIC']), how='left', on=['RIC'])
        try:
            instruments_with_ticker = instruments_with_ticker.join(
                self.__instruments_in_gigant[['ISIN', 'RIC']].set_index(['RIC']), how='left', on=['RIC'])
        except ValueError:
            pass
        return instruments_with_ticker

    def get_ric(self, to_be_mapped_to_ric_df: pd.DataFrame) ->pd.DataFrame:
        to_be_mapped_to_ric_df = to_be_mapped_to_ric_df.join(
            self.__instruments_in_gigant[['ISIN', 'sedol', 'RIC']].set_index(['sedol', 'ISIN']).rename(
                columns={'RIC': 'RIC_b'}), how='left', on=['SEDOL', 'ISIN'])
        to_be_mapped_to_ric_df = to_be_mapped_to_ric_df.join(
            self.__instruments_in_gigant[['ISIN', 'ID_MIC', 'RIC']].set_index(['ISIN', 'ID_MIC']), how='left',
            on=['ISIN', 'MIC'])
        to_be_mapped_to_ric_df['RIC'] = to_be_mapped_to_ric_df['RIC'].fillna(to_be_mapped_to_ric_df['RIC_b'])
        to_be_mapped_to_ric_df = to_be_mapped_to_ric_df.drop(columns='RIC_b')
        return to_be_mapped_to_ric_df

    @staticmethod
    def __get_gigant_instruments(db_user, db_password, db_host, db_port, query):
        """
        Fetching Data from Gigant-Database (Ric, Isin, Sedol, Cusip, Bbg-Ticker, Status + Preprocessing of Data
        Calls DatabaseConnection of Gigant_db_Collector
        Params per input in commando line: database-user, db-password, db-host, db-port, db-name
        :return: list
        """

        db_con = pymysql.connect(
            host=db_host, user=db_user, password=db_password, port=int(db_port)
        )
        with db_con.cursor() as cursor:
            cursor.execute(query)
            instrument_data = cursor.fetchall()

        db_con.close()
        return instrument_data


class GigantCAs:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.df = None
        self._pull_cas()

    def get_stock_dividends(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "Stock Dividend"]
        df = self.df.query("Type == 'STOCK_DIVIDEND'")
        df = df.drop(columns=df.columns.difference(relevant_columns))
        df["Stock Dividend"] = df["Stock Dividend"].apply(lambda x: round(x, 6))
        return df

    def get_stock_splits(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "Relation"]
        df = self.df.query("Type == 'STOCK_SPLIT'")
        df = df.drop(columns=df.columns.difference(relevant_columns))
        df["Relation"] = df["Relation"].apply(self._clean_relations)
        return df

    def get_cash_dividends(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "GROSS", "NET", "Currency", "Dividend Taxation Type",
                            "Franking amount", "CFI amount", "PID percent"]
        df = self.df.query("Type == 'CASH_DIVIDEND' | Type == 'SPECIAL_DIVIDEND'")
        df.loc[:, "GROSS"] = df.apply(lambda row: self._get_value(row, gross=True), axis=1)
        df.loc[:, "NET"] = df.apply(lambda row: self._get_value(row, gross=False), axis=1)
        df = df.drop(columns=df.columns.difference(relevant_columns))
        return df

    def get_rights_issues(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "Terms", "Subscription Price", "Currency"]
        df = self.df.query("Type == 'RIGHTS_ISSUE'")
        df = df.drop(columns=df.columns.difference(relevant_columns))
        df["Terms"] = df["Terms"].apply(lambda x: round(x, 6))
        return df

    @staticmethod
    def _get_value(row, gross=True):
        if gross:
            column_name = "GROSS"
        else:
            column_name = "NET"

        value = round(row["Value"], 6) if row["Withholding Tax Type"] == column_name else None
        return value

    @staticmethod
    def _clean_relations(relation: str):
        relation = relation.split(":")
        dividend = float(relation[0])
        divisor = float(relation[1])
        ratio = dividend / divisor
        if ratio < 1:
            x = 1
            y = round(1 / ratio, 6)
        else:
            x = round(ratio, 6)
            y = 1


        return f"{x}:{y}"

    def _pull_cas(self):
        self.df = InternalDataAPI().corporate_actions(self.start_date, self.end_date)
        self.df["Execution Date"] = pd.to_datetime(self.df["Execution Date"])
        if self.df.empty:
            logger.warning(
                f"Corporate Actions from the Platform are empty! This makes only sense for very short timespans.")


class ReutersCAs:
    def __init__(self, df, start_date, end_date):
        self.df = df
        self.start_date = start_date
        self.end_date = end_date

    def __date_filtering(self, df):
        df = df[(df['Execution Date'] >= self.start_date) & (df['Execution Date'] <= self.end_date)]
        return df

    def get_scrip_issues(self):
        df: pd.DataFrame = self.df.query("Event == 'Scrip Issue'").reset_index(drop=True)

        column_mapping = {
            "RIC": "RIC",
            "Event": "Type",
            "Details": "Execution Date",
            "Unnamed: 7": "Stock Dividend"}
        df = df.drop(columns=df.columns.difference(column_mapping))

        df = df.rename(columns=column_mapping)

        df["Type"] = "STOCK_DIVIDEND"

        df["Execution Date"] = df["Execution Date"].apply(self.clean_execution_date)

        df = self.__date_filtering(df)

        df["Stock Dividend"] = df["Stock Dividend"].apply(
            lambda x: self.calc_terms(x) * 100)  # *100 because Platform stores it in percentage

        return df

    def get_share_splits(self):
        df: pd.DataFrame = self.df.query(
            "Event == 'Share Split' | Event == 'Share Consolidation'").reset_index(
            drop=True)
        column_mapping = {
            "RIC": "RIC",
            "Event": "Type",
            "Details": "Execution Date",
            "Unnamed: 7": "Relation"}
        df = df.drop(columns=df.columns.difference(column_mapping))
        df = df.rename(columns=column_mapping)
        df["Type"] = "STOCK_SPLIT"
        df["Execution Date"] = df["Execution Date"].apply(self.clean_execution_date)
        df["Relation"] = df["Relation"].apply(self.calc_relation)
        df = self.__date_filtering(df)
        return df

    def get_cash_dividends(self):
        q = "Event == 'Cash Dividend' | Event == 'Cash and Stock Alternative' | Event == 'Stock and Cash Alternative'"
        df: pd.DataFrame = self.df.query(q).reset_index(drop=True)
        column_mapping = {
            "RIC": "RIC",
            "Event": "Type",
            "Details": "Execution Date",
            "Unnamed: 7": "GROSS",
            "Unnamed: 8": "NET",
            "Unnamed: 9": "Type_div"}
        df = df.drop(columns=df.columns.difference(column_mapping))
        df = df.rename(columns=column_mapping)
        df["Type"] = df["Type_div"].apply(lambda x: "SPECIAL_DIVIDEND" if (
                    "extraordinary " in x.lower() or "special" in x.lower()) else "CASH_DIVIDEND")
        df["Execution Date"] = df["Execution Date"].apply(self.clean_execution_date)
        df["Currency"] = df["GROSS"].apply(lambda x: x.split(" ")[-1])
        df["GROSS"] = df["GROSS"].apply(lambda x: self.string_to_float(x.split(":")[-1]))
        df["NET"] = df["NET"].apply(lambda x: self.string_to_float(x.split(":")[-1]))
        df = df.drop(columns=["Type_div"])
        df = self.__date_filtering(df)
        return df

    def get_rights_issues(self):
        df: pd.DataFrame = self.df.query("Event == 'Rights Issue' | Event == 'Priority Issue'").reset_index(drop=True)
        column_mapping = {
            "RIC": "RIC",
            "Event": "Type",
            "Details": "Execution Date",
            "Unnamed: 7": "Terms"}
        df = df.drop(columns=df.columns.difference(column_mapping))
        df = df.rename(columns=column_mapping)
        df["Type"] = "RIGHTS_ISSUE"
        df["Execution Date"] = df["Execution Date"].apply(self.clean_execution_date)
        df["Terms"] = df["Terms"].apply(self.calc_terms)
        df = self.clean_rights_issues(df)
        df = self.__date_filtering(df)
        return df

    @staticmethod
    def clean_execution_date(ex_date: str):
        if ex_date[-2:] == "--":
            return pd.Timestamp("1900-01-01")
        date = ex_date.split(":")[-1].strip()
        return pd.Timestamp(date)

    @staticmethod
    def clean_rights_issues(df):
        df = df.loc[df["Execution Date"].notnull()]
        df = df.loc[df["Terms"] != 0]
        return df

    @staticmethod
    def calc_relation(relation: str):
        relation = relation.split(":")
        if relation[1].strip() == "--":
            return 0
        dividend = ReutersCAs.string_to_float(relation[2])
        divisor = ReutersCAs.string_to_float(relation[1])
        ratio = dividend / divisor
        if ratio < 1:
            x = 1
            y = round(1 / ratio, 6)
        else:
            x = round(ratio, 6)
            y = 1


        return f"{x}:{y}"

    @staticmethod
    def calc_terms(terms: str):
        terms = terms.split(":")
        if terms[1].strip() == "--":
            return 0
        dividend = ReutersCAs.string_to_float(terms[2])
        divisor = ReutersCAs.string_to_float(terms[1])
        ratio = dividend / divisor
        return round(ratio, 6)

    @staticmethod
    def string_to_float(number: str):
        copy = number
        number = number.strip()
        number = number.split(" ")[0] if " " in number else number
        try:
            number = round(float(number.strip().replace(",", "")), 6)
            return number
        except Exception as e:
            logger.debug(e)
            logger.warning(f'String could not be converted to number: {copy}. Using 0 instead')
            return 0


class EdiCAs:
    def __init__(self, df, start_date, end_date):
        self.df = df
        self.start_date = start_date
        self.end_date = end_date

    def get_stock_dividends(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "Stock Dividend"]
        df = self.df.loc[self.df['Type'].str.contains("STOCK_DIVIDEND"), :]
        df.loc[self.df['Type'].str.contains('STOCK_DIVIDEND'), 'Type'] = 'STOCK_DIVIDEND'
        df = df.drop(columns=df.columns.difference(relevant_columns))
        df["Stock Dividend"] = df["Stock Dividend"].apply(
            lambda x: self.calc_terms(x) * 100)
        df["Stock Dividend"] = df["Stock Dividend"].apply(lambda x: round(x, 6))
        # df.dropna(subset=["Stock Dividend"], inplace=True)
        return df

    def get_stock_splits(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "Relation"]
        df = self.df.query("Type == 'STOCK_SPLIT'")

        df = df.drop(columns=df.columns.difference(relevant_columns))
        df["Relation"] = df["Relation"].apply(self._clean_relations)
        return df

    def get_cash_dividends(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "GROSS", "NET", "Currency", "Source", "Tax Rate"]
        df = self.df.loc[(self.df['Type'].str.contains('CASH_DIVIDEND')) | (self.df['Type'].str.contains('SPECIAL_DIVIDEND'))]
        df.loc[self.df['Type'].str.contains('CASH_DIVIDEND'), 'Type'] = 'CASH_DIVIDEND'
        df.loc[self.df['Type'].str.contains('SPECIAL_DIVIDEND'), 'Type'] = 'SPECIAL_DIVIDEND'
        df.loc[(df.GROSS.isnull() & df.NET.isnull()), "GROSS"] = df.loc[
            (df.GROSS.isnull() & df.NET.isnull()), "REPORTED_AMT"]
        df.loc[(df.GROSS.isnull() & df.NET.isnull()), "NET"] = df.loc[
            (df.GROSS.isnull() & df.NET.isnull()), "REPORTED_AMT"]

        df = df.drop(columns=df.columns.difference(relevant_columns))
        return df

    def get_rights_issues(self):
        relevant_columns = ["RIC", "Type", "Execution Date", "Terms", "Subscription Price", "Currency"]
        df = self.df.query("Type == 'RIGHTS_ISSUE'")
        df1 = df.copy()
        df1["Currency"] = df["Subscription Currency"]
        df1 = df1.drop(columns=df.columns.difference(relevant_columns))

        df1["Terms"] = df1["Terms"].apply(self.calc_terms)

        return df1

    @staticmethod
    def _get_value(row, gross=True):
        if gross:
            column_name = "GROSS"
        else:
            column_name = "NET"

        value = round(row["Value"], 6) if row["Withholding Tax Type"] == column_name else None
        return value

    @staticmethod
    def _clean_relations(relation: str):
        try:
            ratio = float(relation)
        except:

            relation = relation.split(":")

            if relation[1].strip() == "--":
                return 0

            dividend = float(relation[0])
            divisor = float(relation[1])
            ratio = dividend / divisor

        if ratio < 1:
            x = 1
            y = round(1 / ratio, 6)
        else:
            x = round(ratio, 6)
            y = 1

        return f"{x}:{y}"

    @staticmethod
    def calc_terms(terms: str):
        try:
            ratio = round(float(terms), 6)
        except:
            terms = terms.split(":")

            if terms[1].strip() == "--":
                return 0
            dividend = float(terms[0])
            divisor = float(terms[1])
            ratio = dividend / divisor
        return round(ratio, 6)
