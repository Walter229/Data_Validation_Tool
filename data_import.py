import pandas as pd
import ca_types
from loguru import logger
import os

def get_reuters_data(reuters_path):
    """
    Reads all reuters files, concatenates them
    """
    return_df = pd.DataFrame()
    for i in os.listdir(reuters_path):
        filename = reuters_path + r"\\" + i
        cur_df = pd.read_excel(filename, skiprows=6).dropna(how="all")
        if not cur_df.empty:
            return_df = pd.concat([return_df, cur_df])

    logger.debug(f'Read Reuters data from files in: {reuters_path}')
    return return_df.reset_index(drop=True)


def get_edi_data(start_date, end_date, edi_manual_file):
    """
    Fetches EDI data from EDI API or from EDI.csv, if specified
    :return:
    """
    if edi_manual_file:
        local_edi_dir = os.getcwd() + '\\EDI\\EDI.csv'
        logger.debug(f'Fetching EDI data from {local_edi_dir}...')
        df = pd.read_csv(local_edi_dir)
    else:
        logger.debug('Fetching EDI data from API...')
        df = pd.read_csv('http://XYZ/v1/ca')
    column_mapping = {
        "ID_RIC": "RIC",
        "EVENT_TYPE": "Type",
        "EX_DT": "Execution Date",
        "STOCK_SPLIT_RATIO": "Relation",
        "GROSS_AMT": "GROSS",
        "NET_AMT": "NET",
        "STOCK_DIV_RATIO": "Stock Dividend",
        "REPORTED_AMT": "REPORTED_AMT",
        "SUBSCRIPTION_RATIO": "Terms",
        "CRNCY": "Currency",
        "SUBSCRIPTION_PRICE": "Subscription Price",
        "SUBSCRIPTION_PRICE_CRNCY": "Subscription Currency",
        "SOURCE": "Source",
        "TAX_RATE": "Tax Rate"
    }
    df = df.drop(columns=df.columns.difference(column_mapping))
    df = df.rename(columns=column_mapping)
    df['Execution Date'] = pd.to_datetime(df['Execution Date'])
    df = df[df['Execution Date'] >= start_date]
    df = df[df['Execution Date'] <= end_date]
    df.reset_index(inplace=True, drop=True, )

    return df


def get_stock_splits(reuters_cas, edi_cas, plat_cas):
    """
    Extracts the stock split information from the datasources
    :param reuters_cas:
    :param edi_cas:
    :param plat_cas:
    :return:
    """
    reuters_splits = reuters_cas.get_share_splits()
    edi_splits = edi_cas.get_stock_splits()
    plat_splits = plat_cas.get_stock_splits()

    return (reuters_splits, edi_splits, plat_splits)


def get_stock_divs(reuters_cas, edi_cas, plat_cas):
    """
    Extracts the stock dividend information from the data sources
    :param reuters_cas:
    :param edi_cas:
    :param plat_cas:
    :return:
    """
    reuters_stock_divs = reuters_cas.get_scrip_issues()
    edi_stock_divs = edi_cas.get_stock_dividends()
    plat_stock_divs = plat_cas.get_stock_dividends()

    return (reuters_stock_divs, edi_stock_divs, plat_stock_divs)


def get_rights_issues(reuters_cas, edi_cas, plat_cas):
    """
    Extracts the rights issue information from the data sources
    :param reuters_cas:
    :param edi_cas:
    :param plat_cas:
    :return:
    """
    reuters_rights = reuters_cas.get_rights_issues()
    edi_rights = edi_cas.get_rights_issues()
    plat_rights = plat_cas.get_rights_issues()

    return (reuters_rights, edi_rights, plat_rights)


def get_cash_dividends(reuters_cas, edi_cas, plat_cas):
    """
    Extracts the cash dividend information from the data sources
    :param reuters_cas:
    :param edi_cas:
    :param plat_cas:
    :return:
    """
    reuters_cash = reuters_cas.get_cash_dividends()
    edi_cash = edi_cas.get_cash_dividends()

    # Add EDI Taxation Type: IoC if exchange = .SA and Net < gross
    edi_cash = add_edi_taxtype(edi_cash)

    edi_cash = edi_cash[['RIC', 'Type', 'Dividend Taxation Type', 'Execution Date', 'GROSS', 'NET', 'Currency']]
    plat_cash = plat_cas.get_cash_dividends()
    plat_cash = plat_cash[['RIC', 'Type', 'Execution Date', 'GROSS', 'NET', 'Currency', 'Dividend Taxation Type', 'Franking amount', 'CFI amount']]

    # add info to cfi and franking column
    plat_cash[['Franking amount', 'CFI amount']] = plat_cash.loc[(plat_cash['RIC'].str.contains(r'\.AX$')) | (plat_cash['RIC'].str.contains(r'\.CHA$')) | (plat_cash['RIC'].str.contains(r'\.NZ$')), ['Franking amount', 'CFI amount']]
    plat_cash.loc[~plat_cash['Franking amount'].isna(), 'Franking amount'] = "Franking: " + plat_cash['Franking amount'].astype(str)
    plat_cash.loc[~plat_cash['CFI amount'].isna(), 'CFI amount'] = "CFI: " + plat_cash['CFI amount'].astype(str)

    return (reuters_cash, edi_cash, plat_cash)

def add_edi_taxtype(edi_cash):
    # Default tax type
    edi_cash.loc[:,'Dividend Taxation Type'] = 'DEFAULT'
    # Interest on Capital -> .SA & Tax Rate = 15%
    edi_cash.loc[(edi_cash['RIC'].str.contains('\.SA$')) & (edi_cash['Tax Rate'] == 15), 'Dividend Taxation Type'] = 'INTEREST_ON_CAPITAL'

    # Return of Capital -> based on EDI data source
    edi_cash.loc[edi_cash['Source'] == 'edi_web_ca_rcap', 'Dividend Taxation Type'] = 'RETURN_OF_CAPITAL'

    # add reits to edi_dataframe
    edi_cash = add_reit(edi_cash)

    # add PID to .L, .CHI and .NXX if tax rate = 20
    edi_cash.loc[((edi_cash['RIC'].str.contains(r'\.L$')) |
                  (edi_cash['RIC'].str.contains(r'\.CHI$')) |
                  (edi_cash['RIC'].str.contains(r'\.NXX$')))
                 & (edi_cash['Tax Rate'] == 20), 'Dividend Taxation Type'] = 'PID'
    return edi_cash


def add_reit(cash_div_df: pd.DataFrame):
    """
    Add REIT from symbology to cash dividends.
    :param cash_div_df:
    :return:
    """
    ric_reit_relevant = ['KL', 'MX', 'SI', 'TW', 'TWO', 'IS']
    # fetch data of symbology
    reit_instruments_df = ca_types.fetch_symbology(cash_div_df)
    # filter for reit
    reit_instruments_df.loc[(reit_instruments_df['SECURITY_TYP'] == "REIT") | (reit_instruments_df['SECURITY_TYP_2'] == "REIT"), 'Dividend Taxation Type_temp'] = "REIT"
    reit_instruments_df = reit_instruments_df.dropna(subset=['Dividend Taxation Type_temp'])
    # spliting of exchanges
    reit_instruments_df['Exchange'] = reit_instruments_df['RIC'].str.split('.').apply(lambda x: x[1])
    # filter of relevant exchanges
    reit_instruments_df = reit_instruments_df.loc[reit_instruments_df['Exchange'].isin(ric_reit_relevant), :]
    # mergen
    cash_div_df = pd.merge(cash_div_df, reit_instruments_df, on=['RIC'], how='left')
    cash_div_df.loc[~cash_div_df['Dividend Taxation Type_temp'].isna(),'Dividend Taxation Type'] = 'REIT'
    # drop irrelevant columns
    cash_div_df = cash_div_df.drop(columns=['SECURITY_TYP_2', 'SECURITY_TYP', 'Dividend Taxation Type_temp', 'Exchange'])
    return cash_div_df

def add_comment(cash_div_df: pd.DataFrame, comment_adding_columns: list):
    cash_div_df['Additional Comment'] = cash_div_df[['Additional Comment']+comment_adding_columns].apply(
        lambda x: ", ".join(x.astype(str).replace('nan', '')).strip(', ').replace(' , ', ' '), axis=1)

    cash_div_df = cash_div_df.drop(columns=comment_adding_columns)
    return cash_div_df


def add_adr(cash_div_df: pd.DataFrame):
    """
    Add REIT, ADR, and IoC information from Symbology to cash dividends.
    Also add EDI IoC info
    :param cash_div_df:
    :return:
    """
    # fetch data of symbology
    adr_instruments_df = ca_types.fetch_symbology(cash_div_df)
    # filter for adr
    adr_instruments_df.loc[(adr_instruments_df['SECURITY_TYP'] == "ADR") | (
                adr_instruments_df['SECURITY_TYP_2'] == "ADR"), 'Additional Comment_temp'] = "ADR"

    # merge
    cash_div_df = pd.merge(cash_div_df, adr_instruments_df, on=['RIC'], how='left')

    cash_div_df = cash_div_df.drop(columns=['SECURITY_TYP_2', 'SECURITY_TYP'])
    return cash_div_df

def keep_gigant_instruments(ca_df, gigant_general):
    """
    Filter CA dataframe to only keep instruments that are in the gigant universe
    :param ca_df:
    :param gigant_general:
    :return:
    """
    gigant_instruments = gigant_general.get_all_instruments()
    ca_df_in_gigant = ca_df.loc[ca_df['RIC'].isin(gigant_instruments['RIC']),]
    return ca_df_in_gigant

def fetch_all_data(start_date, end_date, gigant_general, edi_manual_file):
    """
    Fetches the data from Reuters, EDI, and Platform and returns a data dictionariy with the CA type datasets
    for each vendor
    :param start_date:
    :param end_date:
    :return:
    """
    # Get data from the different sources
    # r'C:\Users\EquityOpsShared\Desktop\EIKON OUTPUT FILE-CA'
    reuters_data = get_reuters_data(r'T:\EquityOps\Rundeck\VENDOR_CA_VALIDATION\REUTERS FILES')
    edi_data = get_edi_data(start_date, end_date, edi_manual_file)
    logger.debug('Fetch Platform data from API...')
    plat_data = ca_types.GigantCAs(start_date, end_date).df

    # Initialize CA classes for the different vendors
    reuters_cas = ca_types.ReutersCAs(reuters_data, start_date, end_date)
    edi_cas = ca_types.EdiCAs(edi_data, start_date, end_date)
    plat_cas = ca_types.GigantCAs(start_date, end_date)

    # Filter CAs, to only keep CAs from stocks in the universe
    reuters_cas.df = keep_gigant_instruments(reuters_cas.df, gigant_general)
    edi_cas.df = keep_gigant_instruments(edi_cas.df, gigant_general)

    # Obtain datasets for: Stock dividends, Stock splits, Rights issues, Cash dividends
    reuters_stock_divs, edi_stock_divs, plat_stock_divs = get_stock_divs(reuters_cas, edi_cas, plat_cas)
    reuters_splits, edi_splits, plat_splits = get_stock_splits(reuters_cas, edi_cas, plat_cas)
    reuters_rights, edi_rights, plat_rights = get_rights_issues(reuters_cas, edi_cas, plat_cas)
    reuters_cash_divs, edi_cash_divs, plat_cash_divs = get_cash_dividends(reuters_cas, edi_cas, plat_cas)

    # Store different datasets in dictionaries
    reuters_data_dict = {'Raw data': reuters_data, 'Stock dividends': reuters_stock_divs, 'Stock splits': reuters_splits,
                         'Rights issues': reuters_rights, 'Cash dividends': reuters_cash_divs}
    edi_data_dict = {'Raw data': edi_data, 'Stock dividends': edi_stock_divs, 'Stock splits': edi_splits,
                         'Rights issues': edi_rights, 'Cash dividends': edi_cash_divs}
    plat_data_dict = {'Raw data': plat_data, 'Stock dividends': plat_stock_divs, 'Stock splits': plat_splits,
                         'Rights issues': plat_rights, 'Cash dividends': plat_cash_divs}


    return (reuters_data_dict, edi_data_dict, plat_data_dict)
