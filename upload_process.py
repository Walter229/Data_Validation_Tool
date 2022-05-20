import pandas as pd
import requests
import datetime

def is_adhoc(date):
    """
    Check if date is ad-hoc (t to t+1) or not
    :param date:
    :return:
    """
    today = datetime.date.today()
    next_bus_day = today + pd.tseries.offsets.BusinessDay()
    if date <= next_bus_day:
        return 'YES'
    return 'NO'

def get_currency(RIC):
    """
    Use equivalent to SGET formula to obtain currency
    :param RIC:
    :return:
    """
    try:
        url = f"http://XYZ/internal-data/simple/getData?ref={RIC}&q=CUR"
        response = requests.get(url)
        curr = response.text
    except:
        curr = ""
    return curr


def add_cash_div_upload_cols(df):
    cash_divs_check = df.copy()

    # Determine observations that are not yet in the platform
    not_in_plat = cash_divs_check.loc[cash_divs_check['Plat_Currency'].isnull(),]

    # Select observations that are validated by EDI and Reuters
    not_plat_validated = not_in_plat.loc[(not_in_plat['Reuters-EDI_GROSS']) & (not_in_plat['Reuters-EDI_Currency']),]

    # Define the validated observations that are not yet in the platform as Uploads
    cash_divs_check.loc[:, 'Upload?'] = 'No'

    cash_divs_check.loc[not_plat_validated.index, 'Upload?'] = 'Yes'

    # Add the validated vendor-specific validated data points to dedicated upload columns
    cash_divs_check.loc[:, 'Upload-Value'] = ""
    cash_divs_check.loc[not_plat_validated.index, 'Upload-Value'] = cash_divs_check['Reuters_GROSS']

    cash_divs_check.loc[:, 'Upload-Currency'] = ""
    cash_divs_check.loc[not_plat_validated.index, 'Upload-Currency'] = cash_divs_check['Reuters_Currency']

    # Add necessary columns for upload: Currency, Dividend Subtype and Dividend Taxation Type
    cash_divs_check.loc[:,'Upload-Dividend Subtype'] = "DEFAULT"
    # Capture EST cases for .T, .KS, and .KQ exchanges
    cash_divs_check['Exchange'] = cash_divs_check['RIC'].str.split('.').str[1]
    cash_divs_check.loc[cash_divs_check['Exchange'].isin(['T', 'KS', 'KQ']), 'Upload-Dividend Subtype'] = 'EST'
    cash_divs_check = cash_divs_check.drop(columns=['Exchange'], axis=1)

    cash_divs_check.loc[:,'Upload-Dividend Taxation Type'] = cash_divs_check['Dividend Taxation Type']

    # Add Withholding Tax Type - NET for ADRs & .IS instruments if net=gross
    cash_divs_check.loc[:,'Upload-Withholding Taxation Type'] = 'GROSS'
    cash_divs_check.loc[cash_divs_check['Additional Comment'].str.contains('ADR'),
                        'Upload-Withholding Taxation Type'] = 'NET'
    cash_divs_check.loc[(cash_divs_check['RIC'].str.contains(r'\.IS$'))
                        & (cash_divs_check['Reuters_GROSS'] == cash_divs_check['Reuters_NET']),
                        'Upload-Withholding Taxation Type'] = 'NET'
    # For ADRs, adjust validation to use Net amount
    cash_divs_check.loc[cash_divs_check['Additional Comment'].str.contains('ADR'), 'Upload-Value'] = ""
    cash_divs_check.loc[cash_divs_check['Additional Comment'].str.contains('ADR'), 'Upload-Currency'] = ""
    cash_divs_check.loc[cash_divs_check['Additional Comment'].str.contains('ADR'), 'Upload?'] = 'No'
    adr_not_plat_validated = not_in_plat.loc[(not_in_plat['Reuters-EDI_NET']) &
                                             (not_in_plat['Reuters-EDI_Currency']) &
                                             (not_in_plat['Additional Comment'].str.contains('ADR')),]
    cash_divs_check.loc[adr_not_plat_validated.index, 'Upload?'] = 'Yes'
    cash_divs_check.loc[adr_not_plat_validated.index, 'Upload-Value'] = cash_divs_check['Reuters_NET']
    cash_divs_check.loc[adr_not_plat_validated.index, 'Upload-Currency'] = cash_divs_check['Reuters_Currency']

    # Add Ad-hoc column
    cash_divs_check.loc[:, 'Upload-AdHoc'] = cash_divs_check['Execution Date'].apply(is_adhoc)

    return cash_divs_check


def add_split_upload_cols(df):
    split_check = df.copy()

    # Determine observations that are not yet in the platform
    not_in_plat = split_check.loc[split_check['Plat_Relation'].isnull(),]

    # Select observations that are validated by EDI and Reuters
    not_plat_validated = not_in_plat.loc[not_in_plat['Reuters-EDI'],]

    # Define the validated observations that are not yet in the platform as Uploads
    split_check.loc[:, 'Upload?'] = 'No'
    split_check.loc[not_plat_validated.index, 'Upload?'] = 'Yes'

    # Add the validated vendor-specific validated data points to dedicated upload columns
    split_check.loc[:, 'Upload-Relation'] = ""
    split_check.loc[not_plat_validated.index, 'Upload-Relation'] = split_check['Reuters_Relation']

    # Add necessary columns for upload: Currency, Dividend Subtype and Dividend Taxation Type
    split_check.loc[:,'Upload-Currency'] = split_check['RIC'].apply(get_currency)
    split_check.loc[:,'Upload-Dividend Subtype'] = "DEFAULT"
    split_check.loc[:,'Upload-Dividend Taxation Type'] = "DEFAULT"

    # Add Ad-hoc column
    split_check.loc[:, 'Upload-AdHoc'] = split_check['Execution Date'].apply(is_adhoc)

    return split_check


def add_stock_div_upload_cols(df):
    stock_divs_check = df.copy()

    # Determine observations that are not yet in the platform
    not_in_plat = stock_divs_check.loc[stock_divs_check['Plat_Stock Dividend'].isnull(),]

    # Select observations that are validated by EDI and Reuters
    not_plat_validated = not_in_plat.loc[not_in_plat['Reuters-EDI'],]

    # Define the validated observations that are not yet in the platform as Uploads
    stock_divs_check.loc[:, 'Upload?'] = 'No'
    stock_divs_check.loc[not_plat_validated.index, 'Upload?'] = 'Yes'

    # Add the validated vendor-specific validated data points to dedicated upload columns
    stock_divs_check.loc[:, 'Upload-Stock Dividend'] = ""
    stock_divs_check.loc[not_plat_validated.index, 'Upload-Stock Dividend'] = stock_divs_check['Reuters_Stock Dividend']

    # Add necessary columns for upload: Currency, Dividend Subtype and Dividend Taxation Type
    stock_divs_check.loc[:,'Upload-Currency'] = stock_divs_check['RIC'].apply(get_currency)
    stock_divs_check.loc[:,'Upload-Dividend Subtype'] = "DEFAULT"
    stock_divs_check.loc[:,'Upload-Dividend Taxation Type'] = "DEFAULT"

    # Add Ad-hoc column
    stock_divs_check.loc[:, 'Upload-AdHoc'] = stock_divs_check['Execution Date'].apply(is_adhoc)

    return stock_divs_check


def add_rights_upload_cols(df):
    rights_check = df.copy()

    # Determine observations that are not yet in the platform
    not_plat = rights_check.loc[rights_check['Plat_Terms'].isnull(),]

    # Select observations that are validated by EDI and Reuters,
    not_plat_validated = not_plat.loc[not_plat['Reuters-EDI'],]

    # Define the validated observations that are not yet in the platform as Uploads
    rights_check.loc[:, 'Upload?'] = 'No'
    rights_check.loc[not_plat_validated.index, 'Upload?'] = 'Yes'

    # Add the validated vendor-specific validated data points to dedicated upload columns
    rights_check.loc[:, 'Upload-Terms'] = ""
    rights_check.loc[not_plat_validated.index, 'Upload-Terms'] = rights_check['EDI_Terms']

    rights_check.loc[:, 'Upload-Subscription Price'] = ""
    rights_check.loc[not_plat_validated.index, 'Upload-Subscription Price'] = rights_check['EDI_Subscription Price']

    rights_check.loc[:, 'Upload-Currency'] = ""
    rights_check.loc[not_plat_validated.index, 'Upload-Currency'] = rights_check['EDI_Currency']

    # Add necessary columns for upload: Currency, Dividend Subtype and Dividend Taxation Type
    rights_check.loc[:,'Upload-Dividend Subtype'] = "DEFAULT"
    rights_check.loc[:,'Upload-Dividend Taxation Type'] = "DEFAULT"

    # Add Ad-hoc column
    rights_check.loc[:, 'Upload-AdHoc'] = rights_check['Execution Date'].apply(is_adhoc)

    # Add Execution Order column
    rights_check.loc[:, 'Upload-Execution Order'] = 'SIMILAR'


    return rights_check
