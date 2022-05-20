import pandas as pd
from loguru import logger
import data_import

def compare_ca(dfs, key, gigant_general, per_column = False):
    """
    Compares the Corporate actions between Reuters, EDI and the platform.
    Observations are merged on the specified key and compared upon the remaining columns.
    Matching columns only indicate 'True' if all columns are equal between two vendors.
    :param dfs:
    :param key:
    :return:
    """
    # Determine columns that are not part of the key
    vendor_names = ['Reuters', 'EDI', 'Plat']
    cols_not_key = dfs[0].columns[~dfs[0].columns.isin(key)].to_list()
    not_key_names = {'Reuters':[],
                     'EDI':[],
                     'Plat':[]}
    # Rename columns that are not part of key to: Vendor_column
    for i, df in enumerate(dfs):
        for col_name in cols_not_key:
            not_key_name = f'{vendor_names[i]}_{col_name}'
            not_key_names[vendor_names[i]].append(not_key_name)
            df.rename({col_name: not_key_name}, axis=1, inplace=True)

    # Combine data from vendors based on key
    edi_plat = pd.merge(dfs[2], dfs[1], how='outer', on=key)
    reuters_edi_plat = pd.merge(edi_plat, dfs[0], how='outer', on=['RIC', 'Type', 'Execution Date'])

    # If comparison per column, check all non-key columns individually
    if per_column:
        for col in cols_not_key:
            reuters_edi_plat[f'Reuters-EDI_{col}'] = reuters_edi_plat[f'Reuters_{col}'] == reuters_edi_plat[f'EDI_{col}']
            reuters_edi_plat[f'Reuters-Plat_{col}'] = reuters_edi_plat[f'Reuters_{col}'] == reuters_edi_plat[f'Plat_{col}']
            reuters_edi_plat[f'EDI-Plat_{col}'] = reuters_edi_plat[f'EDI_{col}'] == reuters_edi_plat[f'Plat_{col}']

    # If columns are not compared individually
    else:
        # Reset column names for the vendor-specific columns to enable comparison
        check_reuters = reuters_edi_plat[not_key_names['Reuters']].T.reset_index(drop=True).T
        check_edi = reuters_edi_plat[not_key_names['EDI']].T.reset_index(drop=True).T
        check_plat = reuters_edi_plat[not_key_names['Plat']].T.reset_index(drop=True).T
        # Compare vendor information based on vendor specific information
        reuters_edi_plat['Reuters-EDI'] = (check_reuters == check_edi).all(axis=1)
        reuters_edi_plat['Reuters-Plat'] = (check_reuters == check_plat).all(axis=1)
        reuters_edi_plat['EDI-Plat'] = (check_edi == check_plat).all(axis=1)

    # adds ISIN and bbg-ticker to pd.Dataframe
    try:
        reuters_edi_plat = gigant_general.map_isin_ticker(reuters_edi_plat)
    except ValueError:
        pass

    # Add comment fields
    reuters_edi_plat['Comment'] = ""
    reuters_edi_plat['Additional Comment'] = ""

    return reuters_edi_plat

def remove_duplicates(data_dict):
    """
    Removes duplicates for all dfs within data dictionary
    :param data_dict:
    :return:
    """
    for key in data_dict:
        if isinstance(data_dict[key], pd.DataFrame):
            data_dict[key] = data_dict[key].drop_duplicates()

    return data_dict

def add_rights_event_type(rights_df, gigant_instance):
    """
    Add Event type information from ICE to Rights Issue data
    :param rights_df:
    :return:
    """
    # Pull data from ICE and filter data
    ice_capital_events_data = pd.read_csv('XYZ/ice-equity-ca/rest/v1/corporate-action/instruments?event_type=CapitalEvents')
    ice_capital_events_data = ice_capital_events_data[['ISIN', 'Event_type', 'MIC', 'SEDOL']]

    # map ISIN, MIC and SEDOL to RIC
    ice_capital_events_data = gigant_instance.get_ric(ice_capital_events_data)
    # drop duplicates and irrelevant columns
    ice_capital_events_data = ice_capital_events_data.drop(columns=['ISIN', 'MIC', 'SEDOL'])
    ice_capital_events_data = ice_capital_events_data.drop_duplicates(subset=['RIC'])
    # Merge ICE and rights issue data
    merged = pd.merge(rights_df, ice_capital_events_data, how='left', on=['RIC'])
    merged.loc[:, 'Additional Comment'] = merged['Event_type']
    merged = merged.drop(columns=['Event_type'], axis=1)

    return merged

def max_decimal_equality(row, col_1, col_2):
    """
    Compares two columns of a row and returns the minimum rounding digits for the values to still be equal
    :param float_1:
    :param float_2:
    :return:
    """
    for i in reversed(range(11)):
        try:
            if round(row[col_1], i) == round(row[col_2], i):
                return i
        except ValueError:
            return -1
        except TypeError:
            return -1
    return -1

def remove_rounding_mismatches(ca_df, columns, digits):
    """
    Change mismatch entries for mismatches due to rounding differences
    :param ca_df:
    :param columns:
    :return:
    """
    # For each column specified, compare information from two vendors and check if they are equal after rounding to X digits
    before = ca_df.copy()
    vendor_combinations = ['Reuters-EDI', 'Reuters-Plat', 'EDI-Plat']
    for col in columns:
        for vendor_comb in vendor_combinations:
            comparison_col = f'{vendor_comb}_{col}'
            vendor_1_col = f'{vendor_comb.split("-")[0]}_{col}'
            vendor_2_col = f'{vendor_comb.split("-")[1]}_{col}'
            ca_df[comparison_col] = ca_df.apply(lambda row: max_decimal_equality(row, vendor_1_col, vendor_2_col), axis = 1)
            ca_df[comparison_col] = ca_df[comparison_col] >= digits

    # Add comment to rows that were changed
    changes = pd.merge(before, ca_df, how='left', indicator=True)
    changes = changes.loc[changes['_merge'] == 'left_only',]
    ca_df.iloc[changes.index,changes.columns.get_loc('Additional Comment')] += ' Rounded'

    return ca_df

def platform_lookup(ca_df, plat_data):
    """
    Checks for missing platform information, if there exists a CA with a different type in the platform
    :param ca_df:
    :return:
    """
    # Select missing platform data
    missing_plat = ca_df.loc[(ca_df['Plat_GROSS'].isnull()) & (ca_df['Plat_NET'].isnull()), ]

    # Select relevant columns from platform
    plat_data = plat_data.loc[plat_data['Type'].isin(['CASH_DIVIDEND','SPECIAL_DIVIDEND'])]
    plat_data = plat_data[['RIC', 'Type', 'Execution Date', 'Value', 'Dividend Taxation Type']]
    plat_data['Info'] = plat_data[['RIC', 'Type', 'Dividend Taxation Type','Value']].apply(lambda row: row.to_string(), axis=1)

    # Merge datasets
    missing_plat_enriched = pd.merge(missing_plat, plat_data[['RIC', 'Execution Date', 'Info']], on=['RIC', 'Execution Date'], how='left')
    missing_plat_enriched = missing_plat_enriched[['RIC', 'Execution Date', 'Info']]

    # Assign enriched data back to ca_df
    missing_plat_enriched = missing_plat_enriched.drop_duplicates(subset=['RIC', 'Execution Date'])
    ca_df_enriched = pd.merge(ca_df, missing_plat_enriched, on=['RIC', 'Execution Date'], how='left')
    # Reformat df string
    ca_df_enriched['Info'] = ca_df_enriched['Info'].str.replace(r'\n', ',')
    ca_df_enriched['Info'] = ca_df_enriched['Info'].str.replace('Dividend Taxation Type', 'Dividend_Taxation_Type')
    ca_df_enriched['Info'] = ca_df_enriched['Info'][ca_df_enriched['Info'].notnull()].str.split().apply(lambda x: " : ".join(x))
    ca_df_enriched['Info'] = ca_df_enriched['Info'].str.replace(r',', ', ')
    # Add to comment column
    ca_df_enriched.loc[ca_df_enriched['Plat_GROSS'].isnull(),'Platform_Lookup'] = ca_df_enriched['Info']
    ca_df_enriched = ca_df_enriched.drop(columns=['Info'], axis=1)

    return ca_df_enriched

def flag_zero_divs(ca_df):
    """
    Flags zero dividends reported by Reuters with a comment
    :param ca_df:
    :return:
    """
    ca_df.loc[(ca_df['Reuters_GROSS'] == 0) & (ca_df['Reuters_NET'] == 0), 'Additional Comment'] += ' Reuters zero dividend'
    return ca_df

def analyze_datasets(reuters_data_dict, edi_data_dict, plat_data_dict, gigant_general):
    logger.debug('Compare data from the different vendors...')

    # Remove duplicates in datasets
    reuters_data_dict = remove_duplicates(reuters_data_dict)
    edi_data_dict = remove_duplicates(edi_data_dict)
    plat_data_dict = remove_duplicates(plat_data_dict)

    # Add empty columns to Reuters for subscription price and currency to allow for column-wise comparison
    reuters_data_dict['Rights issues'].loc[:,'Subscription Price'] = None
    reuters_data_dict['Rights issues'].loc[:,'Currency'] = None

    # identifier keys
    ident_keys_not_cash_div = ['RIC', 'Type', 'Execution Date']
    ident_keys_cash_div = ['RIC', 'Type', 'Execution Date', 'Dividend Taxation Type']
    # Compare Reuters, EDI and Platform
    stock_div_check = compare_ca(dfs=[reuters_data_dict['Stock dividends'],
                                        edi_data_dict['Stock dividends'],
                                        plat_data_dict['Stock dividends']],
                                   key=ident_keys_not_cash_div, gigant_general=gigant_general)
    stock_split_check = compare_ca(dfs=[reuters_data_dict['Stock splits'],
                                        edi_data_dict['Stock splits'],
                                        plat_data_dict['Stock splits']],
                                   key=ident_keys_not_cash_div, gigant_general=gigant_general)
    rights_check = compare_ca(dfs=[reuters_data_dict['Rights issues'],
                                   edi_data_dict['Rights issues'],
                                   plat_data_dict['Rights issues']],
                              key=ident_keys_not_cash_div, gigant_general=gigant_general)
    cash_divs_check = compare_ca(dfs=[reuters_data_dict['Cash dividends'],
                                      edi_data_dict['Cash dividends'],
                                      plat_data_dict['Cash dividends']],
                                 key=ident_keys_cash_div, gigant_general=gigant_general, per_column=True)

    # Add ADR to cash dividend df
    cash_divs_check = data_import.add_adr(cash_divs_check)

    # Add Event type to Rights issue
    rights_check = add_rights_event_type(rights_check, gigant_general)

    # Adjust mismatches due to rounding, add comment for changed observtions
    cash_divs_check = remove_rounding_mismatches(cash_divs_check, columns=['GROSS', 'NET'], digits=6)

    # Lookup Cash/Special Dividend if no data from platform
    cash_divs_check = platform_lookup(cash_divs_check, plat_data_dict['Raw data'])

    # Flag cash dividends with value zero from Reuters with a comment
    cash_divs_check = flag_zero_divs(cash_divs_check)

    cash_divs_check = data_import.add_comment(cash_divs_check, ['Additional Comment_temp', 'Franking amount', 'CFI amount'])

    # Adjust order of columns
    stock_div_check = stock_div_check[['RIC', 'ISIN', 'TICKER', 'Name', 'Type', 'Execution Date',
        'Reuters_Stock Dividend', 'EDI_Stock Dividend', 'Plat_Stock Dividend', 'Reuters-EDI', 'Reuters-Plat',
        'EDI-Plat','Additional Comment']].sort_values(by=['RIC', 'Execution Date'])

    stock_split_check = stock_split_check[['RIC', 'ISIN', 'TICKER', 'Name', 'Type', 'Execution Date',
        'Reuters_Relation', 'EDI_Relation','Plat_Relation',  'Reuters-EDI', 'Reuters-Plat', 'EDI-Plat',
        'Additional Comment']].sort_values(by=['RIC', 'Execution Date'])

    rights_check = rights_check[['RIC', 'ISIN', 'TICKER', 'Name', 'Type', 'Execution Date', 'Reuters_Terms',
       'Reuters_Subscription Price', 'Reuters_Currency', 'EDI_Currency', 'EDI_Terms', 'EDI_Subscription Price',
        'Plat_Currency', 'Plat_Subscription Price', 'Plat_Terms', 'Reuters-EDI', 'Reuters-Plat', 'EDI-Plat',
       'Additional Comment']].sort_values(by=['RIC', 'Execution Date'])

    cash_divs_check = cash_divs_check[['RIC', 'ISIN', 'TICKER', 'Name', 'Type', 'Execution Date', 'Dividend Taxation Type', 'Reuters_GROSS',
        'Reuters_NET','Reuters_Currency', 'EDI_GROSS', 'EDI_NET', 'EDI_Currency', 'Plat_GROSS', 'Plat_NET',
        'Plat_Currency', 'Reuters-EDI_GROSS', 'Reuters-Plat_GROSS', 'EDI-Plat_GROSS', 'Reuters-EDI_NET',
       'Reuters-Plat_NET', 'EDI-Plat_NET', 'Reuters-EDI_Currency', 'Reuters-Plat_Currency', 'EDI-Plat_Currency', 'Additional Comment', 'Platform_Lookup']].sort_values(by=['RIC', 'Execution Date'])

    return (stock_div_check, stock_split_check, rights_check, cash_divs_check)
