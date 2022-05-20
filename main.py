import datetime
import data_import
import data_analysis
import ca_types
import excel_funcs
import upload_process
import pandas as pd

pd.set_option('mode.chained_assignment', None)

def get_dates(start_t, end_t):
    """
    Returns start- and end-date in isoformat (YYYY-MM-DD) while taking care of weekends
    :param start_t:
    :param end_t:
    :return:
    """

    start_date = datetime.date.today()+ datetime.timedelta(days=start_t)
    end_date = datetime.date.today() + pd.tseries.offsets.BDay(end_t)

    # Transform to isoformat
    start_date = start_date.isoformat()[0:10]
    end_date = end_date.isoformat()[0:10]

    return start_date, end_date


def main():
    # Get start and end date
    start_date, end_date = get_dates(0, 2)

    # Create instance of Gigant ICE data to be used later on
    gigant_instance = ca_types.Gigant_Generell_Information()

    # Fetch all data from Reuters, EDI and Platform
    # Set to True, if you want to use EDI.csv file instead of API (located in dir: EDI/EDI.csv)
    edi_manual_file = False

    reuters_data_dict, edi_data_dict, plat_data_dict = data_import.fetch_all_data(start_date, end_date, gigant_instance, edi_manual_file)

    # Compare Reuters, EDI and Platform
    stock_div_check, stock_split_check, rights_check, cash_divs_check = data_analysis.analyze_datasets(reuters_data_dict, edi_data_dict, plat_data_dict, gigant_instance)

    # Add upload columns
    stock_div_upload = upload_process.add_stock_div_upload_cols(stock_div_check)
    stock_split_upload = upload_process.add_split_upload_cols(stock_split_check)
    rights_upload = upload_process.add_rights_upload_cols(rights_check)
    cash_divs_upload = upload_process.add_cash_div_upload_cols(cash_divs_check)

    # Create Excel file
    excel_funcs.create_excel(stock_div_upload, stock_split_upload, rights_upload, cash_divs_upload, end_date)

    return None


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
