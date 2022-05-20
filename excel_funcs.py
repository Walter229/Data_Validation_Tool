import pandas as pd
from loguru import logger


def color_code_checks(wb):
    """
    Adds color coding to the check columns
    :param wb:
    :return:
    """
    # Define formats
    format_red = wb.add_format({'bg_color': '#FFC7CE',
                                   'font_color': '#9C0006'})
    format_green = wb.add_format({'bg_color': '#C6EFCE',
                                   'font_color': '#006100'})
    format_default = wb.add_format()

    # Define sheet area to apply styling to
    area = 'A1:AZ50000'

    for worksheet in wb.worksheets():
        # Remove formatting from blank cells
        worksheet.conditional_format(area, {'type': 'blanks',
                                                  'stop_if_true' : True,
                                                  'format': format_default})
        # Format 'FALSE' cells
        worksheet.conditional_format(area, {'type': 'cell',
                                            'criteria': 'equal to',
                                            'value': 'FALSE',
                                            'format': format_red})
        # Format 'TRUE' cells
        worksheet.conditional_format(area, {'type': 'cell',
                                             'criteria': 'equal to',
                                             'value': 'TRUE',
                                             'format': format_green})

    return None


def highlight_special_exchanges(wb):
    """
    Highlights special exchanges that need to be treated carefully in the Cash Dividends sheet
    :param wb:
    :return:
    """
    # Define formats
    format_orange = wb.add_format({'bg_color': '#E26B0A'})
    format_blue = wb.add_format({'bg_color': '#00B0F0'})
    format_default = wb.add_format()

    # Define area to apply formatting to
    area = 'A1:A50000'

    worksheet = wb.get_worksheet_by_name('Cash Dividends')

    # Remove formatting from blank cells
    worksheet.conditional_format(area, {'type': 'blanks',
                                              'stop_if_true' : True,
                                              'format': format_default})
    # Format .SA RICs
    worksheet.conditional_format(area, {'type': 'text',
                                        'criteria': 'containing',
                                        'value': '.SA',
                                        'format': format_orange})
    # Format Australian RICs
    worksheet.conditional_format(area, {'type': 'text',
                                        'criteria': 'containing',
                                        'value': '.AX',
                                        'format': format_blue})

    worksheet.conditional_format(area, {'type': 'text',
                                        'criteria': 'containing',
                                        'value': '.CHA',
                                        'format': format_blue})
    return None


def add_VBA(writer):
    """
    Adds VBA project stored in the same directory as vbaProject.bin to the project and adds buttons for the macros
    :param writer:
    :return:
    """
    workbook = writer.book
    workbook.filename = 'CA_check.xlsm'
    workbook.add_vba_project('./vbaProject.bin')
    worksheet = workbook.get_worksheet_by_name('Upload Sheet')
    worksheet.insert_button('U3', {'macro': 'Create_UploadSheet.Create_UploadSheet',
                                   'caption': 'Fill Upload Sheet',
                                   'width': 191,
                                   'height': 60})
    worksheet.insert_button('Y3', {'macro': 'Clear_UploadSheet.Clear_UploadSheet',
                                   'caption': 'Clear Upload Sheet',
                                   'width': 191,
                                   'height': 60})
    worksheet.insert_button('W7', {'macro': 'Save_UploadSheet.Save_UploadSheet',
                                   'caption': 'Create Upload Sheet file',
                                   'width': 191,
                                   'height': 60})
    return None


def add_blacklist_sheet(writer):
    pd.DataFrame().to_excel(writer, sheet_name='BL', index=False)


def add_ca_sheet(writer, end_date):
    """
    Adding a sheet with a new struct-query for corporate actions, so they can check afterwards
    :param writer: excel writer object for storing new sheets
    :param end_date: date for CAs
    :return:
    """
    ca_after_check = pd.DataFrame([''])
    ca_after_check.iloc[0,0] = f'=STRUCQUERY("corporateActions","","{end_date}","","","")'
    ca_after_check.to_excel(writer, sheet_name='CAs', index=False, header=False)

def add_manual_handling_columns(stock_div_check, stock_split_check,rights_check,cash_divs_check):
    """
    Adds the comment label and BA label at the end of the file
    :param stock_div_check: dataframe for stock dividend check
    :param stock_split_check: dataframe for stock-split check
    :param rights_check: dataframe for rights check
    :param cash_divs_check: dataframe for cash-dividend check
    :return:
    """
    stock_div_check.loc[:, ['BL', 'Comment']] = ''
    stock_split_check.loc[:, ['BL', 'Comment']] = ''
    rights_check.loc[:, ['BL', 'Comment']] = ''
    cash_divs_check.loc[:, ['BL', 'Comment']] = ''

def move_cols_back(df, cols_at_end):
    """
    Moves columns to the back in the ordner specified by the input list
    :param columns:
    :return:
    """
    df = df[[c for c in df if c not in cols_at_end] + [c for c in cols_at_end if c in df]]
    return df

def get_col_widths(df):
    # First we find the maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    # Then, we concatenate this to the max of the lengths of column name and its values for each column, left to right
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def format_file(wb, dfs):
    # Left align all columns of CA sheets
    relevant_sheets = ["Stock Dividends", "Stock Splits", "Rights Issues", "Cash Dividends"]
    left_format = wb.add_format()
    left_format.set_align('left')
    for sheet in relevant_sheets:
        ws = wb.get_worksheet_by_name(sheet)
        ws.set_row(0, None, left_format)

    # Expand Identifiers
    for j,df in enumerate(dfs):
        df = df.iloc[:,:3]
        for i, width in enumerate(get_col_widths(df)[1:]):
            ws = wb.get_worksheet_by_name(relevant_sheets[j])
            ws.set_column(i, i, width+1)

    # Set Zoom
    all_sheets = ['Stock Dividends', 'Stock Splits', 'Rights Issues', 'Cash Dividends', 'Upload Sheet', 'BL', 'CAs']
    for sheet in relevant_sheets:
        ws = wb.get_worksheet_by_name(sheet)
        ws.set_zoom(85)

    # Set TRUE/FALSE columns to a width of 5
    # Sheets that only have 3 columns
    single_sheets = ['Stock Dividends', 'Stock Splits', 'Rights Issues']
    col_names = ['Reuters-EDI', 'Reuters-Plat', 'EDI-Plat']
    for i, sheet in enumerate(single_sheets):
        df = dfs[i]
        ws = wb.get_worksheet_by_name(sheet)
        for col_name in col_names:
            col_index = df.columns.get_loc(col_name)
            ws.set_column(col_index, col_index, 6)
    # Cash Dividend Sheet
    col_names = ['Reuters-EDI_GROSS', 'Reuters-Plat_GROSS',	'EDI-Plat_GROSS', 'Reuters-EDI_NET', 'Reuters-Plat_NET',
                 'EDI-Plat_NET', 'Reuters-EDI_Currency', 'Reuters-Plat_Currency', 'EDI-Plat_Currency']
    df = dfs[3]
    ws = wb.get_worksheet_by_name('Cash Dividends')
    for col_name in col_names:
        col_index = df.columns.get_loc(col_name)
        ws.set_column(col_index, col_index, 6)

def create_excel(stock_div_check, stock_split_check,rights_check,cash_divs_check, end_date):
    # Create empty upload file
    upload_sheet = pd.DataFrame(
        columns=['Financial Instrument Ric*', 'TYPE*', 'Execution Date*', 'Pay Date', 'Is Ad Hoc*', 'CURRENCY*', 'VALUE',
         'RELATION', 'Purchase Price', 'Purchase Relation', 'Withholding Tax Type', 'Stock Dividend', 'Execution Order',
         'Dividend Subtype', 'Record Date', 'Dividend Taxation Type', 'Franking Amount', 'Cfi Amount', 'PID'])

    # Add empty and rearrange some existing columns
    add_manual_handling_columns(stock_div_check, stock_split_check, rights_check, cash_divs_check)
    cash_divs_check = move_cols_back(cash_divs_check, ['Platform_Lookup', 'Comment'])

    # Create basic file structure
    writer = pd.ExcelWriter(f'CA_check.xlsx', engine='xlsxwriter', datetime_format='yyyy/mm/dd')
    stock_div_check.to_excel(writer, sheet_name='Stock Dividends', index=False)
    stock_split_check.to_excel(writer, sheet_name='Stock Splits', index=False)
    rights_check.to_excel(writer, sheet_name='Rights Issues', index=False)
    cash_divs_check.to_excel(writer, sheet_name='Cash Dividends', index=False)
    upload_sheet.to_excel(writer, sheet_name='Upload Sheet', index=False)

    add_blacklist_sheet(writer)
    add_ca_sheet(writer, end_date)

    # Enhance excel file
    wb = writer.book

    # Color code vendor checks (TRUE/FALSE)
    color_code_checks(wb)

    # Highlight certain exchanges in Cash Dividend file
    highlight_special_exchanges(wb)

    # Add VBA macro to Excel file
    add_VBA(writer)

    # Format file
    format_file(wb, [stock_div_check, stock_split_check, rights_check, cash_divs_check])

    # Save file
    writer.save()

    logger.debug('File successfully created!')

