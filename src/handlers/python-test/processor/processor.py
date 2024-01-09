import pandas as pd

def process_excel(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path, header=5, dtype={'Payroll Number*': str, 'Home Phone': str, 'Cell Phone': str})
    df = df.iloc[5:]
    df = df.iloc[:, 1:]

    # Rename columns to match Airtable column names
    column_mapping = {
        'Payroll Number*': 'LiQ - Payroll Number',
        'First Name*': 'LiQ - First Name',
        'Last Name*': 'LiQ - Last Name',
        'Address Line 1': 'LiQ - Address Line 1',
        'Address Line 2': 'LiQ - Address Line 2',
        'Address Town': 'LiQ - Address Town',
        'State': 'LiQ - Province',
        'Address Post Code': 'LiQ - Address Post Code',
        'Home Phone': 'LiQ - Home Phone',
        'Cell Phone': 'LiQ - Cell Phone',
        'Email': 'LiQ - Email',
        'Position*': 'LiQ - Position',
        'Subway Id': 'LiQ - Subway Id',
        'Salaried Employee': 'LiQ - Salaried Employee',
    }
    df.rename(columns=column_mapping, inplace=True)

    # Select only the columns present in column_mapping
    df = df[list(column_mapping.values())]

    # # # Convert date columns to ISO format and handle nulls
    # date_columns = ['LiQ - Date Of Birth', 'LiQ - Hired Date', 'LiQ - Separation date']
    # for col in date_columns:
    #     if col in df.columns:
    #         df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')


    for col in df.columns:
        if df[col].dtype == 'object':  # if column is of object type (string)
            df[col].fillna('', inplace=True)
        else:  # if column is of numeric type
            df[col].fillna(0, inplace=True)

    # Additional transformations
    # For example, converting 'Salaried Employee' to a boolean string
    if 'LiQ - Salaried Employee' in df.columns:
        df['LiQ - Salaried Employee'] = df['LiQ - Salaried Employee'].apply(lambda x: True if x else False)

    # Convert 'Position' column to an array of strings
    if 'LiQ - Position' in df.columns:
        df['LiQ - Position'] = df['LiQ - Position'].apply(lambda x: [str(x)])

    if 'LiQ - Subway Id' in df.columns:
        df['LiQ - Subway Id'] = df['LiQ - Subway Id'].astype(str)

    # if 'LiQ - Cell Phone' in df.columns:
    #     df['LiQ - Cell Phone'] = df['LiQ - Cell Phone'].astype(str)

    # if 'LiQ - Home Phone' in df.columns:
    #     df['LiQ - Home Phone'] = df['LiQ - Home Phone'].astype(str)


    # Additional transformations for other fields can be added here
    # ...

    return df
