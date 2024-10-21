import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
engine = create_engine('')


# Function to calculate age
def calculate_age(dob):
    try:
        dob = datetime.strptime(str(dob), '%d%m%Y')  # Ensure DOB is a string
        today = datetime.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except ValueError as e:
        print(f"Error parsing DOB: {dob} - {e}")
        return None  # Return None or a default value if parsing fails


# Function to calculate days since last consulted
def days_since_last_consulted(last_date):
    try:
        last_date = datetime.strptime(str(last_date), '%d%m%Y')  # Ensure last date is a string
        today = datetime.today()
        return (today - last_date).days
    except ValueError as e:
        print(f"Error parsing Last Consulted Date: {last_date} - {e}")
        return None  # Return None or a default value if parsing fails


# Function to create country specific table
def create_country_table(table_name, conn):
    create_table_query = f"""
    CREATE TABLE {table_name} (
        Customer_Name VARCHAR(255),
        Customer_Id VARCHAR(18),
        Open_Date DATE,
        Last_Consulted_Date DATE,
        Vaccination_Id CHAR(5),
        Dr_Name VARCHAR(255),
        State CHAR(5),
        Country CHAR(5),
        DOB DATE,
        Is_Active CHAR(1),
        Age INT,
        Days_Since_Last_Consulted INT
    );
    """
    conn.execute(text(create_table_query))


# Function to load data into staging table and process
def process_customer_data(file_path):
    # Load the data file into a pandas DataFrame
    data = pd.read_csv(file_path, sep='|', header=None, skiprows=1,
                       names=['Record_Type', 'Customer_Name', 'Customer_Id', 'Open_Date',
                              'Last_Consulted_Date', 'Vaccination_Id', 'Dr_Name', 'State',
                              'Country', 'DOB', 'Is_Active'])

    # Filter only 'D' (detail) records
    detail_data = data[data['Record_Type'] == 'D']

    # Add derived columns
    detail_data['Age'] = detail_data['DOB'].apply(calculate_age)
    detail_data['Days_Since_Last_Consulted'] = detail_data['Last_Consulted_Date'].apply(days_since_last_consulted)

    # Remove audit columns if necessary
    detail_data.drop(['Record_Type'], axis=1, inplace=True)

    # Validate mandatory fields and correct data types
    mandatory_fields = ['Customer_Name', 'Customer_Id', 'Open_Date']
    for field in mandatory_fields:
        detail_data = detail_data[detail_data[field].notna()]

    # Insert data into respective country tables
    for country in detail_data['Country'].unique():
        country_table = f'Table_{country}'
        country_data = detail_data[detail_data['Country'] == country]

        # Check if the table exists and create if necessary
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, country_table):
                create_country_table(country_table, conn)

            # Insert data into the table
            country_data.to_sql(country_table, conn, if_exists='append', index=False)


# Sample file path
file_path = 'C:/Users/ASUS/OneDrive/Documents/Customer.txt'

# Process the customer data file
process_customer_data(file_path)