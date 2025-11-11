# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session


# Write directly to the app
st.title(f":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """
)

name_on_order =st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

# my_dataframe = cnx.query("SELECT fruit_name FROM SMOOTHIES.PUBLIC.FRUIT_OPTIONS")
# # st.dataframe(my_dataframe)

# # df = session.table(smoothies.public.fruit_options).select(fruit_name).distinct()
# options_list = my_dataframe[fruit_name].tolist()

# Function to get a Snowpark session (works automatically in Streamlit in Snowflake)
# def get_snowflake_session():
    # try:
        # session = st.connection("snowflake")
    # except Exception as e:
        # # Handle local development connection (using secrets.toml)
        # conn = st.connection("snowflake")
        # session = conn.get_snowpark_session()
    # return session


@st.cache_data
def get_dropdown_options(table_name, column_name):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    # Use session.sql to run the query and convert to a Pandas DataFrame
    df = cnx.query(query).to_pandas()
    # Extract the column values into a list
    options_list = df[column_name].tolist()
    return options_list
    
   # Define your table and column names
TABLE_NAME = 'SMOOTHIES.PUBLIC.FRUIT_OPTIONS' 
COLUMN_NAME = 'FRUIT_NAME'

# Fetch the options list
options = get_dropdown_options(TABLE_NAME, COLUMN_NAME)

# Create the multiselect widget with a limit of 5 selections
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients'
    , options
    , max_selections = 5
)

if ingredients_list:
    
    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen

    my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order)
                values ('""" + ingredients_string + """', '""" +name_on_order+"""')"""
    

    time_to_start = st.button('Submit Order')

    if time_to_start:
        session.sql(my_insert_stmt).collect()
        
        st.success("""Your Smoothie is ordered, '""" + name_on_order +"""'!""", icon="✅")