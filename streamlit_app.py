# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col
import requests

cnx = st.connection("snowflake")
session = cnx.session
raw_conn = cnx.raw_connection
cursor = raw_conn.cursor()

# Write directly to the app
st.title(f":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """
)

name_on_order =st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

@st.cache_data
def get_dropdown_options(table_name, column_name):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    # Use preset connection to run the query and convert to a Pandas DataFrame
    my_dataframe = cnx.query(query) 
    # Extract the column values into a list
    options_list = my_dataframe[column_name].tolist()
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
        search_on=pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', search_on, '.')
        st.subheader(fruit_chosen + ' Nutrition Information')
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + fruit_chosen)
        sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width = True)

    my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order)
                values ('""" + ingredients_string + """', '""" +name_on_order+"""')"""
    

    time_to_start = st.button('Submit Order')

    if time_to_start:
        try:
            with cnx.cursor() as cur:
                cur.execute(my_insert_stmt)
                cnx.commit()
                st.success("""Your Smoothie is ordered, '""" + name_on_order +"""'!""", icon="✅")
        except Exception as e:
            st.error(f"Error inserting data: {e}")

