#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 16:29:18 2019

@author: georgemeachim
"""
import pandas as pd
from sqlalchemy import create_engine

"""The engine creates a Dialect object tailored towards PostgreSQL, as well as a Pool object which
 will establish a DBAPI connection at localhost:5432 when a connection request is first
 received. Note that the Engine and its underlying Pool do not establish the first actual
 DBAPI connection until the Engine.connect() method is called, or an operation which is
 dependent on this method such as Engine.execute() is invoked.
 In this way, Engine and Pool can be said to have a lazy initialization behavior."""
 
#engine = create_engine('postgresql://postgres:l=den1TiFT@localhost:5432/dvdrental')  # Creating the engine

engine = create_engine('postgresql+psycopg2://postgres:l=den1TiFT@localhost:5432/dvdrental')

query1 = "SELECT * FROM actor;"  # String containing the SQL query to select all rows
df_actor = pd.read_sql_query(query1, engine, parse_dates=['last_update'], index_col='last_update')  # Finally, importing the data into DataFrame df
#df_actor.info()

query2 = "SELECT * FROM address;"  # String containing the SQL query to select all rows
df_address = pd.read_sql_query(query2, engine, parse_dates=['last_update'], index_col='last_update')  
#df_address.info()

query3 = "SELECT * FROM category;"  # String containing the SQL query to select all rows
df_category = pd.read_sql_query(query3, engine, parse_dates=['last_update'], index_col='last_update')
#df_category.info()

query4 = "SELECT * FROM city;"  # String containing the SQL query to select all rows
df_city = pd.read_sql_query(query4, engine, parse_dates=['last_update'], index_col='last_update') 
#df_city.info()

query5 = "SELECT * FROM country;"  # String containing the SQL query to select all rows
df_country = pd.read_sql_query(query5, engine, parse_dates=['last_update'], index_col='last_update') 
#df_country.info() 

query6 = "SELECT * FROM customer;"  # String containing the SQL query to select all rows
df_customer = pd.read_sql_query(query6, engine, parse_dates=['last_update'], index_col='last_update') 
#df_customer.info()

query7 = "SELECT * FROM film;"  # String containing the SQL query to select all rows
df_film = pd.read_sql_query(query7, engine, parse_dates=['last_update'], index_col='last_update') 
#df_film.info()

query8 = "SELECT * FROM film_actor;"  # String containing the SQL query to select all rows
df_film_actor = pd.read_sql_query(query8, engine, parse_dates=['last_update'], index_col='last_update') 
#df_film_actor.info()

query9 = "SELECT * FROM film_category;"  # String containing the SQL query to select all rows
df_film_category = pd.read_sql_query(query9, engine, parse_dates=['last_update'], index_col='last_update') 
#df_film_category.info()

query10 = "SELECT * FROM inventory;"  # String containing the SQL query to select all rows
df_inventory = pd.read_sql_query(query10, engine, parse_dates=['last_update'], index_col='last_update')  
#df_inventory.info()

query11 = "SELECT * FROM language;"  # String containing the SQL query to select all rows
df_actor = pd.read_sql_query(query11, engine, parse_dates=['last_update'], index_col='last_update') 
#df_actor.info()

query12 = "SELECT * FROM payment;"  # String containing the SQL query to select all rows
df_language = pd.read_sql_query(query12, engine, parse_dates=['payment_date'], index_col='payment_date') 
#df_language.info()

query13 = "SELECT * FROM rental;"  # String containing the SQL query to select all rows
df_rental = pd.read_sql_query(query13, engine, parse_dates=['rental_date','return_date','last_update'])
#df_rental.info()

query14 = "SELECT * FROM staff;"  # String containing the SQL query to select all rows
df_staff = pd.read_sql_query(query14, engine, parse_dates=['last_update'], index_col='last_update')
#df_staff.info() 

query15 = "SELECT * FROM store;"  # String containing the SQL query to select all rows
df_store = pd.read_sql_query(query15, engine, parse_dates=['last_update'], index_col='last_update')  
#df_store.info()
