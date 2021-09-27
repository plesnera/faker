# SIMPLE FAKER
Project using the Python fake library to generate a fake CRM dataset based on existing and supplied user_ids and takes into consideration the local/Country code to ensure some alignment.
It consists of the following functions:
* load_crm_userdata - reads a csv file containing user_id and country_code.
* map_country_to_local - creates a mapping dictionary based on country codes and locales.
* generate_record - the row generator 
* produce_fake_set - function that iterates over the user set provided and creates a DataFrame containing the fake CRM records. 

Included in the project is the csv file '100k_userids_m-29cc76b7.csv' containing all user profiles as well as a small sample set.


### TODO
Refactor to increase performance - too slow. 