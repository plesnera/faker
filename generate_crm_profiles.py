# Using data from the app_id m-29cc76b7 in the anonymized dataset collected between 2020-12-03 and 2020-12-10 filtered on the requirements:
# userid not empty and length 33, geo_contry there and more than one event.
# Big qUery inserted:

# select user_id, geo_country, count(*)
# from `verdant-coyote-218213.momentum.beam_events`
# where ymd between '20201203' and '20201210'
# and user_id !='' and geo_country !=''
# and app_id='m-29cc76b7'
# and length(user_id)=33
# group by user_id,geo_country
# having count(*)>1
# order by count(*) desc
# limit 100000


from faker import Faker
from pandas import read_csv, DataFrame
import datetime
# https://faker.readthedocs.io/en/stable/fakerclass.html

# Hardcoded locales taken from the Faker class object
locale_list= ['cs_CZ', 'da_DK', 'de', 'de_AT', 'de_CH', 'de_DE', 'el_GR', 'en', 'en_AU', 'en_CA', 'en_GB', 'en_IE', 'en_IN', 'en_NZ', 'en_PH', 'en_US', 'es', 'es_ES', 'es_MX', 'fa_IR', 'fi_FI', 'fil_PH', 'fr_CH', 'fr_FR', 'he_IL', 'hi_IN', 'hr_HR', 'hu_HU', 'hy_AM', 'id_ID', 'it_IT', 'ja_JP', 'ka_GE', 'ko_KR', 'ne_NP', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_BR', 'pt_PT', 'ro_RO', 'ru_RU', 'sk_SK', 'sl_SI', 'sv_SE', 'ta_IN', 'th', 'th_TH', 'tl_PH', 'uk_UA', 'zh_CN', 'zh_TW']

def load_crm_data(file_path)-> (list, DataFrame):
    crm_source_file = read_csv(file_path)
    # hardcoded sort key - not strictly necessary to sort but will make look-ups later more efficient
    crm_source_file.sort_values('geo_country')
    country_list = crm_source_file['geo_country'].unique().tolist()
    identities_df = crm_source_file[['user_id', 'geo_country']]
    return (country_list, identities_df)

def map_country_to_locale(locales, countries)->dict:
    locale_countries={entry[-2:]:entry for entry in locales}
    for e in countries:
        if e in locale_countries:
            continue
        else:
            locale_countries[e]='en'
    return locale_countries

def generate_record(user, locale, fakers):
    consent_types={0:"None", 1:"Newsletter",2:"Newsletter, Offers", 3:"Newsletters, New Products", 4:"Promotions", 5:"Newsletter, New Products, Promotions"}
    faker = fakers[locale]
    name = faker.name()
    address = faker.address()
    city = faker.city()
    country = faker.country()
    email = faker.email()
    date_of_birth=faker.date_of_birth(minimum_age=18)
    last_login=faker.date_between_dates(datetime.date(2009,6,14),datetime.datetime.now())
    clv = faker.random_number(fix_len=False)
    consent = consent_types[faker.random_int(0,5)]
    return DataFrame({"user_id": [user], "name":[name], "address":[address], "city":[city], "country":[country], "email":[email], "date_of_birth": [date_of_birth],"last_login":[last_login],"clv":[clv],"consent":consent})

def produce_fake_set(identity_dict: DataFrame, mapping_dict)->DataFrame:
    fakers = Faker(locale_list)
    fake_crm_data=DataFrame()
    # Lookup the locale matching
    for index, row in identity_dict.iterrows():
        fake_row=generate_record(row['user_id'],mapping_dict[row['geo_country']],fakers)
        fake_crm_data=fake_crm_data.append(fake_row)
    return fake_crm_data

if __name__ == '__main__':
    country_list, identities = load_crm_data('100k_userids_m-29cc76b7.csv')
    # country_list, identities = load_crm_data('test_sample.csv')
    mappings = map_country_to_locale(locale_list, country_list)
    fake_crm_data=produce_fake_set(identities, mappings)
    fake_crm_data.to_csv('fake_crm.csv',mode="w",index=False)

print('done')
