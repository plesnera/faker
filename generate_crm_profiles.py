from faker import Faker
import csv
import datetime
import json

# https://faker.readthedocs.io/en/stable/fakerclass.html

# Hardcoded locales taken from the Faker class object
locale_list= ['cs_CZ', 'da_DK', 'de', 'de_AT', 'de_CH', 'de_DE', 'el_GR', 'en', 'en_AU', 'en_CA', 'en_GB', 'en_IE', 'en_IN', 'en_NZ', 'en_PH', 'en_US', 'es', 'es_ES', 'es_MX', 'fa_IR', 'fi_FI', 'fil_PH', 'fr_CH', 'fr_FR', 'he_IL', 'hi_IN', 'hr_HR', 'hu_HU', 'hy_AM', 'id_ID', 'it_IT', 'ja_JP', 'ka_GE', 'ko_KR', 'ne_NP', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_BR', 'pt_PT', 'ro_RO', 'ru_RU', 'sk_SK', 'sl_SI', 'sv_SE', 'ta_IN', 'th', 'th_TH', 'tl_PH', 'uk_UA', 'zh_CN', 'zh_TW']
# Using a file with externally provided CRM id's
source_file_name='user_ids_dec_m-29cc76b7.csv'
csv_target_file_name= "fake_crm.csv"
json_target_file_name='fake_crm.json'

def load_crm_data(source_file_path: str) -> (list, list):
    geo_country, app_user_id=list(),list()
    source_file=csv.reader(open(source_file_path))
    for row in source_file:
        geo_country.append(row[1])
        app_user_id.append((row[1],row[0]))
    return list(set(geo_country)),app_user_id


def map_country_to_locale(locales: list, countries: list) -> dict:
    locale_countries={entry[-2:]:entry for entry in locales}
    for e in countries:
        if e in locale_countries:
            continue
        else:
            locale_countries[e]='en'
    return locale_countries


def generate_record(user: str, country:str, faker: Faker) -> dict:
    consent_types={0: 'None', 1: 'Newsletter', 2: 'Newsletter, Offers', 3: 'Newsletters, New Products', 4: 'Promotions',5: 'Newsletter, New Products, Promotions'}
    profile=faker.profile()
    profile['userid']=user
    profile['country_code']=country
    profile['last_login']=datetime.datetime.strftime(faker.date_between_dates(datetime.date(2009,6,14),datetime.datetime.now()),'%Y-%m-%d')
    profile['clv'] = faker.random_number(digits=3)
    profile['consent'] = consent_types[faker.random_int(0,5)]
    profile['birthdate'] = datetime.datetime.strftime(profile['birthdate'], '%Y-%m-%d')
    del(profile['current_location'])
    del(profile['website'])
    return profile

def produce_fake_set(identities: list, mapping_dict: dict) -> None:
    fakers = Faker(locale_list)
    fake_crm_data=list()
    csv_file=csv.DictWriter(open(csv_target_file_name, mode='a', newline=''),
                            fieldnames=['userid', 'name','username', 'address','residence','company','job',
                                        'ssn','blood_group', 'country_code', 'mail', 'sex',
                                        'birthdate', 'last_login', 'clv', 'consent'])
    json_file=open(json_target_file_name, mode='a', newline='')

    def write_and_clear_cache():
        csv_file.writerows(fake_crm_data)
        # produce data for json newline and write to file
        records=[json.dumps(element,ensure_ascii=False) for element in fake_crm_data]
        json_file.writelines('\n'.join(records))
        print(str(index) + " profiles written to file:" + csv_target_file_name)
        fake_crm_data.clear()
        return

    for index, row in enumerate(identities):
        locale=mapping_dict[row[0]] ## convert locale to country list
        profile=generate_record(row[1], row[0], fakers[locale])
        fake_crm_data.append(profile)
        # write to file and clear memory for every 10.000 records
        if index>0 and index%10000 == 0:
            write_and_clear_cache()
    return


if __name__ == '__main__':
    crm_country_list, crm_identities_list = load_crm_data(source_file_name)
    mappings = map_country_to_locale(locale_list, crm_country_list)
    fake_crm_data=produce_fake_set(crm_identities_list, mappings)
print('done')


# Big query SQL used to extract user_id's:

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