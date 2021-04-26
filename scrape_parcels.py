from concurrent.futures import ThreadPoolExecutor
import re

import requests
from bs4 import BeautifulSoup
import geopandas as gpd


url = 'https://apps.richmondgov.com/applications/propertysearch/Detail.aspx?pin={}'
SQ_FT_TO_ACRE = 2.29568e-5


def val_to_float(s):
    if s.startswith('$'):
        s = s[1:]
    s = s.replace(',', '')
    return float(s)



def get_info(pin):
    print(pin)
    dl = url.format(pin)
    resp = requests.get(url.format(pin))
    content = BeautifulSoup(resp.content, 'html.parser')
    #wanted = content.find_all('fieldset', class_='detailSet')
    with open('output/{}.html'.format(pin), 'w') as f:
        f.write(str(content))


def extract_addresses(tag):
    label_and_values = list(zip(tag.find_all('span', class_='columnLabel'), tag.find_all('span', class_='fieldValue')))
    addrs = []
    for label, value in label_and_values:
        if label.text.strip() == 'Street Address:':
            addrs.append(value.text.strip('- \n\r\t')) #may have to keep adjusting chars
        elif label.text.strip() == 'Alternate Street Addresses:':
            alts = value.text.split('\n')
            alts = [a.strip('- \r\t') for a in alts]# check for other chars
            addrs.extend(alts)
    addrs_one_space = [re.sub(' +', ' ', ad) for ad in addrs]
    addrs_no_return = [re.sub('\n|\r|\t', '', ad) for ad in addrs_one_space]
    return addrs_no_return # oops, that's kinda confusing haha


# turns out this is actually in the geodataframe so we can ignore this function
def extract_values(tag):
    label_and_values = list(zip(tag.find_all('span', class_='columnLabel'), tag.find_all('span', class_='fieldValue')))
    land_value = 0
    improv_value = 0
    tax = 0
    for label, value in label_and_values:
        if label.text.strip() == 'Land Value:':
            land_value = val_to_float(value.text.strip())
        elif label.text.strip() == 'Improvement Value:':
            improv_value = val_to_float(value.text.strip())
        elif label.text.strip() == 'Area Tax:':
            tax = val_to_float(value.text.strip())
    return (land_value, improv_value, tax)


# as below, there's no way to match the fieldText with the columnLabel
# the other similar alternative, since I don't like looping over a shitload
# of unrelated tags, is find_all('div'), but those aren't labeled in any way
# so it would still be looping through each and checking the text
def extract_addresses_text(tag):
    tag.find_all('span', class_='columnLabel', text=re.compile('^.<text>.*$', flags=re.M))


if __name__ == '__main__':
    df = gpd.read_file('data') #this will probably be data/parcels
    df['LandAcre'] = df['LandSqFt'] * SQ_FT_TO_ACRE
    df['ValuePerAcre'] = df['TotalValue'] / df['LandAcre']
    df.columns = df.columns.str.lower()
    pins = df['pin'].unique().tolist()
    threads = 8

    files = os.listdir('output')
    fnames = [f[:-4] for f in files]
    new_pins = set(pins) - set(fnames)
    pins = list(new_pins)
    if len(pins) > 0:
        pass
#        with ThreadPoolExecutor(max_workers=threads) as executor:
#            executor.map(get_info, pins)
    else:
        print('No new parcel information added. Continuing')

