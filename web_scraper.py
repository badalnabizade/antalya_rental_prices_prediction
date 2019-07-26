import bs4, time, unicodedata, re, random, warnings, requests, os
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
from connection_manager import ConnectionManager

#Scraper for sahibinden.com
def sahibinden_scraper(num_page):
    """
    Web scraper for sahibinden.com
    param num_page: number of page that wanted to be scraped
    """
    cm = ConnectionManager()
    start_time = time.time()
    data_ids = []
    seller_names = []
    seller_links = []
    ad_titles = []
    ad_links = []
    prices = []
    counties = []
    districts = []
    other_items = [] # this is a container list for attirbutes like date, area, room, floor within building, etc.

    diff = num_page - 2
    last = ((diff * 20) + 20) +1

    for t in range(0, last, 20):
        url = 'https://www.sahibinden.com/kiralik-daire/antalya?pagingOffset='+str(t)
        r = cm.request(url)
        status_code =r.code if r != '' else -1   # Status code of request.
        if status_code == 200:
            so=bs4.BeautifulSoup(r.read(), 'lxml')
            table = so.find('table', id='searchResultsTable')

            tracker = 0
            requests = 0
            for i in table.findAll('tr'):

                if i.get('data-id') is None: # means it is not a home ad. Maybe just a google ad.
                    continue # ignore these data points

                else: # if data points are home ads
                    ids = i.get('data-id')
                    names_seller = [_.get('title') for _ in (i.find_all('a', attrs={'class':'titleIcon store-icon'}))]
                    links_seller = [_.get('href') for _ in (i.find_all('a', attrs={'class':'titleIcon store-icon'}))]
                    titles_ad = [' '.join(_.text.split()) for _ in i.find_all('a', attrs={'class':'classifiedTitle'})]
                    links_ad = ['https://www.sahibinden.com'+_.get('href') for _ in i.find_all('a', attrs={'class':'classifiedTitle'})]

                    data_ids.append(ids)
                    seller_names.append(names_seller)
                    seller_links.append(links_seller)
                    ad_titles.append(titles_ad)
                    ad_links.append(links_ad)

                    #finding prices corresponding to ads
                    for z in i.find_all('td', attrs={'class':'searchResultsPriceValue'}):
                        price = ' '.join(z.text.split())

                        if '.' in price: # if price is like 7.100

                            # convert it to 7100 and keep it in 'prices' list
                            prices.append(price.replace('.', ''))

                        else: # if it is not, just put it to the 'prices' list
                            prices.append(price)

                    # finding addresses corresponding to ads
                    for f in i.findAll('td', attrs={'class':'searchResultsLocationValue'}):
                        turkish_adress_name = ''.join(f.text.split()) # name of county and district in Turkish

                        #converting Turkish charecters to English charecters
                        normalized = unicodedata.normalize('NFD', turkish_adress_name)
                        #converted_adress_name consists of county and district names.
                        converted_adress_name = u"".join([c for c in normalized if not unicodedata.combining(c)])

                        #first_letters is a list consists of first letters of county and district names. For example: ['M', 'F']
                        first_letters = re.findall('[A-Z]+', converted_adress_name)

                        # if district name is not specified, then 'first_letters' will contain only one letter, like ['A']
                        try:
                            # first_letters[1] is a district name
                            # first_letters[0] is a county name
                            districts.append( converted_adress_name[converted_adress_name.find(first_letters[1], 1):] )
                            counties.append(converted_adress_name[converted_adress_name.find(first_letters[0], 0):converted_adress_name.find(first_letters[1], 1)])

                        except: # index error
                            # it will occur, if first_letters includes only one letter.
                            # if district is not specified, put county name both district and county containers.
                            counties.append( converted_adress_name[converted_adress_name.find(first_letters[0]):] )
                            districts.append( converted_adress_name[converted_adress_name.find(first_letters[0]):] )



                    # finding other data points like date of ad, number of rooms, total area, floor within building etc.
                    # to do this, i will iterate through all of advertisement links
                    for y in links_ad:
                        start_time = time.time()
                        ur = cm.request(y)
                        tracker+=1
                        time.sleep(random.randint(2,6))
                        requests+=1
                        elapsed_time = time.time() - start_time
                        print('Request:{}; Frequency: {} requests/s; elapsed time:{}'.format(requests, requests/elapsed_time, elapsed_time))

                        soup = bs4.BeautifulSoup(ur.read(), 'lxml')
                        info =  soup.find('div', attrs={'class':'classifiedInfo'}) # attributes of ads

                        dct = {} # this will contain column names as keys, and data points as values.
                        for v in info.findAll('ul'):
                            head = v.find_all('strong') # name of columns. i.e. 'number_of_rooms' column.
                            attribute = v.find_all('span') # value corresponds to above particular column. i.e. (3+1)

                            for a,b in zip(head, attribute):
                                columns = ' '.join(a.text.split())
                                data = ' '.join(b.text.split())
                                dct[columns] = data

                        print(tracker)
                        if tracker % 5 == 0:
                            cm.new_identity() # After sending 5 requests, change identity in order to avoid being blocked.

                        other_items.append(pd.DataFrame(data = dct, index=[0]))
            print('done')

        else: # If status code of request is not 200.
            break

    print("--- %s seconds ---" % (time.time() - strt))
    return other_items, data_ids, ad_titles, ad_links, seller_names, seller_links, prices, counties, districts

def processing_for_sahibinden(scraper):
    """
    Converts scraped data to pandas data frame and does some basic data cleaning.
    """
    #There are 50 pages in sahibinden.com website, which include home ads.
    other_items, data_ids, ad_titles, ad_links, seller_names, seller_links, prices, counties, districts = scraper(50)

    first_df = pd.concat(other_items, ignore_index=True, sort=False)
    first_df.drop(['Emlak Tipi', 'Kimden', 'Site Adı'], 1, inplace=True) # Dropping redundant columns.

    second_df = pd.DataFrame({'id':data_ids,
                  'ad_title':ad_titles,
                  'ad_link':ad_links,
                  'seller_name':seller_names,
                  'seller_link':seller_links,
                  'price': prices,
                  'county':counties,
                  'district':districts})

    def NaN_convertor(item):
        """
        Checks seller_name, seller_link, ad_title and ad_link for NaN values.
        If those include NaN values, converts them to 'from_owner'
        """
        try:
            return item[0] #name of real estate agency.
        except:
            return 'from owner'

    second_df['seller_name'] = second_df['seller_name'].apply(NaN_convertor)
    second_df['seller_link'] = second_df['seller_link'].apply(NaN_convertor)
    second_df['ad_title'] = second_df['ad_title'].apply(NaN_convertor)
    second_df['ad_link'] = second_df['ad_link'].apply(NaN_convertor)

    final_data = pd.merge(second_df, first_df, left_on='id', right_on='İlan No')
    return final_data


#Scraper for emlakjet.com
def emlakjet_scraper(page_num):
    start = time.time()
    districts = []
    counties = []
    ad_names = [] # list that consists of ad titles.
    prices = []
    data = [] # list that contains other home attirbutes like numbe of rooms, building age, net area etc.
    ids = []

    last = page_num+1
    for i in range(1, last):
        url = requests.get('https://www.emlakjet.com/kiralik-daire/antalya/{}/'.format(i)).text
        soup = bs4.BeautifulSoup(url, 'lxml')
        # links of home ads.
        links = ['https://www.emlakjet.com'+i.get('href') for i in soup.findAll('a', attrs={'class':'listing-url'})]

        for a in links:
            try:
                r = requests.get(a).text
                s = bs4.BeautifulSoup(r, 'lxml')
                #titles of home ads.
                ad_names.append(s.find('h1', attrs={'class':'announTitle'}).text)
                # prices of home ads.
                prices.append(s.find('li', attrs={'class':'priceBox'}).text)
                # ids of home ads are like 210-24565. This code removes dash.
                id = a.split('-')[-1].split('/')[0]
                ids.append(id)

                #iterating through html tag, that includes home addresses.
                for b in s.findAll('li', attrs={'class':'spr-breadcrumb-right-gray'})[-1]:
                    try:
                        # name of district corresponds to particular ad.
                        district = str(b).split('Kiralık Daire&gt;')[-1].split('&gt;')[2].split('}')[0]
                        # name of county corresponds to particular ad.
                        county = str(b).split('Kiralık Daire&gt;')[-1].split('&gt;')[1]
                        districts.append(district)
                        counties.append(county)
                    except:
                        continue

                # left side of table that consists of home attributes like number of rooms, building age etc.
                table_left = s.find('div', {'class':'leftSide'})
                # right side of table that consists of home attributes.
                table_right = s.find('div', {'class':'rightSide'})

                dc = {} # includes column names as keys and rows as values.
                # iterating through items in attribute tables.
                for l,r in zip(table_left, table_right):
                    try:
                        # column names in left side of table.
                        columns_left = str(l).split('<span>')[1].split('</span>')[0]
                        # values of that particular column names in left side of table.
                        rows_left = str(l).split('<span>')[2].split('</span>')[0]
                        #column names in right side of table.
                        columns_right = str(r).split('<span>')[1].split('</span>')[0]
                        # values of that particular columns in right side of table.
                        rows_right = str(r).split('<span>')[2].split('</span>')[0].split('<')[0]

                        dc[columns_right] = rows_right
                        dc[columns_left] = rows_left

                    except:
                        continue
                # list of pandas dataframes.
                data.append(pd.DataFrame(dc, index=[0]))

            except:
                continue

    print("--- {}s seconds ---".format(time.time()- start))
    return data, ids, districts, counties, ad_names, prices

def processing_for_emlakjet(scraper):
    #There are 83 pages which include home ads
    data, ids, districts, counties, ad_names, prices = scraper(83)
    # concatinating list of pandas dataframes.
    first_df = pd.concat(data, ignore_index=True)

    second_df = pd.DataFrame({'İlan Numarası':ids,
                  'district':districts,
                  'county':counties,
                  'Title': ad_names,
                  'price':prices})

    #merging above two data frames on ad ids.
    final_df = pd.merge(first_df, second_df, left_on='İlan Numarası', right_on='İlan Numarası')
    # dropping possible duplicates.
    final_df.drop_duplicates(inplace=True)
    return final_df

#Scraper for hurriyetemlak.com
def hurriyetemlak_scraper(page_num):
    cm = ConnectionManager() # instance of connection manager object. This will be used for changing identity
    start_time = time.time()
    data = [] # list that contains all of the home attirbutes like numbe of rooms, building age, net area etc.
    last = page_num+1
    page_tracker = 0

    for t in range(0, last):

        url = 'https://www.hurriyetemlak.com/antalya-kiralik/daire?page={}'.format(t)
        rr = cm.request(url)
        # status code of request.
        status_code =rr.code if rr != '' else -1

        if status_code == 200:
            soup = bs4.BeautifulSoup(rr.read(), 'lxml')
            page_tracker+=1
    #         links = []
    #         for i in soup.findAll('div', attrs={'class':'list-item timeshare clearfix'}):
    #             attrs = i.find('a')
    #             links.append('https://www.hurriyetemlak.com'+attrs.get('href'))

        # above commented code is first verison of script for getting home advertisement links
        # this code is a map verison of that for speed issiue.
            links = list(map(lambda x: 'https://www.hurriyetemlak.com'+x.find('a').get('href'),
                             soup.findAll('div', attrs={'class':'list-item timeshare clearfix'})))

        else:
            print('Request failed.')
            break

        tracker = 0
        requests = 0 # to track number of requesets that has been successfully sent to website.
        for _ in links:
            start_time = time.time()
            r = cm.request(_)
            tracker+=1
            # in order to avoid overloading website, wait between 2 and 7 seconds for each iteration.
            time.sleep(random.randint(2,7))
            requests+=1
            elapsed_time = time.time() - start_time
            print('Request:{}; Frequency: {} requests/s; elapsed time:{}'.format(requests, requests/elapsed_time, elapsed_time))

            stat_code = r.code if r != '' else -1 # status code of request.
            if stat_code == 200:
                so = bs4.BeautifulSoup(r.read(), 'lxml')
                #lists of attributes of home ads that also includes html tags.
                raw_items = [b.find_all('span') for f in so.findAll('li', attrs={'class':'info-line'}) for b in f.findAll('li')]
            else:
                print('Request failed')
                break

            print(tracker)
            if tracker % 5 == 0:
                cm.new_identity()

            revised_items = []
            for r in raw_items:
                revised_items.append(r)
                # if this particular element is just empty list.
                if str(r) == '[]':
                    break

            dct = {} # dictionary contains column names as keys and values of that columns as values.


            for i in revised_items[:-1]: # last element of revised_items list is just empty list. ignore it.
                try:
                    # names of attributes corresponds to particular home ad.
                    col = str(i[0]).split('>')[1].split('<')[0]
                    # values of above attirbutes.
                    row = str(i[1]).split('>')[1].split('<')[0]
                    dct[col] = row

                    # id_col and id_row are id of particular home ad.
                    id_col = so.find('li', attrs={'class':'realty-numb'}).text.replace('\n', '', 3).split(':')[0]
                    id_row = so.find('li', attrs={'class':'realty-numb'}).text.replace('\n', '', 3).split(':')[1]
                    dct[id_col] = id_row
                    # title of particular home ad.
                    title = so.find('h1', attrs={'class':'details-header'}).text
                    dct['Title'] = title
                    # price of particular home ad.
                    price = so.find('li', attrs={'class':'price-line clearfix'}).text.replace('\n', '', 3)
                    dct['price'] = price
                except:
                    continue
            # list of pandas dataframes.
            data.append(pd.DataFrame(dct, index=[0]))

            delays = [0.5, 1, 1.5, 2, 2.5, 3]
            time.sleep(np.random.choice(delays)) # to avoid overloading website.

        print("page {} is done".format(page_tracker))

    print(time.time() - strt_time)
    return data

def processing_for_hurriyetemlak(scraper):
    # There are 96 pages which include home ads
    data = hurriyetemlak_scraper(96)
    hurriyetemlak_df =pd.concat(data, ignore_index=True)
    return hurriyetemlak_df

# Finding counties and districts for scraped data from hurriyetemlak.com
def locations_for_hurriyetemlak_df(page_num):
    districts = []
    counties = []
    ids = []
    last = page_num

    for t in range(1, last):
        rr = requests.get('https://www.hurriyetemlak.com/antalya-kiralik/daire?pageSize=50&page={}'.format(t),
                              verify=False, timeout=10)
        soup = bs4.BeautifulSoup(rr.text, 'lxml')

        for i, b in zip(soup.findAll('div', attrs={'class':'list-item timeshare clearfix'}),
                        soup.findAll('div', attrs={'class':'list-item timeshare clearfix'})):

            id = b.find('a').get('href').split('/')[-1]
            locations = (i.find_all('li', attrs={'class':'location'}))
            for a in locations:
                leng+=1
                locs = a.find_all('span')[1:]
                county = str(locs[0]).split('>')[1].split('<')[0]
                district = str(locs[1]).split('>')[1].split('<')[0]
                counties.append(county)
                districts.append(district)
                ids.append(id)

    locs_for_huriyyet_df = pd.DataFrame({'county':counties,
              'district':districts,
             'id': ids})

    return locs_for_huriyyet_df


def main():
    sahibinden_data = processing(sahibinden_scraper)
    sahibinden_data.to_csv('./data/sahibinden_final.csv')

    emlakjet_df = processing_for_emlakjet(emlakjet_scraper)
    emlakjet_df.to_csv('./data/emlakjet_data.csv')

    hurriyetemlak_df = processing_for_hurriyetemlak(hurriyetemlak_scraper)
    hurriyetemlak_df.to_csv('./data/huriyyet_final_df.csv')

    locs_for_huriyyet_df = locations_for_hurriyetemlak_df(50)
    locs_for_huriyyet_df.to_csv('./data/locs_for_huriyyet_df.csv')

if __name__ == '__main__':
    main()
