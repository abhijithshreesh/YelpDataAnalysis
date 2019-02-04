from Geolocation import Location
#import csv
import pandas as pd

from uszipcode import ZipcodeSearchEngine
search = ZipcodeSearchEngine()

#df = pd.read_csv('./combined_data/data0.csv')

zipdf = pd.read_csv('./zip_codes_states.csv')

zip_county = zip(zipdf['zip_code'], zipdf['state'] + ', ' + zipdf['county'])

zcdict = {}
for code, county in zip_county:
    zcdict[code] = county



#print(df.head())


#open county code file

fccdf = pd.read_csv('./FipsCountyCodes.csv')
FipsCountyCodes = {}
county_code_zip = zip(fccdf['FIPS'], fccdf['Name'])
for fipcode, name in county_code_zip:
    FipsCountyCodes[name] = fipcode

loc = Location()

#iterate through each file
for fileNo in range(28, 41):
    csvfilenameToOpen = 'data' + str(fileNo)
    csvfilenameToWrite = 'NewCountyData' + str(fileNo)
    df = pd.read_csv('./combined_data/' + csvfilenameToOpen + '.csv')

    lat = df['latitude']
    long = df['longitude']

    df['County'] = df['latitude']
    df['CountyCode'] = df['latitude']
    df.CountyCode = df.CountyCode.astype("int")
    count = len(df['latitude'])
    count2 = len(df['longitude'])



    for i in range(count):

    #location = loc.GetLocation(latitude, longitude)

        latitude = lat[i]
        longitude = long[i]

        result = search.by_coordinate(latitude, longitude, radius=50, returns=5)
        zipcodeList = [int(result[index]['Zipcode']) for index in range(len(result))]
        a = 20

        county = None
        for index in range(len(zipcodeList)):
            try:
                #x = zcdict[89]
                county = zcdict[zipcodeList[index]]
            except:
                pass
            if county != None:
                break

        if county == None:
            try:
                location = loc.GetLocation(latitude, longitude)
                address = location.split(', ')
                cnty = address[-4].split(' ')[0]
                state = address[-2].split(' ')[0]
                county = state + ', ' + cnty
            except:
                print('Error'+' ' + latitude + ' ' + longitude + ' ' + str(location))
                county = None
        #colCounty[i] = county

        df.iloc[i, df.columns.get_loc('County')] = county
        #print(county)
        countycode = FipsCountyCodes[county]

        #Get the county code
        df.iloc[i, df.columns.get_loc('CountyCode')] = countycode

    #df.loc[idx, i] = county

    #Dumping df to csv
    df.to_csv('./Combined_Data_With_Codes/' + csvfilenameToWrite + '.csv', encoding='utf-8', index=False)

#

#
# wr = open('newd.csv', 'w')
#
# with open('./combined_data/data0.csv', newline='') as csvfile:
#     data = csv.reader(csvfile)
#     for row in data:
#         newrow = ','.join(row)
#         latitude = row[6]
#         longitude = row[7]
#         location = ''
#         try:
#             location = loc.GetLocation(latitude, longitude)
#             county = location.split(',')[-4]
#         except:
#             print('Error'+' ' + latitude + ' ' + longitude + ' ' + str(location))
#             county = "County"
#
#         newrow = newrow + ',' + county + '\n'
#         wr.write(newrow)
#
#     a = 20
# wr.close()