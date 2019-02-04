import pandas as pd
import json
import os
import numpy as np
import io
import ast
import textblob
from geopy.geocoders import Nominatim
import geocoder
geolocator = Nominatim()

class ExtractBusinessData(object):
    def __init__(self):
        self.us_state_codes_list = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
        self.food_related_categories = set(['American (New)',  'American (Traditional)', 'Asian Fusion', 'Chinese', 'French', 'Greek' , 'Hawaiian', 'Indian' , 'Italian', 'Japanese', 'Korean', 'Mediterranean', 'Mexican' ,'Southern' ,'Thai' ,'Vietnamese', 'Arabian', 'Indonesian'])
        self.business_id_list = []
        self.geolocator = Nominatim()
        self.business_df_list = []
        self.timings_dict = {
            '0:00': 'Night Life',
            '1:00': 'Night Life',
            '2:00': 'Night Life',
            '3:00': 'Night Life',
            '4:00': 'Breakfast',
            '5:00': 'Breakfast',
            '6:00': 'Breakfast',
            '7:00': 'Breakfast',
            '8:00': 'Breakfast',
            '9:00': 'Breakfast',
            '10:00': 'Breakfast',
            '11:00': 'Breakfast',
            '12:00': 'Lunch',
            '13:00': 'Lunch',
            '14:00': 'Lunch',
            '15:00': 'Lunch',
            '16:00': 'Lunch',
            '17:00': 'Lunch',
            '18:00': 'Dinner',
            '19:00': 'Dinner',
            '20:00': 'Dinner',
            '21:00': 'Dinner',
            '22:00': 'Night Life',
            '23:00': 'Night Life'
        }



    def extract_us_data(self):
        for i in range(30):
            business_df = pd.read_csv(os.path.join("business", "business" + str(i) + ".csv"))
            business_df = business_df[business_df.state.isin(self.us_state_codes_list)]
            business_df.to_csv(os.path.join("business", "business" + str(i) + ".csv"), index=False)
            #business_df['categories'] = pd.Series([ast.literal_eval(each) for each in business_df.categories], index=business_df.index)
            #for each in business_df.categories:
            #    self.food_related_categories.update(each)
        #print(self.food_related_categories)

    def extract_food_data(self):
        for i in range(30):
            business_df = pd.read_csv(os.path.join("business", "business" + str(i) + ".csv"))
            business_df['categories'] = pd.Series([ast.literal_eval(each) for each in business_df.categories],
                                                  index=business_df.index)
            business_df['isCafe'] = pd.Series([1 if set(each).intersection(self.food_related_categories) else 0 for each in business_df.categories], index=business_df.index)
            business_df = business_df[business_df['isCafe']==1]
            business_df.to_csv(os.path.join("business", "business" + str(i) + ".csv"), index=False)
            self.business_id_list.extend(business_df.business_id.unique().tolist())
            self.business_df_list.append(business_df)
            with open('business_ids.txt', 'w') as f:
                f.write('\n'.join(self.business_id_list))
        final_business_df = pd.concat(self.business_df_list)
        final_business_df.to_csv("extracted_business.csv", index=False)

    def extract_food_reviews(self):
        for i in range(60):
            review_df = pd.read_csv(os.path.join("review", "review" + str(i) + ".csv"))
            review_df = review_df[review_df['business_id'].isin(self.business_id_list)]
            review_df.to_csv(os.path.join("extracted_reviews_new", "review" + str(i) + ".csv"), index=False)

    def extract_county_from_coordinates(self, coordinates_string):
       location = self.geolocator.reverse(coordinates_string)
       return location.address.split(',')[3].strip()

    def add_county_info_to_busniess(self):
        for i in range(30):
            business_df = pd.read_csv(os.path.join("business", "business" + str(i) + ".csv"))
            business_df['county'] = pd.Series([self.extract_county_from_coordinates('%s, %s' %(str(lat), str(long)))
                                               for lat, long in zip(business_df.latitude, business_df.longitude)],
                                              index=business_df.index)
            business_df.to_csv(os.path.join("business", "business" + str(i) + ".csv"), index=False)


    def extract_checkin_info(self):
        business_list_df = pd.read_csv("business_ids.txt", header=None)
        checkin_df = pd.read_csv(os.path.join("csv_files", "checkin.csv"))
        checkin_df = checkin_df[checkin_df.business_id.isin(business_list_df[0].tolist())]
        checkin_df.to_csv(os.path.join("csv_files", "updated_checkin.csv"), index=False)


    def combine_checkin_business_info(self):
        checkin_df = pd.read_csv(os.path.join("csv_files", "updated_checkin.csv"))
        business_df = pd.read_csv("extracted_business.csv")
        combined_df = pd.merge(business_df, checkin_df, on="business_id", how="left")
        combined_df.to_csv("business_checkin.csv", index=False)

    def business_checkin_json(self):
        biz_check_df = pd.read_csv("business_checkin.csv")
        business_dict = {biz: [name, checkins, attr, timings] for biz, name, checkins, attr, timings in zip(biz_check_df.business_id, biz_check_df.name, biz_check_df.time, biz_check_df.attributes, biz_check_df.hours)}
        json_list = []
        for key, value_list in business_dict.items():
            data_dict = {}
            #data_dict['id'] = key
            data_dict['name'] = value_list[0]
            data_dict['checkin'] = value_list[1]
            attr_dict = ast.literal_eval(value_list[2])
            data_dict['open_hours'] = value_list[3]
            data_dict['attributes'] = {}
            if 'Alcohol' in attr_dict.keys():
                data_dict['attributes'].update({'Alcohol': attr_dict['Alcohol']})
            else:
                data_dict['attributes'].update({'Alcohol': ''})
            if 'Music' in attr_dict.keys():
                music_list = []
                for m1, v1 in attr_dict.items():
                    if v1:
                        music_list.append(m1)
                data_dict['attributes'].update({'Music': music_list})
            else:
                data_dict['attributes'].update({'Music': ''})
            if 'GoodForMeal' in attr_dict.keys():
                meal_list = []
                for m1, v1 in attr_dict.items():
                    if v1:
                        meal_list.append(m1)
                data_dict['attributes'].update({'GoodForMeal': meal_list})
            else:
                data_dict['attributes'].update({'GoodForMeal': ''})

            # for attr, attrs in attr_dict:
            #     if attrs is dict:
            #         temp_list =[]
            #         for k1, v1 in attrs:
            #             if v1 == True:
            #                 temp_list.append(k1)
            #         attrs = temp_list

            json_list.append({key:data_dict})
        with open('business_attribute.json', 'w') as f:
            json.dump(json_list, f)

    def categorize_checkins(self, checkins_dict):
        day_wise_checkin_dict = {
                                 'Monday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 'Tuesday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 'Wednesday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 'Thursday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 'Friday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 'Saturday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 'Sunday': {'Breakfast': 0, 'Lunch': 0, 'Dinner': 0, 'Night Life': 0},
                                 }
        if checkins_dict:
            for key, value_dict in checkins_dict.items():
                if value_dict:
                    for time, checkins in value_dict.items():
                        day_wise_checkin_dict[key][self.timings_dict[time]] += int(checkins)

        for key, value_dict in day_wise_checkin_dict.items():
            value_sum = sum(value_dict.values())
            if value_sum:
                day_wise_checkin_dict[key] = {meal: int(value/value_sum*100)  for meal, value in value_dict.items()}
        return day_wise_checkin_dict

    def business_checkin_json_new(self):
        json_list = []
        biz_check_df = pd.read_csv("business_checkin.csv")
        business_list_df = pd.read_csv("business_ids.txt", header=None)
        biz_check_df = biz_check_df[biz_check_df.business_id.isin(business_list_df[0].tolist())]
        biz_check_df = biz_check_df.fillna('')
        biz_check_df = biz_check_df[biz_check_df['time']!='']
        business_dict = {biz: [name, checkins, timings, attrs] for biz, name, checkins, timings, attrs in
                         zip(biz_check_df.business_id, biz_check_df.name, biz_check_df.time, biz_check_df.hours, biz_check_df.attributes)}
        checkin_dict = {biz: ast.literal_eval(checkins) for biz, checkins in
                         zip(biz_check_df.business_id, biz_check_df.time)}

        data_attrs_dict = {biz: ast.literal_eval(attrs) for biz, attrs in
                         zip(biz_check_df.business_id, biz_check_df.attributes)}
        all_attr_dict = {}
        for biz, attr_dict in data_attrs_dict.items():
            data_dict = {'attributes': {}}
            if 'Alcohol' in attr_dict.keys():
                data_dict['attributes'].update({'Alcohol': attr_dict['Alcohol']})
            else:
                data_dict['attributes'].update({'Alcohol': ''})
            if 'WheelchairAccessible' in attr_dict.keys():
                data_dict['attributes'].update({'WheelchairAccessible': attr_dict['WheelchairAccessible']})
            else:
                data_dict['attributes'].update({'WheelchairAccessible': ''})
            if 'Music' in attr_dict.keys():
                music_list = []
                for m1, v1 in attr_dict['Music'].items():
                    if v1:
                        music_list.append(m1)
                data_dict['attributes'].update({'Music': music_list})
            else:
                data_dict['attributes'].update({'Music': ''})
            if 'GoodForMeal' in attr_dict.keys():
                meal_list = []
                for m1, v1 in attr_dict['GoodForMeal'].items():
                    if v1:
                        meal_list.append(m1)
                data_dict['attributes'].update({'GoodForMeal': meal_list})
            else:
                data_dict['attributes'].update({'GoodForMeal': ''})
            if 'Ambience' in attr_dict.keys():
                meal_list = []
                for m1, v1 in attr_dict['Ambience'].items():
                    if v1:
                        meal_list.append(m1)
                data_dict['attributes'].update({'Ambience': meal_list})
            else:
                data_dict['attributes'].update({'Ambience': ''})

            if 'BusinessParking' in attr_dict.keys():
                park_list = []
                for m1, v1 in attr_dict['BusinessParking'].items():
                    if v1:
                        park_list.append(m1)
                data_dict['attributes'].update({'BusinessParking': park_list})
            else:
                data_dict['attributes'].update({'BusinessParking': ''})
            all_attr_dict.update({biz: data_dict})

        checkin_dict = {biz: [business_dict[biz][0], self.categorize_checkins(checkins), all_attr_dict[biz]] for biz, checkins in
                        checkin_dict.items()}

        for key, value_list in checkin_dict.items():
            data_dict = {}
            # data_dict['id'] = key
            data_dict['name'] = value_list[0].replace("'", "")
            data_dict['checkins'] = value_list[1]
            data_dict['attributes'] = value_list
            json_list.append({key: data_dict})
        with open('categorized_checkins_new.json', 'w') as f:
            json.dump(json_list, f)


if __name__ == "__main__":
    obj = ExtractBusinessData()
    #obj.extract_us_data()
    #obj.extract_checkin_info()
    #obj.extract_food_data()
    #obj.add_county_info_to_busniess()
    obj.business_checkin_json_new()