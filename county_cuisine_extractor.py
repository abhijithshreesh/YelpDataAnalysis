import pandas as pd
import json
import os
import numpy as np
import ast
import operator
import json

class CountyWiseCuisineExtractor(object):
    def __init__(self):
        self.data_df_list = []
        for i in range(60):
            review_df = pd.read_csv(os.path.join("Combined_Data_With_Codes", "NewCountyData" + str(i) + ".csv"))
            self.data_df_list.append(review_df)
        self.total_data_df = pd.concat(self.data_df_list)
        self.total_data_df.to_csv("all_reviews_data_with_county.csv", index=False)
        self.state_list = ['AZ', 'IL', 'IN', 'NY', 'NC', 'NV', 'OH', 'PA', 'SC', 'WI']
        self.state_code_to_name_dict = {'AZ': 'Arizona', 'IL': 'Illinois', 'IN': 'Indiana', 'NC': 'NorthCarolina',
                                        'NY': 'NewYork', 'NV': 'Nevada', 'OH': 'Ohio', 'PA': 'Pennsylvania',
                                        'SC': 'SouthCarolina', 'WI': 'Wisconsin'}
        self.county_cuisine_dict = {}
        self.food_related_categories = \
            set(['American (New)', 'American (Traditional)', 'Asian Fusion', 'Chinese', 'French', 'Greek', 'Hawaiian','Indian', 'Italian', 'Japanese', 'Korean', 'Mediterranean', 'Mexican', 'Southern', 'Thai', 'Vietnamese', 'Arabian', 'Indonesian'])

    def split_data_county_wise(self):
        grouped_data = self.total_data_df.groupby("state")
        for key, data_df in grouped_data:
            data_df.to_csv(os.path.join("state_wise_data_with_county", "%s.csv"%key))

    def split_data_on_cuisine(self):
        for file in self.state_list:
            data_df = pd.read_csv(os.path.join("state_wise_data_with_county", file + ".csv"))
            data_df["County"] = pd.Series([each.split(", ")[1] for each in data_df.County], index=data_df.index)
            data_df['categories'] = pd.Series([ast.literal_eval(each) for each in data_df.categories],
                                              index=data_df.index)
            data_df["cuisine"] = pd.Series(
                [list((set(each).intersection(self.food_related_categories))) for each in data_df.categories],
                index=data_df.index)
            split_cuisine_series = data_df['cuisine'].apply(pd.Series, 1).stack()
            split_cuisine_series.index = split_cuisine_series.index.droplevel(-1)  # to line up with df's index
            split_cuisine_series.name = 'cuisine'  # needs a name to join
            del data_df['cuisine']
            data_df = data_df.join(split_cuisine_series)
            data_df.to_csv(os.path.join("state_wise_data_with_county", file + ".csv"), index=False)

    def get_location_wise_cuisine(self):
        for file in self.state_list:
            cuisine_dict = {}
            data_df = pd.read_csv(os.path.join("state_wise_data_with_county", file+".csv"))
            county_code_dict = {county: code for county, code in zip(data_df.County, data_df.CountyCode)}
            groupby_county = data_df.groupby('County')
            print(data_df.County.unique())
            for county, county_df in groupby_county:
                groupby_cuisine = county_df.groupby('cuisine')
                max_records = 0
                min_records = 1000000
                max_popularity_score = 0
                #for key, cuisine_df in groupby_cuisine:
                #    max_records = len(cuisine_df.index) if len(cuisine_df.index)>max_records else max_records
                #    min_records = len(cuisine_df.index) if len(cuisine_df.index)<min_records else min_records
                #    #if max_records != min_records:

                for key, cuisine_df in groupby_cuisine:
                        good_review_rows = cuisine_df[cuisine_df['sentiment_score'] == 'good']
                        cuisine_dict[key] = (float(len(good_review_rows.index))/float(len(cuisine_df.index))*6) + \
                                            (float(len(cuisine_df.index))/float(len(county_df.index))*4)
                        #*(float(len(cuisine_df.index)) - float(min_records))/ (float(max_records) - float(min_records))#float(len(good_review_rows.index))/float(len(cuisine_df.index))*
                        #max_popularity_score = cuisine_dict[key] if cuisine_dict[key]>max_popularity_score else max_popularity_score


                #for key, value in cuisine_dict.items():
                 #       cuisine_dict[key] = float(value/max_popularity_score)*100
                #else:
                 #       for key, cuisine_df in groupby_cuisine:
                #            cuisine_dict[key] = 101
                sorted_dict = sorted(cuisine_dict.items(), key=operator.itemgetter(1), reverse=True)
                self.county_cuisine_dict[county] = {'cuisine': sorted_dict[0:5], 'id': county_code_dict[county],
                                                          'state': file}
        return (self.county_cuisine_dict)

    def convert_to_json(self, county_cuisine_dict):
        json_data_list = []
        state_code_df = pd.DataFrame.from_csv("states.tsv", sep='\t')
        state_code_dict = {state: code for state, code in zip(state_code_df.index, state_code_df.id)}
        for key, value_dict in county_cuisine_dict.items():
            data_dict = {}
            #data_dict['id'] = value_dict['id']
            data_dict['county'] = key
            data_dict['state_id'] = state_code_dict[self.state_code_to_name_dict[value_dict['state']]]
            data_dict['state'] = value_dict['state']
            data_dict['top_cuisine'] = value_dict['cuisine'][0][0]
            top_cuisine_list = []
            for each in value_dict['cuisine']:
                temp_dict = {}
                temp_dict['name'] = each[0]
                temp_dict['pos'] = each[1]
                top_cuisine_list.append(temp_dict)
            data_dict['top_5_cuisines'] = top_cuisine_list
            json_data_list.append({value_dict['id']: data_dict})
        with open('county_cuisine_data_new.json', 'w') as f:
            json.dump(json_data_list, f)





if __name__ == "__main__":
    obj = CountyWiseCuisineExtractor()
    county_cuisine_dict = obj.get_location_wise_cuisine()
    obj.convert_to_json(county_cuisine_dict)

