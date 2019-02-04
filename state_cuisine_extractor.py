import pandas as pd
import json
import os
import numpy as np
import ast
import operator
import json

class StateWiseCuisineExtractor(object):
    def __init__(self):
        self.data_df_list = []
        #for i in range(60):
        #    review_df = pd.read_csv(os.path.join("review_data", "data" + str(i) + ".csv"))
        #    self.data_df_list.append(review_df)
        #self.total_data_df = pd.concat(self.data_df_list)
        #self.total_data_df.to_csv("all_reviews_data.csv", index=False)
        self.state_list = ['AZ', 'IL', 'IN', 'NY', 'NC','NV', 'OH', 'PA', 'SC', 'WI']
        self.state_code_to_name_dict = {'AZ': 'Arizona', 'IL': 'Illinois', 'IN': 'Indiana', 'NC': 'NorthCarolina', 'NY': 'NewYork', 'NV': 'Nevada', 'OH': 'Ohio', 'PA': 'Pennsylvania', 'SC': 'SouthCarolina' , 'WI': 'Wisconsin'}
        self.state_cuisine_dict = {}
        self.food_related_categories = set(['American (New)',  'American (Traditional)', 'Asian Fusion', 'Chinese', 'French', 'Greek' , 'Hawaiian', 'Indian' , 'Italian', 'Japanese', 'Korean', 'Mediterranean', 'Mexican' ,'Southern' ,'Thai' ,'Vietnamese', 'Arabian', 'Indonesian'])

    def split_data_state_wise(self):
        grouped_data = self.total_data_df.groupby("state")
        for key, data_df in grouped_data:
            data_df.to_csv(os.path.join("state_wise_data", "%s.csv"%key))

    def split_review_data_state_wise(self):
        grouped_data = self.total_data_df.groupby("state")
        for key, data_df in grouped_data:
            data_df.to_csv(os.path.join("state_wise_review_data", "%s.csv" % key))

    def split_data_on_cuisine(self):
        for file in self.state_list:
            data_df = pd.read_csv(os.path.join("state_wise_data", file+".csv"))
            data_df['categories'] = pd.Series([ast.literal_eval(each) for each in data_df.categories],
                                                  index=data_df.index)
            data_df["cuisine"] = pd.Series([list((set(each).intersection(self.food_related_categories))) for each in data_df.categories], index=data_df.index)
            split_cuisine_series = data_df['cuisine'].apply(pd.Series, 1).stack()
            split_cuisine_series.index = split_cuisine_series.index.droplevel(-1)  # to line up with df's index
            split_cuisine_series.name = 'cuisine'  # needs a name to join
            del data_df['cuisine']
            data_df = data_df.join(split_cuisine_series)
            data_df.to_csv(os.path.join("state_wise_data", file+".csv"), index=False)

    def get_location_wise_cuisine(self):
        for file in self.state_list:
            cuisine_dict = {}
            review_count_dict = {}
            data_df = pd.read_csv(os.path.join("state_wise_data_with_county", file+".csv"))
            max_records = 1
            min_records = 1000000
            max_popularity_score = 0
            top_hotels_dict = {}
            groupby_cuisine = data_df.groupby('cuisine')
            for key, cuisine_df in groupby_cuisine:
                max_records = len(cuisine_df.index) if len(cuisine_df.index)>max_records else max_records
                min_records = len(cuisine_df.index) if len(cuisine_df.index)<min_records else min_records
           # if max_records != min_records:
            for key, cuisine_df in groupby_cuisine:
                    hotel_dict = {}
                    hotel_review_count = {}
                    good_review_rows = cuisine_df[cuisine_df['sentiment_score'] == 'good']
                    cuisine_dict[key] = (float(len(good_review_rows.index))/float(len(cuisine_df.index))*8) + \
                                            (float(len(cuisine_df.index))/float(len(data_df.index))*2)#*(float(len(cuisine_df.index)) - float(min_records))/ (float(max_records) - float(min_records))#float(len(good_review_rows.index))/float(len(cuisine_df.index))*
                    review_count_dict[key] = len(cuisine_df)
                    max_popularity_score = cuisine_dict[key] if cuisine_dict[key] > max_popularity_score else max_popularity_score
                    groupby_hotels = cuisine_df.groupby('business_id')
                    for hotel, hotel_df in groupby_hotels:
                        good_hotel_reviews = hotel_df[hotel_df['sentiment_score'] == 'good']
                        hotel_dict[hotel] = (float(len(good_hotel_reviews.index))/float(len(hotel_df.index))*8) + \
                                            (float(len(hotel_df.index))/float(len(cuisine_df.index))*2)
                        hotel_review_count[hotel] = len(hotel_df.index)
                    hotel_tuples = sorted(hotel_dict.items(), key=operator.itemgetter(1), reverse=True)
                    hotel_tuples = [each[0] for each in hotel_tuples[0:5]]
                    #for each in hotel_tuples:
                    #    each.append(hotel_review_count[each[0]])
                    top_hotels_dict[key] = hotel_tuples

           # for key, value in cuisine_dict.items():
            #        cuisine_dict[key] = float(value/max_popularity_score)*100
            #else:
            #    for key, cuisine_df in groupby_cuisine:
            #        cuisine_dict[key] = 101


            sorted_tuples = sorted(cuisine_dict.items(), key=operator.itemgetter(1), reverse=True)
            sorted_tuples = [list(each) for each in sorted_tuples]
            for each in sorted_tuples:
                each.append(review_count_dict[each[0]])
                each.append(top_hotels_dict[each[0]])
            self.state_cuisine_dict[file] = sorted_tuples[0:5] if len(sorted_tuples) >= 5 else sorted_tuples
        return self.state_cuisine_dict

    def convert_to_json(self, state_cuisine_dict):
        json_data_list = []
        state_code_df = pd.DataFrame.from_csv("states.tsv", sep='\t')
        state_code_dict = {state: code for state, code in zip(state_code_df.index, state_code_df.id)}
        for key, value in state_cuisine_dict.items():
            data_dict = {}
            data_dict['id'] = state_code_dict[self.state_code_to_name_dict[key]]
            data_dict['name'] = self.state_code_to_name_dict[key]
            data_dict['type'] = value
            json_data_list.append(data_dict)
        with open('state_cuisine_data_new.json', 'w') as f:
            json.dump(json_data_list, f)

    def convert_state_info_to_json(self, state_cuisine_dict):
        json_data_list = []
        state_code_df = pd.DataFrame.from_csv("states.tsv", sep='\t')
        state_code_dict = {state: code for state, code in zip(state_code_df.index, state_code_df.id)}
        for key, values in state_cuisine_dict.items():
            data_dict = {}
            # data_dict['id'] = value_dict['id']
            data_dict['id'] = state_code_dict[self.state_code_to_name_dict[key]]
            data_dict['name'] = self.state_code_to_name_dict[key]
            data_dict['top_cuisine'] = values[0][0]
            top_cuisine_list = []
            for each in values:
                temp_dict = {}
                temp_dict['name'] = each[0]
                temp_dict['pos'] = each[1]
                temp_dict['count'] = each[2]
                temp_dict['top_5_restaurants'] = each[3]
                top_cuisine_list.append(temp_dict)
            data_dict['top_5_cuisines'] = top_cuisine_list
            json_data_list.append(data_dict)
        with open('state_data_new.json', 'w') as f:
            json.dump(json_data_list, f)

if __name__ == "__main__":
    obj = StateWiseCuisineExtractor()
    cuisine_dict = obj.get_location_wise_cuisine()
    obj.convert_state_info_to_json(cuisine_dict)