import pandas as pd
import json
import os
import numpy as np
import io
import ast
from textblob import TextBlob
from geopy.geocoders import Nominatim
geolocator = Nominatim()

class AnalyzeReviewData(object):
    def __init__(self):
        self.business_df = pd.read_csv("business_ids.txt", header=None)
        self.business_df.columns = ["business_id"]
        self.business_id_list = self.business_df.business_id.tolist()
        self.selected_business_df_list = []
        self.total_business_df = None


    def assign_sentiment_score(self, text):
        blob = TextBlob(text)
        return "good" if blob.sentiment.polarity>0 else "bad"


    def extract_sentiment_from_reviews(self):
        for i in range(60):
            review_df = pd.read_csv(os.path.join("extracted_reviews_new", "review" + str(i) + ".csv"))
            review_df['sentiment_score'] = pd.Series([self.assign_sentiment_score(each) for each in review_df.text], index = review_df.index)
            review_df[["business_id", "text", "stars", "sentiment_score"]].to_csv(os.path.join("analyzed_reviews", "review" + str(i) + ".csv"), index=False)

    def extract_selected_busniess_info(self):
        for i in range(30):
            business_df = pd.read_csv(os.path.join("business", "business" + str(i) + ".csv"))
            #business_df = business_df[business_df.business_id.isin(self.business_id_list)]
            self.selected_business_df_list.append(business_df)
        self.total_business_df = pd.concat(self.selected_business_df_list)[["categories", "state", "latitude", "longitude", "business_id", "stars"]]
        self.total_business_df.rename(columns={"stars": "rating"})
        self.total_business_df.to_csv("selected_business_info.csv")

    def combine_business_and_review_data(self):
        for i in range(60):
            review_df = pd.read_csv(os.path.join("review", "review" + str(i) + ".csv"))
            combined_df = pd.merge(review_df, self.total_business_df, on="business_id", how="inner")
            combined_df.to_csv(
                os.path.join("review_data", "data" + str(i) + ".csv"), index=False)



if __name__ == "__main__":
    obj = AnalyzeReviewData()
    obj.extract_selected_busniess_info()
    obj.combine_business_and_review_data()