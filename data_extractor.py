import pandas as pd
import json
import os
import numpy as np
import io


from glob import glob

class DataExtractor(object):
    def __init__(self):
        a=1



    def convert_json_csv(self):


        for file in os.listdir("dataset"):
            if '.json' in file:
                with open(os.path.join("dataset", file), 'r', encoding="ANSI") as f:
                    data = f.readlines()

                # remove the trailing "\n" from each line
                data = map(lambda x: x.rstrip(), data)

                data_json_str = "[" + ','.join(data) + "]"

                # now, load it into pandas
                data_df = pd.read_json(data_json_str)
                data_df.to_csv(os.path.join("csv_files", file + '.csv'), index=False)




    def split_csv_into_multiple_csvs(self):
        data_df = pd.read_csv(os.path.join("csv_files", "review.csv"))
        data_df = data_df.sort_values(by=['business_id'])
        df_list = np.array_split(data_df, 60)
        for i in range(len(df_list)):
            df_list[i].to_csv(os.path.join("extracted_reviews_new", "review"+ str(i) +".csv"), index=False, encoding="ANSI")




if __name__ == "__main__":
    data_ext = DataExtractor()
    data_ext.convert_json_csv()
