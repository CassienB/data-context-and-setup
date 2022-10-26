import os
import pandas as pd

# csv_path = os.path.dirname(__file__)
class Olist:
    def get_data(self):

        dir = os.path.dirname(os.path.dirname(os.path.dirname("__file__")))

        csv_path = os.path.join(dir, "..", "data-context-and-setup","data", "csv")


        file_names = os.listdir(csv_path)

        key_names = [file_names[i].replace('_dataset.csv','').replace('olist_','').replace(".csv",'') for i in range(len(file_names))]

        dico=dict(zip(key_names, [pd.read_csv(os.path.join(csv_path, name)) for name in file_names]))


        return dico

        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities


    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
