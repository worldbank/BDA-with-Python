import pandas as pd
from datetime import datetime
from multiprocessing import Pool
import seaborn as sns
from matplotlib import pyplot as plt
from pathlib import Path

# ==================================
# INSTRUCTIONS
# ==================================
# 1. Replace all places where there is 'YOUR CODE HERE" with your own code
# 2. You can test and check the code in Jupyter Notebook but the submission should be a Python file
# 3. Please name your Python script like this: FirstName_MiddleName_LastName

def get_date_range_by_chunking(large_csv):
    """
    In this function, the idea is to use pandas chunk feature.
    :param large_csv: Full path to activity_log_raw.csv
    :return:
    """
    # ======================================
    # EXPLORE THE DATA
    # ======================================
    # Read the first 100,000 rows in the dataset
    df_first_100k = YOUR CODE HERE
    print(df_first_100k.head())
    # Identify the time column in the dataset
    str_time_col = YOUR CODE HERE

    # ============================================================
    # FIND THE FIRST [EARLIEST] AND LAST DATE IN THE WHOLE DATASET
    # BY USING CHUNKING
    # =============================================================
    # set chunk size to some number. You can play around with the chunk size to see its effect
    chunksize = YOUR CODE HERE

    # declare a list to hold the dates
    dates = YOUR CODE HERE
    with pd.read_csv(YOUR CODE HERE, chunksize=chunksize) as reader:
        for chunk in reader:
            # convert the string to Python datetime object
            # add a new column to hold this datetime object. For instance,
            # you can call it "activ_time"
            time_col = YOUR CODE HERE
            chunk[time_col] = chunk[str_time_col].apply(lambda x: pd.to_datetime(x[:9]))
            chunk.sort_values(by=time_col, inplace=True)
            top_date = chunk.iloc[0][time_col]
            # Add the top_date to the list
            YOUR CODE HERE
            chunk.sort_values(by=time_col, ascending=False, inplace=True)
            bottom_date = chunk.iloc[0][time_col]
            # Add the bottom_date to the list
            YOUR CODE HERE


    # Find the earliest and last date by sorting the dates list we created above
    sorted_dates = YOUR CODE HERE
    first = YOUR CODE HERE
    last = YOUR CODE HERE
    print("First date is {} and the last date is {}".format(first, last))

    return first, last


def quadratic_func(x, a):
    """
    Define the quadratic function like this: y = 2x^2 + a -1
    (read as y is equal to 2 x squared plus a minus 1)
    :param x:
    :return:
    """
    y = YOUR CODE HERE
    return y


def run_the_quad_func_without_multiprocessing(list_x, list_y):
    """
    Run the quadratic function on a huge list of X and Ys without using parallelism
    :param list_x: List of xs
    :param list_y: List of ys
    :return:
    """
    results = [quadratic_func(x, y) for x, y in zip(list_x, list_y)]
    return results


def run_the_quad_func_with_multiprocessing(list_x, list_y, num_processors):
    """
    Run the quadratic function with multiprocessing
    :param list_x: List of xs
    :param list_y: List of xs
    :param num_processors: Number of processors to use
    :return:
    """
    # Use the Pool method to initiate processors with number
    # of processors set to num_processors
    processors = YOUR CODE HERE
    params = [i for i in zip(list_x, list_y)]

    # Use the starmap method for Pool to run the processes
    # Like this: Pool.starmap(func_name, params)
    results = YOUR CODE HERE
    processors.close()
    return results


def multiprocessing_vs_sequential_quadratic(list_len, out_plot, out_csv):
    """
    Compare how
    :param list_len:
    :return:
    """

    data = []
    for i in range(1, list_len):
        list_length = 10 ** i

        # Create lists using the range() function
        # and provided list length param
        x = YOUR CODE HERE
        y = YOUR CODE HERE

        start_time = datetime.now()
        # Call the function run_the_quad_func_without_multiprocessing below
        YOUR CODE HERE
        end_time = datetime.now()
        time_taken_seq = (end_time - start_time).total_seconds()
        data.append({'ListLen': list_length, 'Type' : 'Parallel', 'TimeTaken': time_taken_seq})

        start_time = datetime.now()
        # Call the function run_the_quad_func_with_multiprocessing below
        YOUR CODE HERE
        end_time = datetime.now()
        time_taken_mult = (end_time - start_time).total_seconds()
        data.append({'ListLen': list_length, 'Type' : 'Sequential', 'TimeTaken': time_taken_mult})

    # Create a data frame using the data variable defined above
    df = YOUR CODE HERE
    plt.figure(figsize=(12, 8))
    sns.lineplot(data=df, x='ListLen', y='TimeTaken', hue='Type')

    # Use plt.savefig() to save the plot
    YOUR CODE HERE

    # Also save the pandas dataframe defined above
    YOUR CODE HERE


def get_num_uniq_users(csv_file, userid_col):
    """
    A Helper function to help get the number of unique users
    :param csv_file: path to CSV file
    :param userid_col: Column for user ID
    :return:
    """
    # Read the CSV file using pandas
    df = YOUR CODE HERE

    # Use the nunique() method to get number of unique users
    num = YOUR CODE HERE

    return num


def get_tot_uniq_users_parallel(path_to_csv, num_processors):
    """

    :param path_to_csv:
    :return:
    """
    # ==================================================
    # GET LIST OF ALL CSV FILES AND PUT IN A LIST
    # ===================================================
    # convert the string URL for path to a Path object for easier interaction
    start = datetime.now()
    path_to_csv = Path(path_to_csv)
    list_csv = [f for f in path_to_csv.iterdir() if f.suffix == '.csv']


    # ======================================================
    # USE MULTIPROCESSING TO GET UNIQUE USERS FROM ALL CSV'S
    # ======================================================
    # Create processors using Pool and num_processors
    processors = YOUR CODE HERE

    # Prepare parameters for the get_num_uniq_users() function
    user_id_col = ['user_id']*len(list_csv)     # List containing parameters for the user_id column
    params = [i for i in zip(list_csv, user_id_col)]  # combine the two lists
    # Run the function in parallel
    results = YOUR CODE HERE
    processors.close()

    # combine results to get the total
    tot_users = YOUR CODE HERE
    end = datetime.now()
    time_taken = round((end - start).total_seconds(), 2)
    print('Total unique users: {:,} in {} seconds'.format(tot_users, time_taken))

    return tot_users


def get_tot_uniq_users_seq(path_to_csv, userid_col):
    """

    :param path_to_csv:
    :return:
    """
    # ==================================================
    # GET LIST OF ALL CSV FILES AND PUT IN A LIST
    # ===================================================
    # convert the string URL for path to a Path object for easier interaction
    start = datetime.now()
    path_to_csv = Path(path_to_csv)
    list_csv = [f for f in path_to_csv.iterdir() if f.suffix == '.csv']

    tot_users = 0
    for csv in list_csv:
        # Read CSV into pandas dataframe
        df = YOUR CODE HERE
        # Get unique number of users using nunique() and the column for user_id
        uniq = YOUR CODE HERE

        # Increment the total number of users
        tot_users += YOUR CODE HERE

    end = datetime.now()
    time_taken = round((end - start).total_seconds(), 2)
    print('Total unique users: {:,} in {} seconds'.format(tot_users, time_taken))

    return tot_users


if __name__ == '__main__':
    # Question-1: Pandas chunks
    file = ADD PATH TO THIS FILE->"activity_log_raw.csv"
    # get_date_range_by_chunking(file)

    # Question-2: CPU bound parallelization
    out_plot = ADD PATH TO WHERE PLOT WILL BE SAVED WITH PNG EXTENSION
    out_csv = ADD PATH TO WHERE CSV WILL BE SAVED WITH csv EXTENSION
    # multiprocessing_vs_sequential_quadratic(9, out_plot, out_plot)

    # =====================================================
    # QUESTION-3: CPU BOUND PARALLELIZATION WITH PANDAS
    # =====================================================
    fpath = PATH WHERE YOU HAVE FOLDER WITH MANY SMALL CSV FILES
    # get_tot_uniq_users_parallel(fpath, 12)
    # get_tot_uniq_users_seq(fpath, 'user_id')