"""
GENERAL INSTRUCTIONS
1. WHICH PART TO CHANGE?: Uncomment every line with  [YOUR CODE HERE] and replace it with your code.
Please don't change anything else other than these lines. In other parts, the code isnt commented,
so just replace the parts with YOUR CODE or YOUR CODE HEREE

2. USE OF JUPYTER NOTEBOOK: For those who would like to use Jupyter Notebook. You can copy and paste
each function in the notebook environment, test your code their. However,
remember to paste back your code in a .py file and ensure that its running
okay.

3. INDENTATION: Please make sure that you check your indentation

4. Returning things from function: Please dont forget to use the return statement
to return a value when applicable

5. HINTS: please read my comments for hints and instructions where applicable

6. DEFINING YOUR OWN FUNCTIONS: where I ask you to define your own function
please make sure that you name the function exactly as I said.

7. Please work on the following functions in the order provided:
    - add_date_and_filter
    - get_events_before_jul13
    - preprocess_cdrs_using_spark
    - explore_data_with_spark
    - generate_user_attributes_with_pandas
8. Dataset for this work: all the questions are based on the simulated_cdrs dataset,
please download it and have it on your machine
"""
import random
import seaborn as sns
import numpy as np
from collections import namedtuple
from datetime import datetime
from pathlib import Path

def add_date_and_filter(csv_file, date_format, ref_date):
    """
    Create a dataframe, add date and filter out events based on ref date
    :return:
    """
    df = YOUR CODE
    str_time_col = 'cdr datetime'

    # convert date string to Python datetime
    # please extract only the Year, month and day from the date string using
    # indexing, please use solutions from first assignment to achieve this
    df['cdr_date'] = YOUR CODE

    # please retrieve all events older than or equal to the reference date
    # please use the query function for pandas DataFrame like below
    df.query('cdr_date <= @ref_date', inplace=True)

    if df.shape[0]:
        return df
    else:
        return 'NA'


def get_events_before_jul13(csv_folder, ref_date, date_format, num_of_csv_files=5000):
    """
    In this function, we will use the map function to run the
    add_date_and_filter above on a list of CSV_files
    :param csv_folder:
    :return:
    """
    list_csv = [f for f in csv_folder.iterdir() if f.suffix == '.csv']
    # use the function random.choices() to select only a sample of the CSVs  for fast processing
    # with k set to num_of_csv_files
    list_csv = YOUR CODE
    time_format = "%Y%m%d"

    # Use  the datetime.strptime() to convert the string ref_date to Python datetime object
    # This function requires two args, the other one is date format
    ref_date = datetime.strptime(ref_date,  date_format)

    # Run map with multiple iterables(r.g., two lists)
    # Note that input-1 is the list of CSV we generated above
    # Prepare input-2 and input-3 as below. Please use the  multiplication
    # operator on a single list item to repeat the same element multiple
    # times in a list as we did in the first assignment
    list_date_format = YOUR CODE
    list_date_ref_date = YOUR CODE

    # now run the map function with the three input lists we have defined
    results = YOUR CODE

    # write code to identify elements in the list which arent s 'NA'
    # You can use a list comprehension. For example, you use "isinstance()"
    # to check if the datatype is either string or DataFrame
    no_na_results = []
    print('I found {} non NA results'.format(len(no_na_results)))

    return no_na_results


def preprocess_cdrs_using_spark(file_or_folder=None, number_of_users_to_sample=None,
                                output_csv=None, date_format='%Y%m%d%H%M%S',
                                debug_mode=True, loc_file=None, save_to_csv=False):
    """
    In this function, we perfom some basic preprocessing such as below:
    1. rename columns
    2. change some data types
    3. Add location details
    Eventually, we will sample the data to use for our analysis
    :param data_folder:
    :param output_csv_for_sample_users:
    :return:
    """

    # # create a SparkSession object
    # spark = YOUR CODE
    #
    # # read data with spark
    # df = YOUR CODE
    #
    # # repartition to speed up
    # df = df.repartition(10)
    #
    # # if just testing/debugging, pick only a small dataset
    # # by using the sample function of spark
    # if debug_mode:
    #     dfs = YOUR CODE
    #     df = dfs
    #
    # # rename columns to remove space and name them using camel
    # # case  sytle like this: cdr datetime becomes cdrDatetime
    # # calling phonenumber becomes just phoneNumber
    # # if you are renaming more than one column, you can
    # # chain the commands and add round brackets like this:
    # df2 =    YOUR CODE
    #
    # # drop the 'cdr type' column
    # df3 = YOUR CODE
    #
    # # Use Spark UDF to add date and datetime
    # add_datetime = udf(lambda x: datetime.strptime(x, date_format), TimestampType())
    # add_date = udf(lambda x: datetime.strptime(x, date_format), DateType())
    #
    # # create timestamp
    # df4 = df3.withColumn('datetime', add_datetime(col('cdrDatetime')))
    # df5 = YOUR CODE
    #
    # # lets make sure we dont have any null phoneNumbers
    # # use spark filter() function to remove null phoneNumbers
    # df6 = YOUR CODE
    #
    # # Lets merge with location details using cellId from CDRs and also
    # # cellID on the other
    # # read pandas dataframe of location details
    # dfLoc = YOUR CODE
    #
    # # rename column 'cell_id' to 'cellId' in the pandas dataframe
    # YOUR CODE
    #
    # # create spark dataframe from the pandas dataframe
    # sdfLoc = YOUR CODE
    #
    # # join the cdrs dataframe with the location dataframe
    # df7 = YOUR CODE
    #
    # # create a list of unique user phoneNumbers
    # all_users = YOUR CODE
    #
    # # randomly select the required number of users
    # # using the random.choices() function
    # random_user_numbers = YOUR CODE
    #
    # # select only our random user data using spark filter
    # dfu = YOUR CODE
    #
    # # save to CSV if necessary
    # if save_to_csv:
    #     dfu.coalesce(1).write.csv(path=output_csv, header=True)
    # else:
    #     return dfu


def explore_data_with_spark(df=None, output_plot_file=None, output_heatmap=None):
    """
    Lets do a quick exploration of the data by generating the following:
    1. Number of days in the data
    2. User call count stats
    3. Weekday and hour calling patterns
    """
    # =====================================
    # CALCULATE THE NUMBER OF DAYS IN DATA
    # =====================================

    # # use relevant spark function to generate
    # # a list of unique dates, recall that the date
    # # column is 'date
    # dates_rows = YOUR CODE
    # # sort the dates using sorted() function
    # sorted_dates = YOUR CODE
    # # use list indexing to get the first element and last
    # # element from the sorted list, substract them to get
    # # time difference
    # diff = YOUR CODE
    # # use days function to get the number of days
    # num_days = YOUR CODE
    #
    # # =====================================
    # # GENERATE WEEKDAY AND HOUR CALL COUNT
    # # =====================================
    #
    # # define UDF to calculate hour and weekday
    # # for weekday use weekday() function while
    # # for hour, use hour
    # add_hr = YOUR CODE
    # add_wkday = YOUR CODE
    # # create a dictionary with keys as the weekday integers while the values
    # # are the weekday name
    # day_dict = YOUR CODE
    #
    # # add hour column, lets call it 'hr
    # # also add weekday column, we call it 'wkday'
    # dfHr = YOUR CODE
    # dfHr2 = YOUR CODE
    #
    # # use spark group by to counter number of calls by each
    # # hour of the day, the groupBy column will be 'wkday, hr'
    # dfWkDay = YOUR CODE
    #
    # # use the  add_weekdays  function to assign
    # dfWkDay['weekDay'] = dfWkDay.apply(add_weekdays, args=(day_dict,), axis=1)
    # dfWkDay.drop(labels=['wkday'], axis=1, inplace=True)
    # dfWkDayPivot = dfWkDay.pivot(index='weekDay', columns='hr', values='count')
    # d = dfWkDayPivot.reset_index()
    # ax = sns.heatmap(d)
    # ax.get_figure().savefig(output_heatmap)
    #
    # # =====================================
    # # NUMBER OF CALLS FOR EACH USER
    # # =====================================
    # # group user and count number of events
    # # convert resulting spark dataframe to pandas
    # dfGroup = YOUR CODE
    #
    # # create a distribution plot of user call count using
    # # seaborn
    # ax = YOUR CODE
    #
    # # save plot as png file
    # YOUR CODE
    #
    # # report average number calls per day for each user
    # # first use spark groupBy on phoneNumber and day, then
    # # convert that object to pandas dataframe using toPandas()
    # # function
    # dfGroupDay = YOUR CODE
    #
    # # get mean and median
    # mean = YOUR CODE
    # median = YOUR CODE
    #
    # # return results like this mean, median, number of days
    # YOUR CODE
    #


def generate_user_attributes_with_pandas(df_all_users=None, num_of_users=None, out_csv=None):
    """
    Loop through each user and generate their attributes
    :param df_all_users:
    :param num_of_users:
    :param out_csv:
    :return:
    """
    # # create a copy of df to avoid annoying errors
    # df = df_all_users.copy(deep=True)
    #
    # # declare an empty list
    # user_data = YOUR CODE
    #
    # # create a list of unique user IDs using the phoneNumber column
    # user_list = YOUR CODE
    #
    # for i, user in enumerate(user_list):
    #     # subset the data so that we have data for this user
    #     # only using phoneNumber
    #     udf = YOUR CODE
    #
    #     # use generate_trips_by_day function to
    #     # generate attributes for this user
    #     data_pt = YOUR CODE
    #
    #     # append the data_pt above to our user_data list
    #     YOUR CODE
    #
    #     # stop loop execution if we hit the required number of users
    #     if i == num_of_users:
    #         break
    #
    # # create dataframe and save it to CSV
    # YOUR CODE

if __name__ == '__main__':
    # ============================================
    # QUESTION 1: USING MAP FUNCTION
    # ============================================
    time_format = "%Y%m%d"
    reference_date = '20180713'
    num_csv_files = YOUR CODE   # start with a small number like  < 500 to make sure your code is running okay
    #  add full path to the CSV folder
    path_to_csv = YOUR CODE
    get_events_before_jul13(path_to_csv , reference_date, time_format, num_of_csv_files=)

    # ============================================
    # QUESTION 2: PREPROCESS SIMULATED CDRS
    # ============================================
    cdrs_dir = ADD PATH TO FOLDER WITH MULTIPLE CSV FILES
    # please start  with a few number of users, e.g., 1000
    number_of_users_to_sample = YOUR CODE
    # Full path to CSV to write outputs
    output_csv = YOUR CODE
    # Download the file called simulated_locs.csv
    # and add its full path below
    loc_file = YOUR CODE
    # finall call the function preprocess_cdrs_using_spark and with other options left
    # as default
    dfu = YOUR CODE

    # ============================================
    # QUESTION 3: EXPLORE USER ACTIVITY PATTERNS
    # ============================================
    # 1. use the df you save from above as input here
    # 2. create a full path to save a plot with "png" extension
    # 3. create a full path to save the heatmap plot
    # Run the function explore_data_with_spark below
    results =  YOUR CODE

    # ==================================================
    # QUESTION 4: GENERATE USER ATTRIBUTES WITH PANDAS
    # ==================================================
    # 1. use the df of users from the preprocess simulated cdrs function
    # 2. Decide how many users to process
    # 3. Create full path for output file
    # run the function generate_user_attributes_with_pandas
    YOUR CODE
