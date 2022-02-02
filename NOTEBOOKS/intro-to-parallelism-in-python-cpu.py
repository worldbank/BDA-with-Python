
from datetime import datetime
from multiprocessing import Pool
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

def square(num):
    return num**2


def square_add(num1, num2):
    return num1**2 + num2**2


def run_square_without_multiprocessing(input_list):
    # List comprehension 
    result = [square(x) for x in input_list]
    
    return result


def run_square_with_multiprocessing(input_list, num_processors):
    # First, create a list of processors using Pool method 
    # From multiprocessing package
    processors = Pool(num_processors)
    
    # Use the map method to run the square function
    results = processors.map(square,input_list)
    
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
        x = [i for i in range(list_len)]
       
        start_time = datetime.now()
        run_square_without_multiprocessing(x)
        end_time = datetime.now()
        time_taken_seq = (end_time - start_time).total_seconds()
        data.append({'ListLen': list_length, 'Type' : 'Parallel', 'TimeTaken': time_taken_seq})

        start_time = datetime.now()
        run_square_with_multiprocessing(x, 12)
        end_time = datetime.now()
        time_taken_mult = (end_time - start_time).total_seconds()
        data.append({'ListLen': list_length, 'Type': 'Sequential', 'TimeTaken': time_taken_mult})

    df = pd.DataFrame(data)
    plt.figure(figsize=(12, 8))
    sns.lineplot(data=df, x='ListLen', y='TimeTaken', hue='Type')
    plt.savefig(out_plot)
    df.to_csv(out_csv, index=False)


if __name__ == '__main__':
    # ==============================
    # COMPARE MULT-VS-SEQ
    # ==============================
    multiprocessing_vs_sequential_quadratic(9, 'tmp.png', 'tmp.csv')