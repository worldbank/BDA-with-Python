
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
    results = [square(x) for x in input_list]
    return results


def run_square_with_multiprocessing(input_list, num_processors):
    # First, create a list of processors using Pool method 
    # From multiprocessing package
    processors = Pool(num_processors)
    
    # Use the map method to run the square function
    results = processors.map(square, input_list)
    
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
        x = [i for i in range(list_length)]
       
        start_time = datetime.now()
        results = list(run_square_without_multiprocessing(x))
        print(len(results))
        end_time = datetime.now()
        time_taken_seq = (end_time - start_time).total_seconds()
        print('Time taken: {}'.format(time_taken_seq))
        data.append({'ListLen': list_length, 'Type' : 'Sequential', 'TimeTaken': time_taken_seq})

        start_time = datetime.now()
        results = list(run_square_with_multiprocessing(x, 12))
        print(len(results))
        end_time = datetime.now()
        time_taken_mult = (end_time - start_time).total_seconds()
        print('Time taken: {}'.format( time_taken_mult))
        data.append({'ListLen': list_length, 'Type': 'Parallel', 'TimeTaken': time_taken_mult})

    df = pd.DataFrame(data)
    plt.figure(figsize=(12, 8))
    ax = sns.lineplot(data=df, x='ListLen', y='TimeTaken', hue='Type')
    ax.set(xlabel='List Length', ylabel='Time Taken (sec)')
    plt.title("Sequential vs. Parallel [12 cores]")
    plt.savefig(out_plot)
    df.to_csv(out_csv, index=False)


if __name__ == '__main__':
    # ==============================
    # COMPARE MULT-VS-SEQ
    # ==============================
    multiprocessing_vs_sequential_quadratic(9, 'tmp.png', 'tmp.csv')