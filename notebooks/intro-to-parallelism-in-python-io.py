import asyncio
import time
import aiohttp
from multiprocessing import Pool
import time
import plotly.express as px
import plotly
import pandas as pd
import requests


def download_site_seq(url, session):
    with session.get(url) as response:
        print(f"Read {len(response.content)} from {url}")


def download_all_sites_seq(sites):
    with requests.Session() as session:
        for url in sites:
            download_site_seq(url, session)


async def download_site(session, url):
    async with session.get(url) as response:
        print("Read {0} from {1}".format(response.content_length, url))


async def download_all_sites(sites):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in sites:
            task = asyncio.ensure_future(download_site(session, url))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)


def run_downloader_seq():
    sites = [
                "https://www.jython.org",
                "http://olympus.realpython.org/dice",
            ] * 80
    start_time = time.time()
    download_all_sites_seq(sites)
    duration = time.time() - start_time
    print(f"Downloaded {len(sites)} in {duration} seconds")


def run_downloader_concurrent():
    sites = [
        "https://www.jython.org",
        "http://olympus.realpython.org/dice",
    ] * 80
    start_time = time.time()
    asyncio.get_event_loop().run_until_complete(download_all_sites(sites))
    duration = time.time() - start_time
    print(f"Downloaded {len(sites)} sites in {duration} seconds")


def square(num):
    return num**2


def square_add(num1, num2):
    return num1**2 + num2**2


def run_func(list_length):
    print("Size of List:{}".format(list_length))
    t0 = time.time()
    result1 = [square(x) for x in list(range(list_length))]
    t1 = time.time()
    diff = round(t1-t0, 4)
    print("Running time-Sequential Processing: {} seconds".format(diff))
    time_without_multiprocessing = diff
    # Run with multiprocessing
    t0 = time.time()
    pool = Pool(8)
    result2 = pool.map(square,list(range(list_length)))
    pool.close()
    t1 = time.time()
    diff = round(t1-t0, 4)
    print("Running time-Multiprocessing: {} seconds".format(diff))
    time_with_multiprocessing = diff
    return time_without_multiprocessing, time_with_multiprocessing


def main():
    times_taken = []
    for i in range(1, 9):
        list_length = 10**i
        time_seq, time_parallel = run_func(list_length)
        times_taken.append([list_length, 'No Multiproc', time_seq])
        times_taken.append([list_length, 'Multiproc', time_parallel])

    timedf = pd.DataFrame(times_taken,columns = ['list_length', 'type','time_taken'])
    fig =  px.line(timedf,x = 'List-Length',y='Timetaken',color='type',log_x=True)
    plotly.offline.plot(fig, filename='comparison_bw_multiproc.html')

if __name__ ==  '__main__':
    #main()
    # processors = Pool(4)
    # params = [[100,4],[150,5],[200,6],[300,4]]
    # results = processors.starmap(square_add, params)
    # processors.close()
    # print(results[2])

    run_downloader_concurrent()
    # run_downloader_seq()