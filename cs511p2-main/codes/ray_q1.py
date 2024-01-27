"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:47
 	 LastEditTime: 2023-09-11 10:37:24
 	 FilePath: /codes/pandas_q1.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import numpy as np
import math

ray.init(ignore_reinit_error=True)

@ray.remote
def calculate_revenue(time: str, chunk: pd.DataFrame) -> float:

    chunk['l_shipdate'] = pd.to_datetime(chunk['l_shipdate'])

    filtered_chunk = chunk[
        (chunk['l_shipdate'] >= pd.to_datetime(time)) &
        (chunk['l_shipdate'] < pd.to_datetime(time) + pd.DateOffset(years=1)) &
        (chunk['l_discount'] >= 0.05) &
        (chunk['l_discount'] <= 0.07) &
        (chunk['l_quantity'] < 24)
    ]
    
    revenue = (filtered_chunk['l_extendedprice'] * filtered_chunk['l_discount']).sum()
    
    # ray.shutdown()

    return revenue

def ray_q1(time: str, lineitem:pd.DataFrame) -> float:
    # TODO: your codes begin
    # print(len(lineitem))

    chunk_size = 10000
    num_chunks = math.ceil(len(lineitem) // chunk_size)
    chunks = np.array_split(lineitem, num_chunks)
    
    actors = [calculate_revenue.remote(time, chunk) for chunk in chunks]
    
    results = ray.get(actors)
    total_revenue = np.sum(results)
    
    ray.shutdown()
    
    return total_revenue
    # end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # run the test
    result = ray_q1("1994-01-01", lineitem)
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")
