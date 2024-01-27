"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing

import util.judge_df_equal
import numpy as np
import math

ray.init(num_cpus=2, ignore_reinit_error=True)

@ray.remote
def calculate_shipping_priority(chunk: pd.DataFrame, customer: pd.DataFrame, orders: pd.DataFrame, segment: str) -> pd.DataFrame:
    chunk['l_shipdate'] = pd.to_datetime(chunk['l_shipdate'])
    filtered_chunk = chunk[chunk['l_shipdate'] > pd.to_datetime("1995-03-15")]
    
    merged_data = customer.merge(orders, how='left', left_on='c_custkey', right_on='o_custkey').merge(filtered_chunk, how='left', left_on='o_orderkey', right_on='l_orderkey') 

    grouped_data = merged_data.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=pd.NamedAgg(column='l_extendedprice', aggfunc=lambda x: (x * (1 - merged_data['l_discount'])).sum())).reset_index()
    
    return grouped_data



def ray_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin

    customer = customer[customer['c_mktsegment'] == segment]
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    orders = orders[orders['o_orderdate'] < pd.to_datetime("1995-03-15")]

    customer_id = ray.put(customer)
    orders_id = ray.put(orders)
    
    chunk_size = 4
    num_chunks = math.ceil(len(lineitem) / chunk_size)
    chunks = np.array_split(lineitem, 4)
    
    actors = [calculate_shipping_priority.remote(chunk, customer_id, orders_id, segment) for chunk in chunks]
    
    # Collect and concatenate the results from actors
    results = ray.get(actors)
    combine_result = pd.concat(results)
    
    final_result = combine_result.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=pd.NamedAgg(column='revenue', aggfunc='sum')
    ).reset_index()
    
    final_result = final_result.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True])
    
    ray.shutdown()

    return final_result.head(10)
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")


    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")