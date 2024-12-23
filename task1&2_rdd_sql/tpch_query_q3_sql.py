from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    print("\n=== Starting TPC-H Q3 SparkSQL Implementation ===")
    
    spark = SparkSession.builder \
        .appName("TPCH_Query3_SQL") \
        .getOrCreate()
    
    try:
        print("\n1. Reading data files...")
        # 定义schema
        customer_schema = StructType([
            StructField("c_custkey", IntegerType(), False),
            StructField("c_name", StringType(), True),
            StructField("c_address", StringType(), True),
            StructField("c_nationkey", IntegerType(), True),
            StructField("c_phone", StringType(), True),
            StructField("c_acctbal", DoubleType(), True),
            StructField("c_mktsegment", StringType(), True),
            StructField("c_comment", StringType(), True)
        ])
        
        orders_schema = StructType([
            StructField("o_orderkey", IntegerType(), False),
            StructField("o_custkey", IntegerType(), True),
            StructField("o_orderstatus", StringType(), True),
            StructField("o_totalprice", DoubleType(), True),
            StructField("o_orderdate", StringType(), True),
            StructField("o_orderpriority", StringType(), True),
            StructField("o_clerk", StringType(), True),
            StructField("o_shippriority", IntegerType(), True),
            StructField("o_comment", StringType(), True)
        ])
        
        lineitem_schema = StructType([
            StructField("l_orderkey", IntegerType(), False),
            StructField("l_partkey", IntegerType(), True),
            StructField("l_suppkey", IntegerType(), True),
            StructField("l_linenumber", IntegerType(), True),
            StructField("l_quantity", DoubleType(), True),
            StructField("l_extendedprice", DoubleType(), True),
            StructField("l_discount", DoubleType(), True),
            StructField("l_tax", DoubleType(), True),
            StructField("l_returnflag", StringType(), True),
            StructField("l_linestatus", StringType(), True),
            StructField("l_shipdate", StringType(), True),
            StructField("l_commitdate", StringType(), True),
            StructField("l_receiptdate", StringType(), True),
            StructField("l_shipinstruct", StringType(), True),
            StructField("l_shipmode", StringType(), True),
            StructField("l_comment", StringType(), True)
        ])

        # 读取数据
        customer = spark.read.csv("hdfs://master:9000/tpch/customer.tbl", 
                                sep="|", 
                                schema=customer_schema)
        print("   ✓ Customer table loaded")
        
        orders = spark.read.csv("hdfs://master:9000/tpch/orders.tbl", 
                              sep="|", 
                              schema=orders_schema)
        print("   ✓ Orders table loaded")
        
        lineitem = spark.read.csv("hdfs://master:9000/tpch/lineitem.tbl", 
                                sep="|", 
                                schema=lineitem_schema)
        print("   ✓ Lineitem table loaded")

        print("\n2. Creating temp views...")
        customer.createOrReplaceTempView("customer")
        orders.createOrReplaceTempView("orders")
        lineitem.createOrReplaceTempView("lineitem")
        print("   ✓ All views created")

        print("\n3. Executing query...")
        query = """
        SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        FROM
            customer,
            orders,
            lineitem
        WHERE
            c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < '1995-03-15'
            AND l_shipdate > '1995-03-15'
        GROUP BY
            l_orderkey,
            o_orderdate,
            o_shippriority
        ORDER BY
            revenue desc,
            o_orderdate
        """
        
        result = spark.sql(query)
        
        print("\n4. Saving results...")
        result.write.csv("q3_results_sql.txt", sep="|", header=True)
        print("   ✓ Results saved to q3_results_sql.txt")
        
        print("\n=== SparkSQL Implementation Completed ===\n")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 