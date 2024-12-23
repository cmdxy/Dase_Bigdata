import sqlite3
import time
import pandas as pd
import os
import subprocess
import psutil
import json
from datetime import datetime

class PerformanceMonitor:
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.end_time = None
        self.metrics = {
            "开始时间": "",
            "结束时间": "",
            "总执行时间(秒)": 0,
            "启动时间(秒)": 0,
            "数据加载时间(秒)": 0,
            "查询执行时间(秒)": 0,
            "最大内存使用(MB)": 0,
            "平均内存使用(MB)": 0,
            "执行内存(MB)": 0,
            "存储内存(MB)": 0,
            "其他内存开销(MB)": 0,
            "内存使用峰值时间": "",
            "平均CPU使用率(%)": 0,
            "CPU峰值使用率(%)": 0,
            "IO读取(MB)": 0,
            "IO写入(MB)": 0,
            "磁盘交换次数": 0,
            "运行模式": "纯内存模式",
            "数据统计": {
                "customer": {"大小(MB)": 0, "记录数": 0, "唯一值数": 0, "最大最小比": 0},
                "orders": {"大小(MB)": 0, "记录数": 0, "唯一值数": 0, "最大最小比": 0},
                "lineitem": {"大小(MB)": 0, "记录数": 0, "唯一值数": 0, "最大最小比": 0}
            },
            "缓存统计": {
                "缓存命中率(%)": 0,
                "缓存大小(MB)": 0,
                "溢出量(MB)": 0
            }
        }
        self.cpu_percent_values = []
        self.memory_values = []
        self.last_io_read = 0
        self.last_io_write = 0
        
    def start(self):
        self.start_time = time.time()
        self.metrics["开始时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.process.cpu_percent()  # 第一次调用总是返回0，所以先调用一次初始化

    def update_table_stats(self, table_name, size_mb, record_count, unique_count, max_min_ratio):
        """更新表统计信息"""
        self.metrics["数据统计"][table_name] = {
            "大小(MB)": size_mb,
            "记录数": record_count,
            "唯一值数": unique_count,
            "最大最小比": max_min_ratio
        }

    def update_cache_stats(self, hit_rate, cache_size, overflow):
        """更新缓存统计信息"""
        self.metrics["缓存统计"] = {
            "缓存命中率(%)": hit_rate,
            "缓存大小(MB)": cache_size,
            "溢出量(MB)": overflow
        }

    def update_metrics(self):
        current_memory = self.process.memory_info()
        current_memory_mb = current_memory.rss / 1024 / 1024
        self.memory_values.append(current_memory_mb)
        
        cpu_percent = self.process.cpu_percent()
        self.cpu_percent_values.append(cpu_percent)
        if cpu_percent > self.metrics["CPU峰值使用率(%)"]:
            self.metrics["CPU峰值使用率(%)"] = cpu_percent
        
        # 更新内存使用明细（修正计算方式）
        self.metrics["执行内存(MB)"] = current_memory.data / 1024 / 1024
        self.metrics["存储内存(MB)"] = current_memory.rss / 1024 / 1024  # 总内存
        self.metrics["其他内存开销(MB)"] = current_memory.vms / 1024 / 1024 - current_memory.rss / 1024 / 1024
        
        # 检测IO变化
        io_counters = self.process.io_counters()
        current_io_read = io_counters.read_bytes / 1024 / 1024
        current_io_write = io_counters.write_bytes / 1024 / 1024
        
        if (current_io_read - self.last_io_read > 10) or (current_io_write - self.last_io_write > 10):
            self.metrics["磁盘交换次数"] += 1
            
        self.last_io_read = current_io_read
        self.last_io_write = current_io_write

    def stop(self):
        self.end_time = time.time()
        self.metrics["结束时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.metrics["总执行时间(秒)"] = self.end_time - self.start_time
        
        # 计算内存使用统计
        self.metrics["最大内存使用(MB)"] = max(self.memory_values) if self.memory_values else 0
        self.metrics["平均内存使用(MB)"] = sum(self.memory_values) / len(self.memory_values) if self.memory_values else 0
        max_memory_index = self.memory_values.index(max(self.memory_values)) if self.memory_values else 0
        self.metrics["内存使用峰值时间"] = (datetime.fromtimestamp(self.start_time + max_memory_index)).strftime("%Y-%m-%d %H:%M:%S")
        
        # 计算CPU使用率
        self.metrics["平均CPU使用率(%)"] = sum(self.cpu_percent_values) / len(self.cpu_percent_values) if self.cpu_percent_values else 0
        
        # 获取最终IO统计
        io_counters = self.process.io_counters()
        self.metrics["IO读取(MB)"] = io_counters.read_bytes / 1024 / 1024
        self.metrics["IO写入(MB)"] = io_counters.write_bytes / 1024 / 1024

    def print_summary(self):
        print("\n=== 性能指标摘要 ===")
        print(f"运行模式: {self.metrics['运行模式']}")
        print("\n时间指标:")
        print(f"总执行时间: {self.metrics['总执行时间(秒)']:.2f} 秒")
        print(f"启动时间: {self.metrics['启动时间(秒)']:.2f} 秒")
        print(f"数据加载时间: {self.metrics['数据加载时间(秒)']:.2f} 秒")
        print(f"查询执行时间: {self.metrics['查询执行时间(秒)']:.2f} 秒")
        
        print("\n资源使用:")
        print(f"最大内存使用: {self.metrics['最大内存使用(MB)']:.2f} MB")
        print(f"平均内存使用: {self.metrics['平均内存使用(MB)']:.2f} MB")
        print(f"执行内存: {self.metrics['执行内存(MB)']:.2f} MB")
        print(f"存储内存: {self.metrics['存储内存(MB)']:.2f} MB")
        print(f"其他内存开销: {self.metrics['其他内存开销(MB)']:.2f} MB")
        print(f"内存使用峰值时间: {self.metrics['内存使用峰值时间']}")
        print(f"平均CPU使用率: {self.metrics['平均CPU使用率(%)']:.2f}%")
        print(f"CPU峰值使用率: {self.metrics['CPU峰值使用率(%)']:.2f}%")
        print(f"IO读取: {self.metrics['IO读取(MB)']:.2f} MB")
        print(f"IO写入: {self.metrics['IO写入(MB)']:.2f} MB")
        print(f"磁盘交换次数: {self.metrics['磁盘交换次数']}")
        
        print("\n数据统计:")
        for table, stats in self.metrics["数据统计"].items():
            print(f"\n{table}表:")
            print(f"大小: {stats['大小(MB)']:.2f} MB")
            print(f"记录数: {stats['记录数']}")
            print(f"唯一值数: {stats['唯一值数']}")
            print(f"最大最小比: {stats['最大最小比']:.2f}")
        
        print("\n缓存统计:")
        print(f"缓存命中率: {self.metrics['缓存统计']['缓存命中率(%)']:.2f}%")
        print(f"缓存大小: {self.metrics['缓存统计']['缓存大小(MB)']:.2f} MB")
        print(f"溢出量: {self.metrics['缓存统计']['溢出量(MB)']:.2f} MB")
        
        if "Join执行细节" in self.metrics:
            print("\nJoin执行细节:")
            join_details = self.metrics["Join执行细节"]
            print(f"执行计划: {join_details['执行计划']}")
            print(f"中间结果大小: {join_details['中间结果大小(行数)']} 行")
            print(f"缓冲区大小: {join_details['缓冲区大小(KB)']} KB")
            print(f"页大小: {join_details['页大小(bytes)']} bytes")

    def save_metrics(self, filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.metrics, f, ensure_ascii=False, indent=4)
        print(f"性能指标已保存到 {filename}")

class MemorySQL:
    def __init__(self, monitor=None):
        """
        初始化SQLite内存数据库连接
        :param monitor: 性能监控器
        """
        self.monitor = monitor
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        
        # 优化设置 - 使用大内存缓存
        self.cursor.execute('PRAGMA cache_size = -2097152')  # 设置2GB缓存
        self.cursor.execute('PRAGMA temp_store = MEMORY')    # 临时表使用内存
        self.cursor.execute('PRAGMA journal_mode = WAL')     # 使用WAL模式提高写入性能
        self.cursor.execute('PRAGMA synchronous = NORMAL')   # 降低同步级别提高性能
        
        print("使用内存数据库模式")

    def read_from_hdfs(self, hdfs_path):
        """
        从HDFS读取文件内容
        :param hdfs_path: HDFS上的文件路径
        :return: 文件内容的行列表
        """
        cmd = ["hadoop", "fs", "-cat", hdfs_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.splitlines()
        except subprocess.CalledProcessError as e:
            print(f"读取HDFS文件失败: {e}")
            raise

    def create_table_from_tpch(self, hdfs_path, table_name, schema):
        """
        从TPC-H格式的HDFS数据文件创建表
        :param hdfs_path: HDFS上的数据文件路径
        :param table_name: 表名
        :param schema: 列名和类型字典
        """
        print(f"正在加载 {table_name} 表...")
        
        load_start_time = time.time()
        
        # 创建表
        columns = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(create_table_sql)
        
        # 从HDFS读取数据并批量插入
        lines = self.read_from_hdfs(hdfs_path)
        # 计算每个表的列数，确保批量插入不超过SQLite的变量限制（999）
        num_columns = len(schema)
        # 每批次的行数 = 最大变量数 / 列数，向下取整
        rows_per_batch = 999 // num_columns
        batch = []
        total_rows = 0
        last_progress = 0
        
        for line in lines:
            values = line.strip().split('|')[:-1]  # TPC-H文件以|结尾，需要去掉最后一个空值
            batch.append(values)
            total_rows += 1
            
            if len(batch) >= rows_per_batch:
                # 批量插入
                placeholders = ",".join(["(" + ",".join(["?" for _ in values]) + ")" for _ in batch])
                flat_values = [item for sublist in batch for item in sublist]
                insert_sql = f"INSERT INTO {table_name} VALUES {placeholders}"
                self.cursor.execute(insert_sql, flat_values)
                
                # 每10000行更新一次监控和进度
                current_progress = total_rows // 10000
                if current_progress > last_progress:
                    if self.monitor:
                        self.monitor.update_metrics()
                    print(f"已加载 {total_rows} 行...")
                    last_progress = current_progress
                
                batch = []
        
        # 插入剩余的数据
        if batch:
            placeholders = ",".join(["(" + ",".join(["?" for _ in values]) + ")" for _ in batch])
            flat_values = [item for sublist in batch for item in sublist]
            insert_sql = f"INSERT INTO {table_name} VALUES {placeholders}"
            self.cursor.execute(insert_sql, flat_values)
        
        self.conn.commit()
        
        # 创建索引以优化查询
        print(f"为 {table_name} 创建索引...")
        if table_name == 'customer':
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_custkey ON {table_name}(c_custkey)")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_mktsegment ON {table_name}(c_mktsegment)")
        elif table_name == 'orders':
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_orderkey ON {table_name}(o_orderkey)")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_custkey ON {table_name}(o_custkey)")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_orderdate ON {table_name}(o_orderdate)")
        elif table_name == 'lineitem':
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_orderkey ON {table_name}(l_orderkey)")
            self.cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_shipdate ON {table_name}(l_shipdate)")
        
        self.conn.commit()
        
        # 收集表统计信息
        if self.monitor:
            # 获取大小（近似值，因为是内存数据库）
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = self.cursor.fetchone()[0]
            
            # 获取第一个列的唯一值数量和最大最小比
            first_column = list(schema.keys())[0]
            self.cursor.execute(f"""
                SELECT COUNT(DISTINCT {first_column}) as unique_count,
                       CAST(MAX(cnt) AS FLOAT) / MIN(cnt) as max_min_ratio
                FROM (
                    SELECT {first_column}, COUNT(*) as cnt
                    FROM {table_name}
                    GROUP BY {first_column}
                ) t
            """)
            unique_count, max_min_ratio = self.cursor.fetchone()
            
            # 估算表大小（假设每行平均100字节）
            estimated_size_mb = (record_count * 100) / (1024 * 1024)
            
            self.monitor.update_table_stats(
                table_name,
                estimated_size_mb,
                record_count,
                unique_count,
                max_min_ratio if max_min_ratio else 1.0
            )
        
        load_time = time.time() - load_start_time
        if self.monitor:
            self.monitor.metrics["数据加载时间(秒)"] += load_time
            
        print(f"✓ {table_name} 表加载完成，耗时: {load_time:.2f} 秒")

    def execute_tpch_q3(self):
        """
        执行TPC-H Q3查询并计时
        :return: 查询结果和执行时间
        """
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
            revenue DESC,
            o_orderdate
        """
        
        # 首先执行主查询并记录时间
        query_start_time = time.time()
        result = pd.read_sql_query(query, self.conn)
        query_time = time.time() - query_start_time
        
        if self.monitor:
            self.monitor.metrics["查询执行时间(秒)"] = query_time
            self.monitor.update_metrics()
            
            # 在主查询完成后收集执行细节
            try:
                # 获取执行计划
                self.cursor.execute("EXPLAIN QUERY PLAN SELECT * FROM customer,orders,lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey")
                query_plan = self.cursor.fetchall()
                
                # 获取中间结果大小
                self.cursor.execute("""
                    SELECT COUNT(*) as result_size
                    FROM (
                        SELECT DISTINCT l_orderkey, o_orderdate, o_shippriority
                        FROM customer,orders,lineitem 
                        WHERE c_mktsegment = 'BUILDING'
                        AND c_custkey = o_custkey
                        AND l_orderkey = o_orderkey
                        AND o_orderdate < '1995-03-15'
                        AND l_shipdate > '1995-03-15'
                    ) t
                """)
                intermediate_result_size = self.cursor.fetchone()[0]
                
                # 获取缓冲区设置
                self.cursor.execute('PRAGMA page_size')
                page_size = self.cursor.fetchone()[0]
                self.cursor.execute('PRAGMA cache_size')
                cache_size = self.cursor.fetchone()[0]
                buffer_pool_size = abs(cache_size) * page_size / 1024  # 转换为KB
                
                # 添加Join执行细节到监控指标
                self.monitor.metrics["Join执行细节"] = {
                    "执行计划": str(query_plan),
                    "中间结果大小(行数)": intermediate_result_size,
                    "缓冲区大小(KB)": buffer_pool_size,
                    "页大小(bytes)": page_size
                }
            except Exception as e:
                print(f"收集执行细节时发生错误: {e}")
            
        return result, query_time

    def close(self):
        """
        关闭数据库连接
        """
        self.conn.close()

def main():
    print("\n=== 开始 TPC-H Q3 纯内存SQL实现 ===")
    
    # 创建输出目录
    OUTPUT_DIR = "output/memory"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 初始化性能监控
    monitor = PerformanceMonitor()
    monitor.start()
    
    # HDFS路径
    HDFS_BASE = "hdfs://master:9000/tpch"
    
    # 定义表结构
    customer_schema = {
        "c_custkey": "INTEGER",
        "c_name": "TEXT",
        "c_address": "TEXT",
        "c_nationkey": "INTEGER",
        "c_phone": "TEXT",
        "c_acctbal": "REAL",
        "c_mktsegment": "TEXT",
        "c_comment": "TEXT"
    }
    
    orders_schema = {
        "o_orderkey": "INTEGER",
        "o_custkey": "INTEGER",
        "o_orderstatus": "TEXT",
        "o_totalprice": "REAL",
        "o_orderdate": "TEXT",
        "o_orderpriority": "TEXT",
        "o_clerk": "TEXT",
        "o_shippriority": "INTEGER",
        "o_comment": "TEXT"
    }
    
    lineitem_schema = {
        "l_orderkey": "INTEGER",
        "l_partkey": "INTEGER",
        "l_suppkey": "INTEGER",
        "l_linenumber": "INTEGER",
        "l_quantity": "REAL",
        "l_extendedprice": "REAL",
        "l_discount": "REAL",
        "l_tax": "REAL",
        "l_returnflag": "TEXT",
        "l_linestatus": "TEXT",
        "l_shipdate": "TEXT",
        "l_commitdate": "TEXT",
        "l_receiptdate": "TEXT",
        "l_shipinstruct": "TEXT",
        "l_shipmode": "TEXT",
        "l_comment": "TEXT"
    }

    try:
        sql = MemorySQL(monitor=monitor)
        
        print("\n1. 加载数据文件...")
        # 加载TPC-H数据文件
        sql.create_table_from_tpch(f"{HDFS_BASE}/customer.tbl", "customer", customer_schema)
        sql.create_table_from_tpch(f"{HDFS_BASE}/orders.tbl", "orders", orders_schema)
        sql.create_table_from_tpch(f"{HDFS_BASE}/lineitem.tbl", "lineitem", lineitem_schema)
        
        print("\n2. 执行Q3查询...")
        result, query_time = sql.execute_tpch_q3()
        
        print("\n3. 保存结果...")
        # 保存查询结果
        result_file = os.path.join(OUTPUT_DIR, "q3_results_memory.txt")
        result.to_csv(result_file, sep="|", index=False)
        print(f"✓ 查询时间: {query_time:.2f} 秒")
        print(f"查询结果已保存到 {result_file}")
        
        # 停止性能监控并保存指标
        monitor.stop()
        metrics_file = os.path.join(OUTPUT_DIR, "q3_performance_memory.json")
        monitor.save_metrics(metrics_file)
        
        # 打印性能摘要
        monitor.print_summary()
        
        print("\n=== 纯内存SQL实现完成 ===\n")
        
    except Exception as e:
        print(f"发生错误: {e}")
        raise
    finally:
        sql.close()

if __name__ == "__main__":
    main() 