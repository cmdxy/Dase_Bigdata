#!/usr/bin/env python3
import subprocess
import time
import json
import os
from datetime import datetime

def run_single_test(script_path):
    # 创建输出目录
    output_dir = "benchmark_results"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 设置命令
    cmd = [
        "/usr/local/spark3.4.4/bin/spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--num-executors", "2",
        "--executor-cores", "1",
        "--executor-memory", "512M",
        "--conf", "spark.eventLog.enabled=true",
        "--conf", "spark.eventLog.dir=hdfs://master:9000/spark-history",
        "--conf", "spark.history.fs.logDirectory=hdfs://master:9000/spark-history",
        "--conf", "spark.yarn.historyServer.address=master:18080",
        script_path
    ]
    
    # 记录开始时间
    start_time = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # 运行命令
        print("Starting Spark job...")
        process = subprocess.run(cmd, capture_output=True, text=True)
        duration = time.time() - start_time
        
        # 从输出中提取应用程序ID
        stdout_lines = process.stdout.split('\n')
        app_id = None
        for line in stdout_lines:
            if "application_" in line:
                app_id = line.split("application_")[1].split()[0]
                app_id = "application_" + app_id
                break
        
        # 收集结果
        result = {
            "timestamp": timestamp,
            "duration": duration,
            "success": process.returncode == 0,
            "application_id": app_id,
            "stdout": process.stdout,
            "stderr": process.stderr
        }
        
        # 添加性能指标（如果存在）
        if os.path.exists('q3_performance_cache.txt'):
            with open('q3_performance_cache.txt', 'r') as f:
                result["performance_metrics"] = f.read()
        
        # 保存结果到JSON文件
        result_file = f"{output_dir}/single_test_result_cache_{timestamp}.json"
        with open(result_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        # 打印结果摘要
        print("\nTest Results Summary:")
        print("-" * 80)
        print(f"{'Duration':<10} {'App ID':<35} {'Success':<8}")
        print("-" * 80)
        if app_id:
            print(f"{duration:.2f}s     {app_id:<35} {'✓' if result['success'] else '✗':<8}")
        else:
            print(f"{duration:.2f}s     {'<No App ID>':<35} {'✗':<8}")
        print("-" * 80)
        
        print("\nWeb UI访问指南:")
        print("1. Spark History Server: http://master:18080")
        print("2. YARN Resource Manager: http://master:8088")
        print(f"\n结果已保存到: {result_file}")
        
        return result
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

def main():
    script_path = "tpch_query_q3_rdd_cache.py"
    run_single_test(script_path)

if __name__ == "__main__":
    main() 
