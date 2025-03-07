#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
import sys
import os
import time
from datetime import datetime

# MySQL连接配置 - 使用已知可连接的参数
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'hello!edgenesis',
    'database': 'wechat_data',
    'port': 13306,
    'connect_timeout': 60,
    'unix_socket': ''  # 确保使用TCP/IP连接
}

# 输出文件路径
OUTPUT_FILE = 'input.txt'

# 表名 - 默认为"messages"，可以更改为实际表名
TABLE_NAME = os.getenv("DB_TABLE", "messages")

def connect_to_mysql_with_retry(max_retries=3, retry_delay=5):
    """带重试机制的MySQL连接函数"""
    for attempt in range(max_retries):
        try:
            print(f"连接尝试 {attempt+1}/{max_retries}...")
            connection = mysql.connector.connect(**DB_CONFIG)
            print("连接成功!")
            return connection
        except mysql.connector.Error as e:
            print(f"连接失败: {e}")
            if attempt < max_retries - 1:
                print(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print("达到最大重试次数，放弃连接")
                raise

def list_tables(connection):
    """列出数据库中的所有表"""
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables
    except mysql.connector.Error as e:
        print(f"获取表列表时出错: {e}")
        return []

def fetch_data(connection, table_name):
    """从指定表获取数据"""
    try:
        cursor = connection.cursor(dictionary=True)
        
        # 查询表中的所有数据
        query = f"SELECT * FROM {table_name} ORDER BY id DESC"
        print(f"执行查询: {query}")
        cursor.execute(query)
        
        # 获取所有行
        rows = cursor.fetchall()
        
        # 获取列名
        columns = [column[0] for column in cursor.description]
        
        cursor.close()
        
        print(f"成功从 {table_name} 获取 {len(rows)} 条记录")
        return rows, columns
    
    except mysql.connector.Error as e:
        print(f"查询数据时出错: {e}")
        return None, None

def format_data_for_txt(data, columns):
    """将数据格式化为文本格式"""
    formatted_lines = []
    
    if not data or not columns:
        print("没有数据可格式化")
        return []
    
    # 识别关键字段
    id_field = next((col for col in columns if col.lower() == 'id'), None)
    user_field = next((col for col in columns if col.lower() in ['user_id', 'customer', 'user', 'from_user']), None)
    time_field = next((col for col in columns if col.lower() in ['time', 'date', 'timestamp', 'created_at', 'create_time']), None)
    message_field = next((col for col in columns if col.lower() in ['message', 'content', 'chat_records', 'comment', 'msg', 'text']), None)
    
    if not id_field:
        print("警告: 无法识别ID字段")
        print(f"可用字段: {columns}")
        id_field = columns[0]  # 使用第一列作为ID
        print(f"使用 {id_field} 作为ID字段")
    
    if not message_field:
        print("警告: 无法识别消息内容字段")
        print(f"可用字段: {columns}")
        # 尝试找到可能包含文本内容的最长字段
        potential_text_fields = []
        for row in data[:5]:  # 只检查前5行
            for col in columns:
                if isinstance(row[col], str) and len(row[col]) > 10:
                    potential_text_fields.append((col, len(row[col])))
        
        if potential_text_fields:
            # 选择平均长度最长的字段
            from collections import defaultdict
            field_lengths = defaultdict(list)
            for field, length in potential_text_fields:
                field_lengths[field].append(length)
            
            avg_lengths = [(field, sum(lengths)/len(lengths)) for field, lengths in field_lengths.items()]
            message_field = max(avg_lengths, key=lambda x: x[1])[0]
            print(f"使用 {message_field} 作为消息内容字段")
    
    # 格式化每一行数据
    for row in data:
        line_parts = []
        
        # 添加ID
        if id_field:
            line_parts.append(f"id:{row[id_field]}")
        
        # 添加用户ID (如果存在)
        if user_field and row[user_field]:
            line_parts.append(f"user_id:{row[user_field]}")
        
        # 添加时间 (如果存在)
        if time_field and row[time_field]:
            # 处理不同格式的时间
            time_value = row[time_field]
            if isinstance(time_value, datetime):
                time_str = time_value.strftime("%H:%M:%S")
            else:
                time_str = str(time_value)
            line_parts.append(f"time:{time_str}")
        
        # 添加消息内容
        if message_field and row[message_field]:
            line_parts.append(f"message:{row[message_field]}")
        
        # 将所有部分组合成一行
        formatted_line = " ".join(line_parts)
        formatted_lines.append(formatted_line)
    
    return formatted_lines

def save_to_file(formatted_data, filename):
    """保存格式化的数据到文件"""
    if not formatted_data:
        print("没有数据可保存")
        return
        
    try:
        print(f"正在保存数据到文件: {filename}")
        
        # 直接覆盖现有文件，不创建备份
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(formatted_data))
        
        print(f"数据已成功保存到 {filename}")
        print(f"文件大小: {os.path.getsize(filename)} 字节")
    except Exception as e:
        print(f"保存文件失败: {e}")
        sys.exit(1)

def main():
    """主函数"""
    print("开始从MySQL导出数据到文本文件...")
    
    # 连接数据库
    connection = connect_to_mysql_with_retry()
    
    try:
        # 列出所有表
        tables = list_tables(connection)
        print(f"数据库中的表: {', '.join(tables)}")
        
        # 确定要查询的表
        table_to_query = TABLE_NAME
        if table_to_query not in tables:
            print(f"警告: 表 '{table_to_query}' 不存在")
            if tables:
                table_to_query = tables[0]
                print(f"使用第一个可用的表: '{table_to_query}'")
            else:
                print("数据库中没有表，无法继续")
                return
        
        # 获取数据
        data, columns = fetch_data(connection, table_to_query)
        
        if data:
            # 格式化数据
            formatted_data = format_data_for_txt(data, columns)
            
            # 保存到文件
            save_to_file(formatted_data, OUTPUT_FILE)
            
            print("数据导出完成!")
        else:
            print("没有数据可导出")
    finally:
        # 关闭连接
        connection.close()
        print("数据库连接已关闭")

if __name__ == "__main__":
    main()