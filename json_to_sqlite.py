import json
import sqlite3
import os
from datetime import datetime

# 固定的输入和输出文件
INPUT_JSON = 'output.json'
OUTPUT_DB = 'customer_service.db'

def create_tables(conn):
    """创建数据库表"""
    cursor = conn.cursor()
    
    # 创建问题反馈表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS issues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        issue_type TEXT,
        description TEXT,
        urgency TEXT,
        completion INTEGER,
        status TEXT,
        negative_feedback TEXT
    )
    ''')
    
    # 创建销售数据表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        region TEXT,
        product TEXT,
        sales_count INTEGER,
        sales_amount REAL,
        achievement_rate REAL
    )
    ''')
    
    conn.commit()

def import_json_to_sqlite():
    """将JSON数据导入到SQLite数据库"""
    print(f"正在将JSON数据 ({INPUT_JSON}) 导入到SQLite数据库 ({OUTPUT_DB})...")
    
    # 检查输入文件是否存在
    if not os.path.exists(INPUT_JSON):
        print(f"错误: 输入文件 '{INPUT_JSON}' 不存在")
        return False
    
    try:
        # 读取JSON文件
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 如果数据库已存在，删除它
        if os.path.exists(OUTPUT_DB):
            print(f"删除现有数据库文件: {OUTPUT_DB}")
            os.remove(OUTPUT_DB)
        
        # 连接到SQLite数据库（这将创建一个新的数据库文件）
        conn = sqlite3.connect(OUTPUT_DB)
        
        # 创建表
        create_tables(conn)
        
        # 导入问题反馈数据
        issues_count = 0
        if "issues" in data and data["issues"]:
            cursor = conn.cursor()
            
            # 准备SQL语句
            sql = '''
            INSERT INTO issues (date, issue_type, description, urgency, completion, status, negative_feedback)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            '''
            
            # 准备数据
            values = []
            for issue in data["issues"]:
                values.append((
                    issue.get("date", ""),
                    issue.get("issue_type", ""),
                    issue.get("description", ""),
                    issue.get("urgency", "中"),
                    issue.get("completion", 0),
                    issue.get("status", "未处理"),
                    issue.get("negative_feedback", "否")
                ))
            
            # 执行批量插入
            cursor.executemany(sql, values)
            conn.commit()
            
            issues_count = len(values)
            print(f"成功导入 {issues_count} 条问题反馈数据")
        
        # 导入销售数据
        sales_count = 0
        if "sales" in data and data["sales"]:
            cursor = conn.cursor()
            
            # 准备SQL语句
            sql = '''
            INSERT INTO sales (date, region, product, sales_count, sales_amount, achievement_rate)
            VALUES (?, ?, ?, ?, ?, ?)
            '''
            
            # 准备数据
            values = []
            for sale in data["sales"]:
                values.append((
                    sale.get("date", ""),
                    sale.get("region", ""),
                    sale.get("product_model", ""),  # 注意：JSON中可能是product_model
                    sale.get("quantity", 0),        # 注意：JSON中可能是quantity
                    sale.get("amount", 0.0),        # 注意：JSON中可能是amount
                    sale.get("completion_rate", 0.0) # 注意：JSON中可能是completion_rate
                ))
            
            # 执行批量插入
            cursor.executemany(sql, values)
            conn.commit()
            
            sales_count = len(values)
            print(f"成功导入 {sales_count} 条销售数据")
        
        # 关闭连接
        conn.close()
        
        # 显示数据库信息
        print(f"\n数据库信息:")
        print(f"- 数据库文件: {OUTPUT_DB}")
        print(f"- 文件大小: {os.path.getsize(OUTPUT_DB)} 字节")
        print(f"- 问题反馈记录: {issues_count} 条")
        print(f"- 销售数据记录: {sales_count} 条")
        print(f"- 总记录数: {issues_count + sales_count} 条")
        
        print(f"\n数据导入完成!")
        return True
        
    except json.JSONDecodeError:
        print(f"错误: '{INPUT_JSON}' 不是有效的JSON文件")
        return False
    except Exception as e:
        print(f"导入数据时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    import_json_to_sqlite()

if __name__ == "__main__":
    main() 