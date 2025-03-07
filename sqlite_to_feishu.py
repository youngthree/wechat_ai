import json
import argparse
import sqlite3
import requests
import time
from datetime import datetime
import os
import sys
import subprocess

class FeishuUploader:
    def __init__(self, config_path="feishu_config.json"):
        """初始化飞书上传器"""
        self.config_path = config_path
        
        # 从配置文件读取信息
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        self.app_id = config.get('app_id')
        self.app_secret = config.get('app_secret')
        self.bitable_id = config.get('bitable_id')  # app_token
        self.issues_table_id = config.get('issues_table_id')
        self.sales_table_id = config.get('sales_table_id')
        self.access_token = config.get('access_token')
        self.token_expires_at = config.get('token_expires_at', 0)
        
        # 检查令牌是否过期，如果过期则刷新
        if time.time() > self.token_expires_at:
            self._refresh_token_with_manager()
        
        # 上传配置
        self.batch_size = 20
        self.max_retries = 3
        self.initial_retry_delay = 2  # 初始重试延迟（秒）
        self.max_retry_delay = 30  # 最大重试延迟（秒）
        
    def _refresh_token_with_manager(self):
        """使用token_manager.py刷新令牌"""
        print("令牌已过期，使用token_manager.py刷新...")
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            token_manager_path = os.path.join(script_dir, "token_manager.py")
            
            # 运行token_manager.py来更新令牌
            result = subprocess.run([sys.executable, token_manager_path, "--force"], 
                                   check=True, capture_output=True, text=True)
            
            print(result.stdout)
            
            # 重新加载配置
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            self.access_token = config.get('access_token')
            self.token_expires_at = config.get('token_expires_at', 0)
            print(f"令牌已刷新: {self.access_token[:10]}...{self.access_token[-10:]}")
            
        except subprocess.CalledProcessError as e:
            print(f"刷新令牌失败: {e}")
            if e.stdout:
                print(f"输出: {e.stdout}")
            if e.stderr:
                print(f"错误: {e.stderr}")
            raise Exception("无法刷新飞书访问令牌")
            
    def upload_issues_to_feishu(self, issues_data):
        """将问题反馈数据上传到飞书表格"""
        if not issues_data:
            print("没有问题反馈数据需要上传")
            return 0
        
        # 先删除现有记录
        print("正在删除问题反馈表中的现有记录...")
        self.delete_all_records(self.issues_table_id)
        
        if not self.bitable_id:
            raise ValueError("未提供多维表格ID")
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.bitable_id}/tables/{self.issues_table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        # 准备数据
        records = []
        for issue in issues_data:
            # 将日期字符串转换为Unix时间戳（毫秒）
            date_str = issue.get("date", "").replace("/", "-") if issue.get("date") else ""
            date_timestamp = None
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    # 转换为毫秒级时间戳
                    date_timestamp = int(time.mktime(date_obj.timetuple())) * 1000
                except ValueError:
                    print(f"警告: 无法解析日期 '{date_str}'，使用当前时间")
                    date_timestamp = int(time.time()) * 1000
            
            fields = {
                "日期": date_timestamp,
                "问题类型": [issue.get("issue_type", "")] if issue.get("issue_type") else [],
                "问题描述": issue.get("description", ""),
                "紧急程度": issue.get("urgency", "中"),
                "完成度": issue.get("completion", 0),
                "处理状态": issue.get("status", "未处理"),
                "负反馈": issue.get("negative_feedback", "否")
            }
            
            record = {"fields": fields}
            records.append(record)
        
        # 分批上传
        total_batches = (len(records) + self.batch_size - 1) // self.batch_size
        print(f"问题反馈数据共 {len(records)} 条，将分 {total_batches} 批上传")
        
        success_count = 0
        
        for i in range(0, len(records), self.batch_size):
            batch_records = records[i:i+self.batch_size]
            batch_num = i // self.batch_size + 1
            
            data = {
                "records": batch_records
            }
            
            # 使用重试机制
            success = self._upload_batch_with_retry(
                url, 
                headers, 
                data, 
                f"问题反馈数据批次 {batch_num}/{total_batches}"
            )
            
            if success:
                success_count += len(batch_records)
        
        print(f"问题反馈数据上传完成: 成功 {success_count}/{len(records)} 条")
        return success_count
    
    def upload_sales_to_feishu(self, sales_data):
        """将销售数据上传到飞书表格"""
        if not sales_data:
            print("没有销售数据需要上传")
            return 0
        
        # 先删除现有记录
        print("正在删除销售数据表中的现有记录...")
        self.delete_all_records(self.sales_table_id)
        
        if not self.bitable_id:
            raise ValueError("未提供多维表格ID")
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.bitable_id}/tables/{self.sales_table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        # 准备数据
        records = []
        for sale in sales_data:
            fields = {
                "日期": sale.get("date", "").replace("/", "-") if sale.get("date") else "",
                "区域": sale.get("region", ""),
                "产品型号": sale.get("product", ""),
                "销售": sale.get("sales_count", 0),
                "销售额": sale.get("sales_amount", 0),
                "达成率": sale.get("achievement_rate", 0)
            }
            
            record = {"fields": fields}
            records.append(record)
        
        # 分批上传，使用增强的批处理逻辑
        total_batches = (len(records) + self.batch_size - 1) // self.batch_size
        print(f"销售数据共 {len(records)} 条，将分 {total_batches} 批上传")
        
        success_count = 0
        
        for i in range(0, len(records), self.batch_size):
            batch_records = records[i:i+self.batch_size]
            batch_num = i // self.batch_size + 1
            
            data = {
                "records": batch_records
            }
            
            # 使用重试机制
            success = self._upload_batch_with_retry(
                url, 
                headers, 
                data, 
                f"销售数据批次 {batch_num}/{total_batches}"
            )
            
            if success:
                success_count += len(batch_records)
        
        print(f"销售数据上传完成: 成功 {success_count}/{len(records)} 条")
        return success_count
    
    def _upload_batch_with_retry(self, url, headers, data, batch_desc):
        """带重试机制的批量上传"""
        for retry in range(self.max_retries):
            try:
                # 检查并刷新令牌
                if retry > 0:
                    # 刷新令牌
                    self._refresh_token_with_manager()
                    headers["Authorization"] = f"Bearer {self.access_token}"
                
                response = requests.post(url, headers=headers, json=data)
                response_data = response.json()
                
                if response_data.get("code") == 0:
                    print(f"成功上传{batch_desc}")
                    return True
                elif response_data.get("code") == 99991663:  # 令牌过期
                    print(f"令牌已过期，正在刷新...")
                    # 强制刷新令牌
                    self._refresh_token_with_manager()
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    # 不计入重试次数，直接重试
                    continue
                else:
                    error_msg = response_data.get("msg", "未知错误")
                    print(f"上传{batch_desc}失败: {error_msg}")
                    print(f"完整响应: {response_data}")
                    
                    if "Authentication token expired" in error_msg:
                        print("检测到令牌过期，强制刷新...")
                        self._refresh_token_with_manager()
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        continue
                    
                    # 如果是API限流错误，等待更长时间
                    if "频率" in error_msg or "rate limit" in error_msg.lower():
                        wait_time = self.initial_retry_delay * (2 ** retry)
                        wait_time = min(wait_time, self.max_retry_delay)
                        print(f"检测到API限流，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                    else:
                        # 其他错误，等待较短时间
                        wait_time = self.initial_retry_delay * (retry + 1)
                        print(f"将在 {wait_time} 秒后重试 ({retry+1}/{self.max_retries})...")
                        time.sleep(wait_time)
            except Exception as e:
                print(f"上传{batch_desc}时出错: {e}")
                try:
                    wait_time = self.initial_retry_delay * (retry + 1)
                    print(f"将在 {wait_time} 秒后重试 ({retry+1}/{self.max_retries})...")
                    time.sleep(wait_time)
                except AttributeError:
                    # 防御性编程：如果initial_retry_delay不存在
                    print(f"将在 {(retry+1)*2} 秒后重试 ({retry+1}/{self.max_retries})...")
                    time.sleep((retry+1)*2)
        
        print(f"上传{batch_desc}失败，已达到最大重试次数")
        return False
    
    def delete_all_records(self, table_id):
        """删除表中的所有记录"""
        if not self.bitable_id:
            raise ValueError("未提供多维表格ID")
        
        # 首先获取所有记录
        all_record_ids = []
        page_token = None
        
        while True:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.bitable_id}/tables/{table_id}/records"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response_data = response.json()
                
                if response_data.get("code") == 0:
                    records = response_data.get("data", {}).get("items", [])
                    for record in records:
                        all_record_ids.append(record.get("record_id"))
                    
                    page_token = response_data.get("data", {}).get("page_token")
                    if not page_token:
                        break
                elif response_data.get("code") == 99991663:  # 令牌过期
                    print("令牌已过期，正在刷新...")
                    self._refresh_token_with_manager()
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    # 不改变page_token，重试当前页
                    continue
                else:
                    print(f"获取记录失败: {response_data}")
                    return 0
            except Exception as e:
                print(f"获取记录时出错: {e}")
                return 0
        
        print(f"找到 {len(all_record_ids)} 条记录需要删除")
        
        # 如果没有记录，直接返回
        if not all_record_ids:
            return 0
        
        # 批量删除记录
        batch_size = 100
        success_count = 0
        total_batches = (len(all_record_ids) + batch_size - 1) // batch_size
        
        for i in range(0, len(all_record_ids), batch_size):
            batch_records = all_record_ids[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.bitable_id}/tables/{table_id}/records/batch_delete"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            data = {
                "records": batch_records
            }
            
            for retry in range(self.max_retries):
                try:
                    response = requests.post(url, headers=headers, json=data)
                    response_data = response.json()
                    
                    if response_data.get("code") == 0:
                        success_count += len(batch_records)
                        print(f"成功删除批次 {batch_num}/{total_batches}")
                        break
                    elif response_data.get("code") == 99991663:  # 令牌过期
                        print("令牌已过期，正在刷新...")
                        self._refresh_token_with_manager()
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        continue
                    else:
                        print(f"删除批次 {batch_num} 失败: {response_data}")
                        time.sleep(2 * (retry + 1))
                except Exception as e:
                    print(f"删除批次 {batch_num} 时出错: {e}")
                    time.sleep(2 * (retry + 1))
            
            # 批次间暂停，避免API限流
            time.sleep(1)
        
        print(f"删除完成: 成功 {success_count}/{len(all_record_ids)} 条")
        return success_count

    def use_app_access_token(self):
        """尝试使用app_access_token而不是tenant_access_token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/"
        headers = {"Content-Type": "application/json"}
        data = {"app_id": self.app_id, "app_secret": self.app_secret}
        
        try:
            response = requests.post(url, headers=headers, json=data)
            result = response.json()
            
            if response.status_code == 200 and result.get("code") == 0:
                self.access_token = result.get("app_access_token")
                # 不保存到config文件，只在当前会话使用
                print("已切换到应用访问令牌")
                return True
            else:
                print(f"获取应用访问令牌失败: {result}")
                return False
        except Exception as e:
            print(f"请求应用访问令牌出错: {e}")
            return False

    def test_and_confirm_permissions(self):
        """测试并确认多维表格权限"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.bitable_id}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers)
            result = response.json()
            
            if response.status_code == 200 and result.get("code") == 0:
                app_name = result.get("data", {}).get("name", "未命名")
                print(f"✅ 权限验证成功! 可以访问多维表格: {app_name}")
                return True
            else:
                error_code = result.get("code")
                error_msg = result.get("msg", "未知错误")
                
                if error_code == 1254302:  # RolePermNotAllow
                    print("❌ 权限错误: 应用没有足够权限访问此多维表格")
                    print("请确保:")
                    print("1. 在飞书开发者平台为应用添加了 bitable:app:manage 权限")
                    print("2. 在多维表格中已将应用添加为协作者 (共享 -> 添加协作者 -> 搜索应用名称)")
                    print("3. 应用已更新并发布到最新版本")
                else:
                    print(f"❌ 访问错误 (代码: {error_code}): {error_msg}")
                
                return False
        except Exception as e:
            print(f"❌ 测试权限时出错: {e}")
            return False

def read_from_sqlite(db_file):
    """从SQLite数据库读取数据"""
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 读取问题反馈数据
    cursor.execute("SELECT * FROM issues")
    issues = []
    for row in cursor.fetchall():
        issue = {
            "date": row["date"],
            "issue_type": row["issue_type"],
            "description": row["description"],
            "urgency": row["urgency"],
            "completion": row["completion"],
            "status": row["status"],
            "negative_feedback": row["negative_feedback"]
        }
        issues.append(issue)
    
    # 读取销售数据
    cursor.execute("SELECT * FROM sales")
    sales = []
    for row in cursor.fetchall():
        sale = {
            "date": row["date"],
            "region": row["region"],
            "product": row["product"],
            "sales_count": row["sales_count"],
            "sales_amount": row["sales_amount"],
            "achievement_rate": row["achievement_rate"]
        }
        sales.append(sale)
    
    conn.close()
    
    return {
        "issues": issues,
        "sales": sales
    }

def main():
    parser = argparse.ArgumentParser(description='将SQLite数据上传到飞书多维表格')
    parser.add_argument('--db', default='customer_service.db', help='SQLite数据库文件路径')
    parser.add_argument('--config', default='feishu_config.json', help='飞书配置文件路径')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    
    args = parser.parse_args()
    
    # 启用调试模式
    if args.debug:
        import http.client as http_client
        http_client.HTTPConnection.debuglevel = 1
    
    # 从配置文件读取飞书应用凭证
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
            app_id = config.get('app_id')
            app_secret = config.get('app_secret')
            bitable_id = config.get('bitable_id')
            issues_table_id = config.get('issues_table_id')
            sales_table_id = config.get('sales_table_id')
            access_token = config.get('access_token')
    except Exception as e:
        print(f"读取配置文件时出错: {e}")
        return
    
    # 检查必要的参数
    if not app_id or not app_secret:
        print("错误: 必须提供飞书应用ID和密钥")
        return
    
    if not bitable_id:
        print("错误: 必须提供多维表格ID")
        return
    
    if not issues_table_id and not sales_table_id:
        print("错误: 必须至少提供一个表格ID")
        return
    
    # 读取SQLite数据
    try:
        print(f"正在从SQLite数据库读取数据: {args.db}")
        data = read_from_sqlite(args.db)
        print(f"成功读取数据: 问题反馈 {len(data['issues'])} 条, 销售数据 {len(data['sales'])} 条")
    except Exception as e:
        print(f"读取SQLite数据时出错: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 上传数据到飞书
    try:
        start_time = time.time()
        uploader = FeishuUploader(args.config)
        
        # 尝试使用应用访问令牌
        uploader.use_app_access_token()
        
        # 测试并确认多维表格权限
        if not uploader.test_and_confirm_permissions():
            return
        
        # 上传问题反馈数据
        issues_count = 0
        if issues_table_id and data["issues"]:
            issues_count = uploader.upload_issues_to_feishu(data["issues"])
        
        # 上传销售数据
        sales_count = 0
        if sales_table_id and data["sales"]:
            sales_count = uploader.upload_sales_to_feishu(data["sales"])
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "="*50)
        print("上传统计:")
        print(f"- 问题反馈: {issues_count}/{len(data['issues'])} 条")
        print(f"- 销售数据: {sales_count}/{len(data['sales'])} 条")
        print(f"- 总记录数: {issues_count + sales_count}/{len(data['issues']) + len(data['sales'])} 条")
        print(f"- 总耗时: {duration:.2f} 秒")
        print("="*50)
        
        print("\n数据上传完成!")
    except Exception as e:
        print(f"上传数据到飞书时出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 