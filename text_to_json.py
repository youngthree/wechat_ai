#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import requests
from datetime import datetime
import sys
import math

# 固定的输入和输出文件
INPUT_FILE = 'input.txt'
OUTPUT_FILE = 'output.json'

class TextProcessor:
    def __init__(self, api_key=None, endpoint=None, deployment=None, api_version=None):
        """初始化处理器，设置Azure OpenAI API参数"""
        # 优先使用传入的参数，否则使用环境变量或默认值
        self.api_key = api_key or os.environ.get("AZURE_API_KEY_GPT4", "de7dd2fbb8404f08ad04ac22d515df87")
        self.endpoint = endpoint or os.environ.get("AZURE_ENDPOINT_GPT4", "https://edgenesis-openai-sc-01.openai.azure.com/openai")
        self.deployment = deployment or os.environ.get("AZURE_DEPLOYMENT_GPT4", "gpt-4o")
        self.api_version = api_version or os.environ.get("AZURE_API_VERSION_GPT4", "2024-08-01-preview")
        
        # 构建完整的API URL
        self.api_url = f"{self.endpoint}/deployments/{self.deployment}/chat/completions?api-version={self.api_version}"
        
        print(f"API URL: {self.api_url}")
        
        # 设置批处理参数
        self.max_tokens_per_request = 4000
        self.max_input_tokens = 100000  # 设置一个非常大的值，实际上不限制输入大小
        self.chunk_size = 50  # 每个批次处理的行数
    
    def process_text_in_batches(self, text_data, company_name="新文蓄电池"):
        """分批处理大量文本数据"""
        # 将文本分割成行
        lines = text_data.strip().split('\n')
        total_lines = len(lines)
        print(f"总共读取了 {total_lines} 行数据")
        
        # 如果行数很少，直接处理
        if total_lines <= self.chunk_size:
            return self.process_text(text_data, company_name)
        
        # 计算需要的批次数
        num_batches = math.ceil(total_lines / self.chunk_size)
        print(f"数据将分为 {num_batches} 批处理")
        
        # 初始化结果
        all_issues = []
        all_sales = []
        
        # 分批处理
        for i in range(num_batches):
            start_idx = i * self.chunk_size
            end_idx = min((i + 1) * self.chunk_size, total_lines)
            batch_lines = lines[start_idx:end_idx]
            batch_text = '\n'.join(batch_lines)
            
            print(f"处理第 {i+1}/{num_batches} 批 (行 {start_idx+1} 到 {end_idx})")
            
            # 处理当前批次
            batch_result = self.process_text(batch_text, company_name)
            
            # 检查结果是否有效
            if isinstance(batch_result, dict) and not batch_result.get("error"):
                # 合并结果
                all_issues.extend(batch_result.get("issues", []))
                all_sales.extend(batch_result.get("sales", []))
                print(f"第 {i+1} 批处理完成，累计: issues={len(all_issues)}, sales={len(all_sales)}")
            else:
                print(f"第 {i+1} 批处理失败: {batch_result.get('error', '未知错误')}")
        
        # 合并所有批次的结果
        combined_result = {
            "issues": all_issues,
            "sales": all_sales,
            "metadata": {
                "total_records": len(all_issues) + len(all_sales),
                "total_issues": len(all_issues),
                "total_sales": len(all_sales),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source_file": INPUT_FILE,
                "batches_processed": num_batches
            }
        }
        
        print(f"所有批次处理完成，总计: issues={len(all_issues)}, sales={len(all_sales)}")
        return combined_result
    
    def process_text(self, text_data, company_name="新文蓄电池"):
        """使用Azure OpenAI处理文本数据并返回结构化JSON"""
        # 构建提示词
        prompt = self._build_prompt(text_data, company_name)
        
        # 准备请求头和请求体
        headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key
        }
        
        payload = {
            "messages": [
                {"role": "system", "content": "你是一个专业的数据分析助手，擅长从文本中提取结构化信息并生成符合特定格式的JSON数据。"},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,
            "max_tokens": self.max_tokens_per_request,
            "response_format": {"type": "json_object"}
        }
        
        print("正在调用Azure OpenAI API处理文本...")
        
        # 发送请求到Azure OpenAI
        try:
            response = requests.post(self.api_url, headers=headers, json=payload)
            
            # 检查HTTP状态码
            if response.status_code != 200:
                error_msg = f"API请求失败，HTTP状态码: {response.status_code}"
                try:
                    error_details = response.json()
                    if "error" in error_details:
                        error_msg += f"\n错误信息: {error_details['error'].get('message', '未知错误')}"
                        error_msg += f"\n错误代码: {error_details['error'].get('code', '未知代码')}"
                        
                        # 提供常见错误的解决建议
                        if "rate limit" in error_msg.lower():
                            error_msg += "\n建议: API请求频率过高，请降低请求频率或等待一段时间后重试。"
                        elif "authentication" in error_msg.lower() or "api key" in error_msg.lower():
                            error_msg += "\n建议: 请检查API密钥是否正确，或者API密钥是否已过期。"
                        elif "quota" in error_msg.lower():
                            error_msg += "\n建议: 您的API配额可能已用尽，请检查账户余额或提高配额限制。"
                        elif "content filter" in error_msg.lower():
                            error_msg += "\n建议: 输入内容可能触发了内容过滤器，请检查并修改输入文本。"
                except:
                    error_msg += f"\n无法解析错误详情: {response.text[:200]}..."
                
                print(error_msg)
                return {"error": error_msg}
            
            # 解析响应
            try:
                response_data = response.json()
                
                # 检查API返回的JSON结构是否符合预期
                if "choices" not in response_data or len(response_data["choices"]) == 0:
                    error_msg = "API返回的数据格式不符合预期，缺少'choices'字段"
                    print(error_msg)
                    return {"error": error_msg, "raw_response": response_data}
                
                if "message" not in response_data["choices"][0]:
                    error_msg = "API返回的数据格式不符合预期，缺少'message'字段"
                    print(error_msg)
                    return {"error": error_msg, "raw_response": response_data}
                
                if "content" not in response_data["choices"][0]["message"]:
                    error_msg = "API返回的数据格式不符合预期，缺少'content'字段"
                    print(error_msg)
                    return {"error": error_msg, "raw_response": response_data}
                
                json_response = response_data["choices"][0]["message"]["content"]
                
                # 检查是否有截断或不完整的情况
                if "finish_reason" in response_data["choices"][0]:
                    finish_reason = response_data["choices"][0]["finish_reason"]
                    if finish_reason != "stop":
                        print(f"警告: API响应可能不完整，完成原因: {finish_reason}")
                        if finish_reason == "length":
                            print("建议: 响应被截断，请考虑增加max_tokens参数或减少输入文本长度。")
                
                print("API调用成功，正在解析JSON响应...")
                
                try:
                    structured_data = json.loads(json_response)
                    
                    # 验证返回的JSON结构是否符合预期
                    if "issues" not in structured_data or "sales" not in structured_data:
                        print("警告: 返回的JSON缺少预期的'issues'或'sales'字段")
                    
                    return structured_data
                except json.JSONDecodeError as e:
                    # 如果返回的不是有效JSON，尝试提取JSON部分
                    import re
                    json_match = re.search(r'```json\n(.*?)\n```', json_response, re.DOTALL)
                    if json_match:
                        try:
                            structured_data = json.loads(json_match.group(1))
                            return structured_data
                        except json.JSONDecodeError as e2:
                            error_msg = f"无法解析提取的JSON: {str(e2)}"
                            print(error_msg)
                            return {"error": error_msg, "extracted_text": json_match.group(1)}
                    
                    # 如果仍然失败，返回详细的错误信息
                    error_msg = f"无法解析API返回的JSON: {str(e)}"
                    print(error_msg)
                    
                    # 尝试提供更有用的错误信息
                    if "Expecting property name" in str(e):
                        print("可能是JSON格式错误，缺少属性名或引号")
                    elif "Expecting value" in str(e):
                        print("可能是JSON格式错误，缺少值或逗号")
                    
                    # 显示问题部分的上下文
                    error_pos = e.pos
                    context_start = max(0, error_pos - 50)
                    context_end = min(len(json_response), error_pos + 50)
                    error_context = json_response[context_start:context_end]
                    print(f"错误上下文: ...{error_context}...")
                    print(f"错误位置: 第{error_pos}个字符")
                    
                    return {"error": error_msg, "raw_response": json_response}
                    
            except Exception as e:
                error_msg = f"解析API响应时出错: {str(e)}"
                print(error_msg)
                return {"error": error_msg, "raw_response": response.text[:500]}
                
        except requests.exceptions.RequestException as e:
            error_msg = f"API请求失败: {str(e)}"
            print(error_msg)
            
            # 提供网络相关错误的解决建议
            if "ConnectionError" in str(e.__class__):
                print("建议: 网络连接问题，请检查您的网络连接或API端点URL是否正确。")
            elif "Timeout" in str(e.__class__):
                print("建议: 请求超时，服务器可能繁忙，请稍后重试或增加超时时间。")
            elif "TooManyRedirects" in str(e.__class__):
                print("建议: 重定向次数过多，请检查API端点URL是否正确。")
            
            return {"error": error_msg}
    
    def _build_prompt(self, text_data, company_name):
        """构建提示词，指导Azure OpenAI如何处理文本"""
        prompt = f"""
请分析以下来自{company_name}经销商的客服聊天内容和反馈消息，并提取关键信息生成两个结构化的JSON数据，分别对应问题反馈表和销售数据表。

文本内容包含多种格式：
1. 客户与客服的对话（格式如"客户1: 内容"，"客服: 内容"）
2. 经销商反馈消息，可能有以下几种格式：
   - 带序号的格式：如"1. [日期 时间] 经销商-联系人：问题描述"
   - 不带序号的格式：如"[日期 时间] 经销商-联系人：问题描述"
   - 可能存在格式不规范的行，如多条消息合并在一行或有额外空格
   - 可能包含id、user_id、time、message等字段的格式

请仔细分析每一行内容，识别其格式并提取相关信息。对于经销商反馈消息，请特别注意：
- 从日期时间中提取日期（格式为YYYY/MM/DD）
- 从经销商名称中识别区域（华东、华南、华北、华西、华中等）
- 从问题描述中识别产品型号、问题类型和紧急程度

请按照以下格式输出两个JSON数组：

1. 问题反馈表(issues)：提取所有客户和经销商反馈的问题
   - date: 日期，格式为"YYYY/MM/DD"
   - issue_type: 问题类型，限定为以下几种：订单、物流、产品、技术支持、售后、其他
   - description: 问题描述
   - urgency: 紧急程度，限定为：高、中、低（根据描述中的紧急词语判断，如"急用"、"催单"为高，一般问题为中，咨询类为低）
   - completion: 完成度，0-100的整数（默认为0）
   - status: 处理状态，限定为：处理中、未处理、已处理（默认为"未处理"）
   - negative_feedback: 是否为负面反馈，限定为：是、否（根据内容判断，如质量问题、延误、故障等为"是"）

2. 销售数据表(sales)：提取所有销售相关数据
   - date: 日期，格式为"YYYY/MM/DD"
   - region: 区域，限定为：华东、华南、华北、华西、华中
   - product_model: 产品型号
   - quantity: 销售数量，整数
   - amount: 销售金额，整数
   - completion_rate: 达成率，0-100的整数

特别注意：
1. 对于格式为"[日期 时间] 经销商-联系人：问题描述"的行，应识别为经销商反馈，提取为issues
2. 对于包含销量、采购等词语的内容，应提取为sales
3. 对于格式不规范的行（如多条消息合并在一行），请尝试分割并单独处理每条消息
4. 请确保每条有效信息都被解析，不要忽略任何格式的内容
5. 请注意生成和文本中有效信息数量相同的JSON记录，如果判断是无效信息可以忽略
6. 对于格式为"id:XX user_id:XX time:XX message:XX"的行，应提取message作为问题描述，time作为时间

请确保输出的JSON格式正确，包含两个数组：issues和sales。

以下是需要分析的文本内容：

{text_data}

请返回完整的JSON格式数据，不要包含任何其他解释或说明。
"""
        return prompt
    
    def save_json(self, data, output_path):
        """将JSON数据保存到文件"""
        print(f"准备保存JSON数据到: {output_path}")
        
        # 统计并打印JSON记录数量
        if isinstance(data, dict):
            issues_count = len(data.get("issues", []))
            sales_count = len(data.get("sales", []))
            total_count = issues_count + sales_count
            print(f"生成的JSON记录数量: issues={issues_count}, sales={sales_count}, 总计={total_count}")
        
        try:
            # 使用'w'模式打开文件，这会覆盖现有内容
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # 验证文件是否成功写入
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                print(f"JSON数据已成功保存到: {output_path}")
                print(f"文件大小: {os.path.getsize(output_path)} 字节")
            else:
                raise Exception("文件写入后大小为0或不存在")
            
        except Exception as e:
            print(f"保存JSON数据时出错: {str(e)}")
            
            # 尝试保存到当前目录
            fallback_path = os.path.basename(output_path)
            print(f"尝试保存到备用位置: {fallback_path}")
            
            try:
                with open(fallback_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"JSON数据已保存到备用位置: {fallback_path}")
            except Exception as e2:
                print(f"保存到备用位置也失败: {str(e2)}")
            
                # 最后尝试使用临时文件名
                temp_path = f"temp_{int(datetime.now().timestamp())}.json"
                try:
                    with open(temp_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"JSON数据已保存到临时文件: {temp_path}")
                except Exception as e3:
                    print(f"所有保存尝试均失败: {str(e3)}")

def main():
    """主函数"""
    print(f"开始处理文件: {INPUT_FILE}")
    
    # 读取输入文件
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            text_data = f.read()
        print(f"成功读取文件: {INPUT_FILE}")
        print(f"文件大小: {os.path.getsize(INPUT_FILE)} 字节")
        
        # 计算行数
        line_count = text_data.count('\n') + 1
        print(f"文件包含 {line_count} 行数据")
    except FileNotFoundError:
        print(f"错误: 找不到文件 '{INPUT_FILE}'")
        return
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return
    
    # 处理文本
    try:
        processor = TextProcessor()
        
        print("开始处理文本数据...")
        # 使用分批处理方法处理大量数据
        result = processor.process_text_in_batches(text_data)
        
        # 添加处理时间戳
        if isinstance(result, dict) and "metadata" not in result:
            result["metadata"] = {}
        
        if isinstance(result, dict) and "metadata" in result:
            result["metadata"]["generation_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 保存结果
        processor.save_json(result, OUTPUT_FILE)
        
        # 验证输出文件
        if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
            print(f"验证成功: 输出文件 {OUTPUT_FILE} 已创建并包含数据")
            
            # 统计并显示记录数量
            issues_count = len(result.get("issues", []))
            sales_count = len(result.get("sales", []))
            total_count = issues_count + sales_count
            
            print("\n" + "="*50)
            print(f"处理完成! 总共生成 {total_count} 条记录:")
            print(f"- 问题反馈: {issues_count} 条")
            print(f"- 销售数据: {sales_count} 条")
            print("="*50 + "\n")
        else:
            print(f"警告: 输出文件 {OUTPUT_FILE} 不存在或为空")
        
    except Exception as e:
        print(f"处理文本时出错: {e}")
        import traceback
        traceback.print_exc()

# 简单测试函数
def test_api_connection():
    """测试API连接是否正常工作"""
    processor = TextProcessor()
    test_text = "客户1: 您好，我是华东区的经销商，我们昨天订购的DX12电池还没有收到，物流显示延误了三天。"
    
    print("正在测试API连接...")
    result = processor.process_text(test_text)
    
    if result and not result.get("error"):
        print("API连接测试成功!")
        return True
    else:
        print("API连接测试失败!")
        return False

if __name__ == "__main__":
    # 如果带--test参数，则运行测试
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_api_connection()
    else:
        main() 