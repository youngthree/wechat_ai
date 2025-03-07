#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import requests
import time
from datetime import datetime
import sys
import os

def update_feishu_token(config_path="feishu_config.json"):
    """更新飞书租户访问令牌并保存到配置文件"""
    print(f"开始更新飞书访问令牌 (配置文件: {config_path})...")
    
    # 加载配置文件
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        app_id = config.get('app_id')
        app_secret = config.get('app_secret')
        
        if not app_id or not app_secret:
            print("错误: 配置文件中缺少应用ID或密钥")
            return False
            
    except Exception as e:
        print(f"读取配置文件失败: {e}")
        return False
    
    # 请求新令牌
    try:
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
        headers = {"Content-Type": "application/json"}
        data = {"app_id": app_id, "app_secret": app_secret}
        
        print("正在请求新的访问令牌...")
        response = requests.post(url, headers=headers, json=data)
        result = response.json()
        
        if response.status_code == 200 and result.get("code") == 0:
            # 更新令牌和过期时间
            new_token = result.get("tenant_access_token")
            expires_in = result.get("expire", 7200)  # 默认2小时
            
            # 更新配置
            config["access_token"] = new_token
            config["token_expires_at"] = time.time() + expires_in - 60  # 提前60秒过期以避免边界问题
            config["token_updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 保存配置
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=4)
                
            print(f"访问令牌已更新!")
            print(f"新令牌: {new_token[:10]}...{new_token[-10:]}")
            print(f"有效期: {expires_in} 秒")
            print(f"更新时间: {config['token_updated_at']}")
            print(f"过期时间: {datetime.fromtimestamp(config['token_expires_at']).strftime('%Y-%m-%d %H:%M:%S')}")
            return True
        else:
            print(f"获取访问令牌失败: {result}")
            return False
            
    except Exception as e:
        print(f"请求令牌过程中出错: {e}")
        return False

def check_token_status(config_path="feishu_config.json"):
    """检查当前令牌状态"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        token = config.get('access_token')
        expires_at = config.get('token_expires_at', 0)
        updated_at = config.get('token_updated_at', '未知')
        
        if not token:
            print("警告: 配置文件中没有访问令牌")
            return False
            
        # 检查令牌是否过期
        current_time = time.time()
        if current_time > expires_at:
            print(f"令牌状态: 已过期")
            print(f"上次更新: {updated_at}")
            print(f"过期时间: {datetime.fromtimestamp(expires_at).strftime('%Y-%m-%d %H:%M:%S')}")
            return False
        else:
            remaining = expires_at - current_time
            print(f"令牌状态: 有效")
            print(f"令牌: {token[:10]}...{token[-10:]}")
            print(f"上次更新: {updated_at}")
            print(f"剩余时间: {int(remaining//3600)}小时 {int((remaining%3600)//60)}分钟")
            return True
            
    except Exception as e:
        print(f"检查令牌状态时出错: {e}")
        return False

if __name__ == "__main__":
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='飞书访问令牌更新工具')
    parser.add_argument('--config', default='feishu_config.json', help='配置文件路径')
    parser.add_argument('--check', action='store_true', help='只检查令牌状态，不更新')
    parser.add_argument('--force', action='store_true', help='强制更新令牌，即使当前令牌仍然有效')
    
    args = parser.parse_args()
    
    # 检查令牌状态
    token_valid = check_token_status(args.config)
    
    # 根据参数决定是否更新令牌
    if args.check:
        # 只检查状态，不更新
        sys.exit(0 if token_valid else 1)
    elif args.force or not token_valid:
        # 强制更新或令牌已过期
        success = update_feishu_token(args.config)
        sys.exit(0 if success else 1)
    else:
        print("令牌仍然有效，无需更新")
        sys.exit(0)