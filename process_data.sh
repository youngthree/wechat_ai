#!/bin/bash

# 数据处理流程自动化脚本 - 后台定时版
# 每5分钟执行一次完整数据流：MySQL -> TXT -> JSON -> SQLite -> 飞书

# 设置日志文件
LOG_DIR="./logs"
mkdir -p $LOG_DIR
LOG_FILE="${LOG_DIR}/process_data_$(date '+%Y%m%d').log"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # 无颜色

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# 显示带颜色的标题同时写入日志
print_title() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
    log "=== $1 ==="
}

# 显示成功消息同时写入日志
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
    log "✓ $1"
}

# 显示错误消息同时写入日志
print_error() {
    echo -e "${RED}✗ $1${NC}"
    log "✗ $1"
}

# 显示警告消息同时写入日志
print_warning() {
    echo -e "${YELLOW}! $1${NC}"
    log "! $1"
}

# 执行单次数据处理流程
run_data_process() {
    # 显示脚本开始信息
    log "================================================"
    log "      客服数据处理流程自动化脚本 v2.0 (后台定时版)"
    log "================================================"
    log "开始执行处理流程"

    # 步骤1: 从MySQL导出数据到TXT
    print_title "步骤1: 从MySQL导出数据到TXT"
    log "执行: python mysql_to_txt.py"
    if python mysql_to_txt.py >> "$LOG_FILE" 2>&1; then
        print_success "MySQL数据成功导出到input.txt"
    else
        print_error "MySQL数据导出失败，错误代码: $?"
        return 1
    fi

    # 步骤2: 使用GPT处理TXT数据并生成JSON
    print_title "步骤2: 使用GPT处理TXT数据并生成JSON"
    log "执行: python text_to_json.py"
    if python text_to_json.py >> "$LOG_FILE" 2>&1; then
        print_success "TXT数据成功处理并生成output.json"
    else
        print_error "TXT数据处理失败，错误代码: $?"
        print_warning "本次流程中断，将在下次迭代重试"
        return 1
    fi

    # 步骤3: 将JSON数据导入到SQLite
    print_title "步骤3: 将JSON数据导入到SQLite"
    log "执行: python json_to_sqlite.py"
    if python json_to_sqlite.py >> "$LOG_FILE" 2>&1; then
        print_success "JSON数据成功导入到customer_service.db"
    else
        print_error "JSON数据导入失败，错误代码: $?"
        print_warning "本次流程中断，将在下次迭代重试"
        return 1
    fi

    # 步骤4: 将SQLite数据上传到飞书
    print_title "步骤4: 将SQLite数据上传到飞书"
    log "执行: python sqlite_to_feishu.py"
    if python sqlite_to_feishu.py --db customer_service.db --config feishu_config.json >> "$LOG_FILE" 2>&1; then
        print_success "SQLite数据成功上传到飞书"
    else
        print_error "SQLite数据上传失败，错误代码: $?"
        print_warning "飞书上传失败，但前面的步骤已完成"
        return 1
    fi

    # 显示完成信息
    log "================================================"
    log "      数据处理流程全部完成!"
    log "================================================"
    log "数据流: MySQL -> input.txt -> output.json -> customer_service.db -> 飞书"
    
    return 0
}

# 主循环函数 - 每5分钟执行一次
main_loop() {
    log "启动后台定时执行模式，每5分钟执行一次"
    
    while true; do
        # 记录迭代开始时间
        ITERATION_START=$(date '+%Y-%m-%d %H:%M:%S')
        log "开始新的迭代 - $ITERATION_START"
        
        # 执行数据处理流程
        run_data_process
        
        # 计算下次执行时间（5分钟后）
        log "本次执行完成，将在5分钟后再次执行"
        log "--------------------------------------------"
        
        # 休眠5分钟 (300秒)
        sleep 300
    done
}

# 检查是否以后台模式运行
if [ "$1" = "--daemon" ]; then
    # 以守护进程方式运行
    nohup bash "$0" --run-loop > /dev/null 2>&1 &
    echo "脚本已在后台启动，进程ID: $!"
    echo "日志文件: $LOG_FILE"
    exit 0
elif [ "$1" = "--run-loop" ]; then
    # 执行主循环
    main_loop
else
    # 显示使用说明
    echo "使用方法:"
    echo "  $0 --daemon    在后台运行脚本（每5分钟执行一次）"
    echo "  $0             显示此帮助信息"
    echo ""
    echo "脚本将创建日志文件: $LOG_FILE"
    exit 1
fi 