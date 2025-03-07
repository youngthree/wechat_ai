# 项目名称

本项目主要用于演示如何安装依赖并运行脚本 `process_data.sh`。下面将介绍项目的依赖安装、环境准备以及脚本运行方式等信息。

---

## 一、环境准备

在开始之前，请确保你所使用的环境中已经安装了以下软件或工具：

- Python 3.x
- pip（Python 包管理器）
- Bash 或兼容的 Shell 环境（用于运行 `process_data.sh`）

---

## 二、安装依赖

linux请按照以下步骤在终端（或命令行）中执行安装命令：

```bash
# 安装第三方 Python 库
pip install requests
pip install pandas
pip install mysql-connector-python
pip install selenium
pip install beautifulsoup4
pip install sqlite
sudo apt-get install sqlite3
pip install pandas openpyxl
pip install nltk

##三、运行程序
##1.赋予脚本执行权限
chmod +x process_data.sh

./process_data.sh --daemon
   
  ## 停止脚本：
   ##找到脚本进程ID
   ps aux | grep process_data.sh
   
   # 终止进程
   kill [进程ID]
   
   确认进程中止：记得结束杀进程
   
   if pgrep -f "process_data.sh --run-loop" > /dev/null; then
    echo "进程仍在运行！"
   else
    echo "进程已终止"
   fi
