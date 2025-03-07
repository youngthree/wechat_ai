一、环境准备
安装python
二、安装依赖

   # 核心依赖
pip install requests
   # 数据处理依赖
pip install pandas   
   # MySQL连接器 (适用于mysql_to_txt.py)
pip install mysql-connector-python 
   # JSON处理
   pip install simplejson
brew install sqlite
sudo apt-get install sqlite3
pip install pandas openpyxl
pip install nltk

三、运行程序
   chmod +x process_data.sh
   ./process_data.sh --daemon
   
   停止脚本：
      # 找到脚本进程ID
   ps aux | grep process_data.sh
   
   # 终止进程
   kill [进程ID]
   
   确认进程中止：
   
   if pgrep -f "process_data.sh --run-loop" > /dev/null; then
    echo "进程仍在运行！"
else
    echo "进程已终止"
fi
