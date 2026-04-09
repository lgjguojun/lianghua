import akshare as ak
import pandas as pd
import struct
import os
import time
import random
from datetime import datetime, timedelta

# --- 路径配置：确保在 GitHub Actions 下也能找到正确位置 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# --- 抓取配置 ---
SYMBOL_COUNT = 80  # 稍微减少数量，提高成功率
DAYS_NEEDED = 120    

def get_lhb_list():
    print("开始获取龙虎榜名单...")
    try:
        # 尝试获取近一月数据
        lhb_df = ak.stock_lhb_stock_statistic_em(symbol="近一月")
        if lhb_df.empty:
            return []
        # 过滤掉北交所(920/8/4开头)，这些容易报错导致接口封锁
        symbols = [s for s in lhb_df['代码'].tolist() if not s.startswith(('9', '8', '4'))]
        return symbols[:SYMBOL_COUNT]
    except Exception as e:
        print(f"获取名单异常: {e}")
        return []

def save_as_day_file(df, symbol):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    
    file_path = os.path.join(DATA_DIR, f"{symbol}.day")
    try:
        with open(file_path, "wb") as f:
            for _, row in df.iterrows():
                dt = int(row['date'].replace("-", ""))
                # 转换价格并打包
                o, h, l, c = [int(round(row[x] * 1000)) for x in ['open', 'high', 'low', 'close']]
                vol, amount = int(row['volume']), float(row['amount'])
                data = struct.pack("<IIIIIfII", dt, o, h, l, c, amount, vol, 0)
                f.write(data)
        return True
    except:
        return False

def update_all():
    print(f"当前工作目录: {os.getcwd()}")
    print(f"数据保存目录: {DATA_DIR}")
    
    symbols = get_lhb_list()
    if not symbols:
        print("名单为空，退出任务")
        return

    start_date = (datetime.now() - timedelta(days=DAYS_NEEDED)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")
    
    success_count = 0
    for i, symbol in enumerate(symbols):
        try:
            # 下载日线，增加重试
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if not df.empty:
                df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']]
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
                if save_as_day_file(df, symbol):
                    success_count += 1
            
            # 关键：每 5 只股票随机多休息一会儿，防止 IP 被封
            time.sleep(random.uniform(0.5, 1.2))
            if (i + 1) % 5 == 0:
                print(f"已处理 {i+1}/{len(symbols)}...")
                time.sleep(random.uniform(1, 3))
                
        except Exception as e:
            print(f"处理 {symbol} 失败: {e}")
            continue

    print("-" * 30)
    print(f"任务完成！成功抓取 {success_count} 个文件")
    # 列出生成的文件，方便在日志中确认
    if os.path.exists(DATA_DIR):
        print(f"目录下的文件数量: {len(os.listdir(DATA_DIR))}")

if __name__ == "__main__":
    update_all()
