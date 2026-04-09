import akshare as ak
import pandas as pd
import struct
import os
import time
import random
from datetime import datetime, timedelta

# 1. 路径锁定：确保 data 文件夹创建在脚本同级目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def update_all():
    # 确保文件夹存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
    
    print(f"工作目录: {os.getcwd()}")
    print(f"数据保存路径: {DATA_DIR}")

    # 2. 获取龙虎榜名单（使用详情接口更稳定）
    try:
        print("正在获取龙虎榜名单...")
        lhb_df = ak.stock_lhb_detail_em()
        if lhb_df.empty:
            print("无法获取名单，接口返回空")
            return
        
        # 去重并过滤北交所/三板 (9, 8, 4开头)
        all_symbols = lhb_df['代码'].unique().tolist()
        symbols = [s for s in all_symbols if not s.startswith(('9', '8', '4'))][:60] 
        print(f"成功锁定 {len(symbols)} 只目标活跃股")
    except Exception as e:
        print(f"获取名单异常: {e}")
        return

    # 3. 下载历史数据
    start_date = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
    success_count = 0

    for symbol in symbols:
        try:
            # 抓取日线
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, adjust="qfq")
            
            if not df.empty:
                # 转换格式并保存
                file_path = os.path.join(DATA_DIR, f"{symbol}.day")
                with open(file_path, "wb") as f:
                    for _, row in df.iterrows():
                        # 日期 2024-01-01 -> 20240101
                        dt = int(row['日期'].replace("-", ""))
                        # 价格放大1000倍转整数
                        o, h, l, c = [int(round(row[x] * 1000)) for x in ['开盘', '最高', '最低', '收盘']]
                        vol, amt = int(row['成交量']), float(row['成交额'])
                        # 打包二进制 (通达信兼容格式)
                        data = struct.pack("<IIIIIfII", dt, o, h, l, c, amt, vol, 0)
                        f.write(data)
                success_count += 1
                print(f"已同步: {symbol}")
            
            # 随机延迟防止封锁
            time.sleep(random.uniform(0.6, 1.2))
        except Exception as e:
            print(f"跳过 {symbol}: {e}")
            continue

    print("-" * 30)
    print(f"抓取结束！成功保存 {success_count} 个文件到 {DATA_DIR}")
    print(f"目录内文件列表: {os.listdir(DATA_DIR)}")

if __name__ == "__main__":
    update_all()
