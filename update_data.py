import akshare as ak
import pandas as pd
import struct
import os
import time
from datetime import datetime, timedelta

# --- 配置区 ---
DATA_DIR = "data"
SYMBOL_COUNT = 100  # 抓取龙虎榜前100只股票
DAYS_NEEDED = 120    # 下载最近约4个月的数据，确保回测有足够缓冲

def get_lhb_list():
    """获取最新龙虎榜个股排行数据"""
    print("正在从东方财富接口获取龙虎榜名单...")
    try:
        # 获取近一月的龙虎榜个股统计数据
        lhb_df = ak.stock_lhb_stock_statistic_em(symbol="近一月")
        if lhb_df.empty:
            print("警告：未获取到龙虎榜数据")
            return []
        
        # 提取代码列，前100只
        symbols = lhb_df['代码'].head(SYMBOL_COUNT).tolist()
        print(f"成功获取 {len(symbols)} 只候选股票")
        return symbols
    except Exception as e:
        print(f"获取名单失败: {e}")
        return []

def save_as_day_file(df, symbol):
    """将数据保存为兼容原系统的32字节二进制格式"""
    # 确保文件夹存在
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
        
    file_path = os.path.join(DATA_DIR, f"{symbol}.day")
    
    try:
        with open(file_path, "wb") as f:
            for _, row in df.iterrows():
                # 转换日期格式为 YYYYMMDD 整数
                dt_str = row['date'].replace("-", "") if isinstance(row['date'], str) else row['date'].strftime("%Y%m%d")
                dt = int(dt_str)
                
                # 价格放大1000倍转为整数存储
                o = int(round(row['open'] * 1000))
                h = int(round(row['high'] * 1000))
                l = int(round(row['low'] * 1000))
                c = int(round(row['close'] * 1000))
                
                vol = int(row['volume'])
                amount = float(row['amount'])
                
                # 打包为二进制: 日期(I), 开(I), 高(I), 低(I), 收(I), 成交额(f), 成交量(I), 保留(I)
                data = struct.pack("<IIIIIfII", dt, o, h, l, c, amount, vol, 0)
                f.write(data)
        return True
    except Exception as e:
        print(f"文件保存失败 {symbol}: {e}")
        return False

def update_all():
    # 1. 创建目录
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)

    # 2. 获取名单
    symbols = get_lhb_list()
    if not symbols:
        return

    # 3. 设置起止日期
    start_date = (datetime.now() - timedelta(days=DAYS_NEEDED)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")
    
    success_count = 0
    print(f"准备下载从 {start_date} 到 {end_date} 的历史数据...")

    for i, symbol in enumerate(symbols):
        try:
            # 下载日线行情（前复权）
            df = ak.stock_zh_a_hist(
                symbol=symbol, 
                period="daily", 
                start_date=start_date, 
                end_date=end_date, 
                adjust="qfq"
            )
            
            if not df.empty:
                # 规范化列名
                df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']]
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
                
                if save_as_day_file(df, symbol):
                    success_count += 1
            
            # 打印进度（每10只显示一次）
            if (i + 1) % 10 == 0:
                print(f"进度: {i+1}/{len(symbols)} 完成")
                
            # 轻微延迟，防止触发接口频率限制
            time.sleep(0.1)
            
        except Exception as e:
            print(f"跳过 {symbol}: 可能是停牌或接口限制")
            continue
            
    print("-" * 30)
    print(f"任务结束 | 成功更新: {success_count} 只股票 | 目录: {DATA_DIR}")

if __name__ == "__main__":
    update_all()
