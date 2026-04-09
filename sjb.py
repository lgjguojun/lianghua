import streamlit as st
import pandas as pd
import struct
import matplotlib.pyplot as plt
from pathlib import Path

st.set_page_config(page_title="量化挂单工作站 v5.4", layout="wide")

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

@st.cache_data
def load_data(symbol):
    path = Path(f"data/{symbol}.day")
    if not path.exists(): return None
    data = []
    try:
        with open(path, "rb") as f:
            while chunk := f.read(32):
                dt, o, h, l, c, _, _, _ = struct.unpack("<IIIIIfII", chunk)
                data.append({
                    "date": pd.to_datetime(str(dt)), 
                    "open": o/1000.0, "high": h/1000.0, 
                    "low": l/1000.0, "close": c/1000.0
                })
        return pd.DataFrame(data).set_index("date").sort_index()
    except:
        return None

def core_engine(df, n_days, buffer_pct, b_ratio, s_ratio, fee):
    # 核心修正：如果数据太短，直接返回初始状态
    if len(df) <= n_days:
        df_empty = df.copy()
        df_empty['equity'] = 1.0
        df_empty['raw_signal'] = 0
        return df_empty, pd.DataFrame()

    work_df = df.copy()
    # 计算突破信号
    work_df['rolling_max'] = work_df['close'].rolling(n_days).max()
    work_df['raw_signal'] = (work_df['close'] > work_df['rolling_max'] * (1 - buffer_pct)).astype(int)
    
    equity = [1.0] * len(work_df)
    pos, cash, units = 0, 1.0, 0
    details = []

    for i in range(1, len(work_df)):
        prev_c = work_df['close'].iloc[i-1]
        c_o, c_h, c_l, c_c = work_df['open'].iloc[i], work_df['high'].iloc[i], work_df['low'].iloc[i], work_df['close'].iloc[i]
        trade_msg = ""

        # 买入逻辑
        if pos == 0:
            if work_df['raw_signal'].iloc[i-1] == 1:
                b_px = prev_c * b_ratio
                if c_o <= b_px:
                    units = (cash * (1-fee)) / c_o; cash = 0; pos = 1
                    trade_msg = f"买入(低开):{c_o:.2f}"
                elif c_l <= b_px:
                    units = (cash * (1-fee)) / b_px; cash = 0; pos = 1
                    trade_msg = f"买入(挂单):{b_px:.2f}"
        
        # 卖出逻辑
        elif pos == 1:
            s_px = prev_c * s_ratio
            sold = False
            if c_o >= s_px:
                cash = units * c_o * (1-fee); units = 0; pos = 0; sold = True
                trade_msg = f"卖出(高开):{c_o:.2f}"
            elif c_h >= s_px:
                cash = units * s_px * (1-fee); units = 0; pos = 0; sold = True
                trade_msg = f"卖出(挂单):{s_px:.2f}"
            
            # 强制清仓逻辑 (信号消失)
            if not sold and work_df['raw_signal'].iloc[i] == 0:
                cash = units * c_c * (1-fee); units = 0; pos = 0
                trade_msg = f"卖出(强卖):{c_c:.2f}"
        
        equity[i] = cash if pos == 0 else units * c_c
        if trade_msg:
            details.append({"日期": work_df.index[i].date(), "动作": trade_msg, "总资产": round(equity[i], 3)})
    
    work_df['equity'] = equity
    return work_df, pd.DataFrame(details)

# --- 界面 ---
st.sidebar.header("📊 策略参数")
data_dir = Path("data")
stock_files = [f.stem for f in data_dir.glob("*.day")] if data_dir.exists() else []

n_days = st.sidebar.slider("回顾天数(N)", 1, 20, 3)
buffer_pct = st.sidebar.slider("缓冲区(%)", 0.0, 5.0, 1.0) / 100
buy_limit = st.sidebar.number_input("买入挂单比例", value=0.97, step=0.01)
sell_limit = st.sidebar.number_input("卖出挂单比例", value=1.03, step=0.01)
fee = st.sidebar.number_input("手续费率", value=0.0005, format="%.4f")

st.title("📈 A股挂单量化回测工作站")
tab1, tab2 = st.tabs(["🎯 单股分析", "🏆 全量排行"])

with tab1:
    selected_stock = st.selectbox("选择股票", stock_files)
    if selected_stock:
        raw_df = load_data(selected_stock)
        if raw_df is not None:
            processed_df, history = core_engine(raw_df, n_days, buffer_pct, buy_limit, sell_limit, fee)
            
            c1, c2, c3 = st.columns(3)
            final_ret = (processed_df['equity'].iloc[-1] - 1) * 100
            mdd = ((processed_df['equity'] / processed_df['equity'].cummax() - 1).min()) * 100
            
            c1.metric("累计收益率", f"{final_ret:.2f}%")
            c2.metric("最大回撤", f"{mdd:.2f}%")
            c3.metric("最后资产", f"{processed_df['equity'].iloc[-1]:.3f}")

            st.line_chart(processed_df['equity'])
            st.dataframe(history, use_container_width=True)

with tab2:
    if st.button("🚀 开始全量同步回测"):
        results = []
        progress_bar = st.progress(0)
        
        for i, symbol in enumerate(stock_files):
            df_single = load_data(symbol)
            if df_single is not None:
                # 核心修正：这里使用的参数必须与单股分析完全一致
                res_df, _ = core_engine(df_single, n_days, buffer_pct, buy_limit, sell_limit, fee)
                
                # 获取最终收益
                total_ret = res_df['equity'].iloc[-1] - 1
                results.append({"代码": symbol, "总收益": total_ret})
            
            progress_bar.progress((i + 1) / len(stock_files))
        
        ranking_df = pd.DataFrame(results).sort_values("总收益", ascending=False)
        
        # 验证是否存在负收益
        neg_count = len(ranking_df[ranking_df['总收益'] < 0])
        st.write(f"扫描完成！其中正收益: {len(ranking_df)-neg_count} 只，负收益: {neg_count} 只。")
        
        st.write("### 收益排行榜 (全样本)")
        st.dataframe(
            ranking_df.style.format({"总收益": "{:.2%}"}).background_gradient(cmap='RdYlGn', subset=['总收益']),
            use_container_width=True
        )