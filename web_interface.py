"""
Web可视化接口 - 使用Streamlit构建简单UI
"""
try:
    import streamlit as st
except ImportError:
    print("⚠️  Streamlit未安装，请运行: pip install streamlit")
    exit(1)

import pandas as pd
from datetime import datetime, timedelta
import query_api
import database


def main():
    st.set_page_config(
        page_title="A股连板追踪系统",
        page_icon="📈",
        layout="wide"
    )
    
    st.title("📈 A股连板高度追踪系统")
    st.markdown("---")
    
    # 侧边栏
    with st.sidebar:
        st.header("⚙️ 功能菜单")
        page = st.radio(
            "选择功能",
            ["市场概览", "涨停/炸板明细", "高连板查询", "个股分析", "历史统计"]
        )
    
    # 初始化API
    api = query_api.LimitQueryAPI()
    api.connect()
    
    # 获取可用日期
    conn = database.get_connection()
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT date FROM limit_analysis_result ORDER BY date DESC LIMIT 30",
        conn
    )
    conn.close()
    
    if dates_df.empty:
        st.warning("⚠️ 数据库中暂无数据，请先运行MVP流程生成数据")
        return
    
    available_dates = dates_df['date'].tolist()
    
    # 根据选择的页面显示不同内容
    if page == "市场概览":
        show_market_overview(api, available_dates)
    elif page == "涨停/炸板明细":
        show_daily_limit_details(api, available_dates)
    elif page == "高连板查询":
        show_high_chain_query(api, available_dates)
    elif page == "个股分析":
        show_stock_analysis(api, available_dates)
    elif page == "历史统计":
        show_historical_stats(api, available_dates)
    
    api.close()


def show_market_overview(api, available_dates):
    """市场概览页面"""
    st.header("市场概览")
    
    # 日期选择
    selected_date = st.selectbox("选择日期", available_dates)
    
    # 获取市场摘要
    summary = api.query_daily_summary(selected_date)
    
    # 显示关键指标
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("涨停总数", summary['total_limit'])
    with col2:
        st.metric("一字板", summary['yizi_count'])
    with col3:
        st.metric("炸板", summary['fried_count'])
    with col4:
        normal_limit = summary['total_limit'] - summary['yizi_count']
        st.metric("普通涨停", normal_limit)
    
    # 连板分布
    st.subheader("连板高度分布")
    
    if summary['chain_distribution']:
        dist_df = pd.DataFrame(summary['chain_distribution'])
        dist_df.columns = ['连板高度', '数量']
        
        # 使用柱状图显示
        st.bar_chart(dist_df.set_index('连板高度'))
        
        # 详细表格
        st.dataframe(dist_df, use_container_width=True)
    else:
        st.info("当日无连板数据")


def show_high_chain_query(api, available_dates):
    """高连板查询页面"""
    st.header("高连板股票查询")
    
    col1, col2 = st.columns(2)
    
    with col1:
        selected_date = st.selectbox("选择日期", available_dates)
    with col2:
        min_height = st.slider("最小连板高度", 1, 10, 2)
    
    # 查询高连板股票
    high_chain = api.query_high_chain_stocks(selected_date, min_height)
    
    if not high_chain.empty:
        st.success(f"找到 {len(high_chain)} 只股票")
        
        # 格式化显示
        display_df = high_chain[['code', 'name', 'chain_height', 'board_type', 
                                'change_pct', 'close']].copy()
        display_df.columns = ['代码', '名称', '连板高度', '板型', '涨幅%', '收盘价']
        
        # 添加颜色标记
        def highlight_chain(row):
            if row['连板高度'] >= 5:
                return ['background-color: #ffcccc'] * len(row)
            elif row['连板高度'] >= 3:
                return ['background-color: #ffffcc'] * len(row)
            else:
                return [''] * len(row)
        
        st.dataframe(
            display_df.style.apply(highlight_chain, axis=1),
            use_container_width=True,
            height=400
        )
        
        # 下载选项
        csv = display_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 下载CSV",
            data=csv,
            file_name=f'high_chain_{selected_date}.csv',
            mime='text/csv'
        )
    else:
        st.info(f"当日无 {min_height} 板及以上的股票")


def show_daily_limit_details(api, available_dates):
    """每日涨停/炸板明细页面"""
    st.header("每日涨停/炸板明细")
    
    # 日期选择
    selected_date = st.selectbox("选择日期", available_dates)
    
    # 摘要指标
    summary = api.query_daily_summary(selected_date)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("涨停总数", summary['total_limit'])
    with col2:
        st.metric("连板分布", f"{len(summary['chain_distribution'])}档")
    with col3:
        st.metric("炸板数", summary['fried_count'])
    
    # 查询明细
    limit_df = api.query_daily_limit_stocks(selected_date)
    fried_df = api.query_daily_fried_stocks(selected_date)
    
    tab1, tab2 = st.tabs(["涨停股", "炸板股"])
    
    with tab1:
        if not limit_df.empty:
            display_df = limit_df[['code', 'name', 'chain_height', 'board_type', 
                                   'is_fried', 'change_pct', 'close', 'volume']].copy()
            display_df.columns = ['代码', '名称', '连板高度', '板型', '是否炸板', '涨幅%', '收盘价', '成交量']
            st.dataframe(display_df, use_container_width=True, height=520)
            
            csv = display_df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 下载涨停股CSV",
                data=csv,
                file_name=f'limit_stocks_{selected_date}.csv',
                mime='text/csv'
            )
        else:
            st.info("当日无涨停股票")
    
    with tab2:
        if not fried_df.empty:
            display_df = fried_df[['code', 'name', 'chain_height', 'board_type', 
                                   'change_pct', 'close', 'volume']].copy()
            display_df.columns = ['代码', '名称', '连板高度', '板型', '涨幅%', '收盘价', '成交量']
            st.dataframe(display_df, use_container_width=True, height=520)
            
            csv = display_df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 下载炸板股CSV",
                data=csv,
                file_name=f'fried_stocks_{selected_date}.csv',
                mime='text/csv'
            )
        else:
            st.info("当日无炸板股票")


def show_stock_analysis(api, available_dates):
    """个股分析页面"""
    st.header("个股连板分析")
    
    # 股票搜索
    col1, col2 = st.columns([2, 1])
    
    with col1:
        keyword = st.text_input("输入股票代码或名称", "")
    
    if keyword:
        # 搜索股票
        search_results = api.search_stocks_by_name(keyword)
        
        if not search_results.empty:
            with col2:
                selected_stock = st.selectbox(
                    "选择股票",
                    search_results['code'] + ' - ' + search_results['name']
                )
            
            code = selected_stock.split(' - ')[0]
            
            # 显示股票基本信息
            stock_info = search_results[search_results['code'] == code].iloc[0]
            st.info(f"**{stock_info['name']}** ({code}) - {stock_info['board_type']}")
            
            # 查询历史最高连板
            max_chain = api.query_stock_max_chain(code)
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("历史最高连板", f"{max_chain['max_chain']}板")
            with col2:
                if max_chain['date']:
                    st.metric("最高连板日期", max_chain['date'])
            
            # 查询连板历史
            st.subheader("连板历史记录")
            
            # 日期范围选择
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.selectbox("开始日期", available_dates[-10:], index=0)
            with col2:
                end_date = st.selectbox("结束日期", available_dates, index=0)
            
            history = api.query_stock_chain_history(code, start_date, end_date)
            
            if not history.empty:
                # 只显示有连板的记录
                chain_records = history[history['chain_height'] > 0]
                
                if not chain_records.empty:
                    display_df = chain_records[['date', 'chain_height', 'board_type', 
                                               'close', 'volume']].copy()
                    display_df.columns = ['日期', '连板高度', '板型', '收盘价', '成交量']
                    
                    st.dataframe(display_df, use_container_width=True)
                    
                    # 连板高度趋势图
                    st.line_chart(history.set_index('date')['chain_height'])
                else:
                    st.info("该时间范围内无连板记录")
            else:
                st.warning("未找到历史数据")
        else:
            st.warning("未找到匹配的股票")


def show_historical_stats(api, available_dates):
    """历史统计页面"""
    st.header("历史统计分析")
    
    conn = database.get_connection()
    
    # 每日涨停数量趋势
    st.subheader("每日涨停数量趋势")
    
    trend_query = """
    SELECT 
        date,
        SUM(CASE WHEN limit_status = 1 THEN 1 ELSE 0 END) as limit_count,
        SUM(CASE WHEN is_fried = 1 THEN 1 ELSE 0 END) as fried_count
    FROM limit_analysis_result
    GROUP BY date
    ORDER BY date
    """
    
    trend_df = pd.read_sql_query(trend_query, conn)
    
    if not trend_df.empty:
        trend_df['date'] = pd.to_datetime(trend_df['date'], format='%Y%m%d')
        trend_df = trend_df.set_index('date')
        
        st.line_chart(trend_df[['limit_count', 'fried_count']])
    
    # 高连板股票统计
    st.subheader("高连板股票排行（历史Top 20）")
    
    top_query = """
    SELECT 
        l.code,
        s.name,
        l.date,
        l.chain_height
    FROM limit_analysis_result l
    LEFT JOIN stock_meta s ON l.code = s.code
    WHERE l.chain_height >= 3
    ORDER BY l.chain_height DESC, l.date DESC
    LIMIT 20
    """
    
    top_df = pd.read_sql_query(top_query, conn)
    
    if not top_df.empty:
        top_df.columns = ['代码', '名称', '日期', '连板高度']
        st.dataframe(top_df, use_container_width=True)
    
    conn.close()


if __name__ == '__main__':
    main()
