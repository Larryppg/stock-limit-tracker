"""
A股连板高度追踪系统 - 主入口
"""
import argparse
from datetime import datetime
import batch_processor


def main():
    parser = argparse.ArgumentParser(description='A股连板高度追踪系统')
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['mvp', 'backfill', 'daily', 'test'],
        default='mvp',
        help='运行模式: mvp(MVP验证) / backfill(全量回填) / daily(每日更新) / test(测试)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='MVP模式下获取的天数（默认90天）'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='回填模式的起始日期 YYYYMMDD'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='回填模式的结束日期 YYYYMMDD'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='每日更新的目标日期 YYYYMMDD（默认今天）'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("     A股连板高度追踪系统")
    print("="*60 + "\n")
    
    if args.mode == 'mvp':
        print(f"运行模式: MVP验证（最近{args.days}天）\n")
        batch_processor.run_mvp_pipeline(recent_days=args.days)
        
    elif args.mode == 'backfill':
        print("运行模式: 全量历史回填\n")
        batch_processor.run_full_backfill(
            start_date=args.start_date,
            end_date=args.end_date
        )
        
    elif args.mode == 'daily':
        print("运行模式: 每日增量更新\n")
        batch_processor.run_daily_update(target_date=args.date)
        
    elif args.mode == 'test':
        print("运行模式: 测试\n")
        test_algorithms()
    
    print("\n程序执行完毕。")


def test_algorithms():
    """测试核心算法"""
    import pandas as pd
    import limit_calculator
    
    print("测试连板计算算法...\n")
    
    # 构造测试数据：5天3板 -> 断链 -> 2天2板
    test_data = pd.DataFrame({
        'date': ['20240102', '20240103', '20240104', '20240105', '20240108', '20240109', '20240110'],
        'code': ['000001'] * 7,
        'open': [10.0, 11.0, 12.1, 13.31, 13.0, 14.3, 15.73],
        'high': [11.0, 12.1, 13.31, 13.5, 14.3, 15.73, 17.0],
        'low': [10.0, 11.0, 12.1, 13.0, 13.0, 14.3, 15.5],
        'close': [11.0, 12.1, 13.31, 13.2, 14.3, 15.73, 16.0],
        'pre_close': [10.0, 11.0, 12.1, 13.31, 13.0, 14.3, 15.73],
        'volume': [1000000] * 7,
        'amount': [10000000] * 7
    })
    
    print("测试数据:")
    print(test_data[['date', 'close', 'pre_close']])
    print()
    
    # 计算连板（10%涨跌幅）
    result = limit_calculator.calculate_single_stock_chain(test_data, '000001', 0.10)
    
    print("连板分析结果:")
    print(result)
    print()
    
    # 验证结果
    expected_heights = [1, 2, 3, 0, 1, 2, 0]
    actual_heights = result['chain_height'].tolist()
    
    if actual_heights == expected_heights:
        print("✓ 测试通过！连板计算逻辑正确")
    else:
        print("✗ 测试失败！")
        print(f"预期: {expected_heights}")
        print(f"实际: {actual_heights}")


if __name__ == '__main__':
    main()
