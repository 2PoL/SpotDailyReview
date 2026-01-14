import pandas as pd
import re
from pathlib import Path
from datetime import datetime


def extract_online_capacity(text):
    """从出清概况中提取在线机组容量"""
    if pd.isna(text):
        return None
    # 匹配 "运行机组容量42340.00MW"
    match = re.search(r'运行机组容量(\d+\.?\d*)\s*MW', str(text))
    if match:
        return float(match.group(1))
    return None


def preprocess_data():
    """预处理所有数据文件"""
    data_dir = Path("margin_data")

    # 初始化结果DataFrame
    result_df = pd.DataFrame()

    # 1. 读取日前统调系统负荷预测_REPORT0 - D列 -> 省调负荷(MW) E列
    print("处理日前统调系统负荷预测...")
    df_load = pd.read_excel(data_dir / "日前统调系统负荷预测_REPORT0.xlsx", header=0)
    # 从第2行开始读取数据（跳过表头行）
    df_load = df_load.iloc[1:].reset_index(drop=True)
    df_load['日期'] = pd.to_datetime(df_load.iloc[:, 1]).dt.date
    df_load['时点'] = df_load.iloc[:, 2].astype(str)
    df_load['省调负荷(MW)'] = pd.to_numeric(df_load.iloc[:, 3], errors='coerce')

    # 2. 读取日前新能源负荷预测_REPORT0 - E,F列 -> F,G列, D列 -> H列
    print("处理日前新能源负荷预测...")
    df_renewable = pd.read_excel(data_dir / "日前新能源负荷预测_REPORT0.xlsx", header=0)
    df_renewable = df_renewable.iloc[1:].reset_index(drop=True)
    df_renewable['日期'] = pd.to_datetime(df_renewable.iloc[:, 1]).dt.date
    df_renewable['时点'] = df_renewable.iloc[:, 2].astype(str)
    df_renewable['风电(MW)'] = pd.to_numeric(df_renewable.iloc[:, 4], errors='coerce')
    df_renewable['光伏(MW)'] = pd.to_numeric(df_renewable.iloc[:, 5], errors='coerce')
    df_renewable['新能源负荷(MW)'] = pd.to_numeric(df_renewable.iloc[:, 3], errors='coerce')

    # 3. 读取披露信息96点数据_REPORT0 - D列 -> 非市场化出力(MW) I列
    print("处理披露信息96点数据...")
    df_disclosure = pd.read_excel(data_dir / "披露信息96点数据_REPORT0.xlsx", header=0)
    df_disclosure = df_disclosure.iloc[1:].reset_index(drop=True)
    df_disclosure['日期'] = pd.to_datetime(df_disclosure.iloc[:, 1]).dt.date
    df_disclosure['时点'] = df_disclosure.iloc[:, 2].astype(str)
    df_disclosure['非市场化出力(MW)'] = pd.to_numeric(df_disclosure.iloc[:, 3], errors='coerce')

    # 4. 读取日前联络线计划_REPORT0 - E列 -> 联络线计划(MW) K列
    print("处理日前联络线计划...")
    df_tie_line = pd.read_excel(data_dir / "日前联络线计划_REPORT0.xlsx", header=0)
    df_tie_line = df_tie_line.iloc[1:].reset_index(drop=True)
    df_tie_line = df_tie_line[df_tie_line.iloc[:, 1] == '总加']  # 只取总加行
    df_tie_line['日期'] = pd.to_datetime(df_tie_line.iloc[:, 2]).dt.date
    df_tie_line['时点'] = df_tie_line.iloc[:, 3].astype(str)
    df_tie_line['联络线计划(MW)'] = pd.to_numeric(df_tie_line.iloc[:, 4], errors='coerce')

    # 5. 读取日前市场出清情况_TABLE - 提取在线机组容量 -> L列
    print("处理日前市场出清情况...")
    df_clearing = pd.read_excel(data_dir / "日前市场出清情况_TABLE.xlsx", header=0)
    df_clearing = df_clearing.iloc[1:].reset_index(drop=True)
    # 提取在线机组容量（只有一个值，应用到所有行）
    online_capacity = extract_online_capacity(df_clearing.iloc[0, 2])
    print(f"  提取到在线机组容量: {online_capacity} MW")

    # 7. 读取日前水电计划发电总出力预测_REPORT0 - D列 -> 水电出力(MW) J列
    print("处理日前水电计划...")
    df_hydro = pd.read_excel(data_dir / "日前水电计划发电总出力预测_REPORT0.xlsx", header=0)
    df_hydro = df_hydro.iloc[1:].reset_index(drop=True)
    df_hydro['日期'] = pd.to_datetime(df_hydro.iloc[:, 1]).dt.date
    df_hydro['时点'] = df_hydro.iloc[:, 2].astype(str)
    df_hydro['水电出力(MW)'] = pd.to_numeric(df_hydro.iloc[:, 3], errors='coerce')

    # 8. 读取96点电网运行实际值_REPORT0 - 实时数据
    print("处理96点电网运行实际值...")
    df_actual = pd.read_excel(data_dir / "96点电网运行实际值_REPORT0.xlsx", header=0)
    df_actual = df_actual.iloc[1:].reset_index(drop=True)
    df_actual['日期'] = pd.to_datetime(df_actual.iloc[:, 1]).dt.date
    df_actual['时点'] = df_actual.iloc[:, 2].astype(str)
    df_actual['省调负荷(MW)'] = pd.to_numeric(df_actual.iloc[:, 3], errors='coerce')
    df_actual['风电(MW)'] = pd.to_numeric(df_actual.iloc[:, 5], errors='coerce')
    df_actual['光伏(MW)'] = pd.to_numeric(df_actual.iloc[:, 6], errors='coerce')
    df_actual['新能源负荷(MW)'] = pd.to_numeric(df_actual.iloc[:, 7], errors='coerce')
    df_actual['水电出力(MW)'] = pd.to_numeric(df_actual.iloc[:, 8], errors='coerce')
    df_actual['非市场化出力(MW)'] = pd.to_numeric(df_actual.iloc[:, 11], errors='coerce')

    # 9. 读取实时联络线计划_REPORT0 - E列 -> 联络线计划(MW) K列（实时）
    print("处理实时联络线计划...")
    df_tie_line_rt = pd.read_excel(data_dir / "实时联络线计划_REPORT0.xlsx", header=0)
    df_tie_line_rt = df_tie_line_rt.iloc[1:].reset_index(drop=True)
    df_tie_line_rt = df_tie_line_rt[df_tie_line_rt.iloc[:, 1] == '总加']  # 只取总加行
    df_tie_line_rt['日期'] = pd.to_datetime(df_tie_line_rt.iloc[:, 2]).dt.date
    df_tie_line_rt['时点'] = df_tie_line_rt.iloc[:, 3].astype(str)
    df_tie_line_rt['联络线计划(MW)'] = pd.to_numeric(df_tie_line_rt.iloc[:, 4], errors='coerce')

    # 10. 读取现货出清电价_REPORT0 - 实时和日前出清价格
    print("处理现货出清电价...")
    df_price = pd.read_excel(data_dir / "现货出清电价_REPORT0.xlsx")
    # 过滤掉均价汇总行（序号不是数字的行）
    df_price = df_price[pd.to_numeric(df_price['序号'], errors='coerce').notna()]
    df_price['日期'] = pd.to_datetime(df_price['日期']).dt.date
    df_price['时点'] = df_price['时点'].astype(str)
    df_price['实时出清价格(元/MWh)'] = pd.to_numeric(df_price['实时出清价格(元/MWh)'], errors='coerce')
    df_price['日前出清价格(元/MWh)'] = pd.to_numeric(df_price['日前出清价格(元/MWh)'], errors='coerce')

    # 合并所有日前数据
    print("合并日前数据...")
    day_ahead_data = pd.merge(
        df_load[['日期', '时点', '省调负荷(MW)']],
        df_renewable[['日期', '时点', '风电(MW)', '光伏(MW)', '新能源负荷(MW)']],
        on=['日期', '时点'],
        how='outer'
    )
    day_ahead_data = pd.merge(
        day_ahead_data,
        df_disclosure[['日期', '时点', '非市场化出力(MW)']],
        on=['日期', '时点'],
        how='outer'
    )
    day_ahead_data = pd.merge(
        day_ahead_data,
        df_tie_line[['日期', '时点', '联络线计划(MW)']],
        on=['日期', '时点'],
        how='outer'
    )
    day_ahead_data = pd.merge(
        day_ahead_data,
        df_hydro[['日期', '时点', '水电出力(MW)']],
        on=['日期', '时点'],
        how='outer'
    )
    day_ahead_data = pd.merge(
        day_ahead_data,
        df_price[['日期', '时点', '日前出清价格(元/MWh)']],
        on=['日期', '时点'],
        how='outer'
    )

    # 添加边界数据类型和在线机组容量
    day_ahead_data['边界数据类型'] = '日前'
    day_ahead_data['在线机组容量(MW)'] = online_capacity

    # 合并所有实时数据
    print("合并实时数据...")
    real_time_data = df_actual[['日期', '时点', '省调负荷(MW)', '风电(MW)', '光伏(MW)',
                                  '新能源负荷(MW)', '水电出力(MW)', '非市场化出力(MW)']].copy()
    real_time_data = pd.merge(
        real_time_data,
        df_tie_line_rt[['日期', '时点', '联络线计划(MW)']],
        on=['日期', '时点'],
        how='left'
    )
    real_time_data = pd.merge(
        real_time_data,
        df_price[['日期', '时点', '实时出清价格(元/MWh)']],
        on=['日期', '时点'],
        how='left'
    )
    real_time_data['边界数据类型'] = '实时'

    # 合并日前和实时数据
    print("合并所有数据...")
    result_df = pd.concat([day_ahead_data, real_time_data], ignore_index=True)

    # 添加缺失的列（如果需要）
    columns = ['日期', '时点', '边界数据类型', '竞价空间(MW)', '省调负荷(MW)', '风电(MW)',
               '光伏(MW)', '新能源负荷(MW)', '非市场化出力(MW)', '水电出力(MW)',
               '联络线计划(MW)', '在线机组容量(MW)', '日前出清价格(元/MWh)',
               '实时出清价格(元/MWh)', '负荷率(%)']

    # 确保所有列都存在
    for col in columns:
        if col not in result_df.columns:
            result_df[col] = None

    # 重新排序列
    result_df = result_df[columns]

    # 排序：先按边界数据类型排序（日前在前，实时在后），再按日期和时点排序
    # 这样可以保证：所有日前数据在前（按时点），然后所有实时数据在后（按时点）
    result_df['时点_排序'] = pd.to_datetime(result_df['时点'], format='%H:%M', errors='coerce')
    result_df['边界数据类型_排序'] = result_df['边界数据类型'].map({'日前': 0, '实时': 1})
    result_df = result_df.sort_values(['边界数据类型_排序', '日期', '时点_排序']).reset_index(drop=True)
    result_df = result_df.drop(columns=['时点_排序', '边界数据类型_排序'])

    return result_df


def main():
    """主函数"""
    print("=" * 50)
    print("开始数据预处理")
    print("=" * 50)

    # 预处理数据
    result_df = preprocess_data()

    # 保存结果
    output_path = "预处理结果_新版.xlsx"
    result_df.to_excel(output_path, index=False, sheet_name="预处理数据")

    print("\n" + "=" * 50)
    print("预处理完成！")
    print(f"输出文件: {output_path}")
    print(f"总行数: {len(result_df)}")
    print("\n数据预览:")
    print(result_df.head(20).to_string())
    print("\n数据统计:")
    print(f"  日前数据行数: {len(result_df[result_df['边界数据类型'] == '日前'])}")
    print(f"  实时数据行数: {len(result_df[result_df['边界数据类型'] == '实时'])}")
    print("=" * 50)


if __name__ == "__main__":
    main()