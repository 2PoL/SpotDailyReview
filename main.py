import os
import pandas as pd
from pathlib import Path


def main():
    # 设置data文件夹路径
    data_dir = Path("data")

    # 存储所有数据的列表
    all_data = []

    # 遍历data文件夹中的所有Excel文件
    for file_path in data_dir.glob("*.xlsx"):
        # 从文件名中提取公司名称（例如：河津-电力营销信息统计1.10-20260112(1).xlsx）
        company_name = file_path.stem.split("-")[0]

        try:
            # 读取Excel文件中的"1.交易量价数据信息"sheet，header=1表示第二行是表头
            df = pd.read_excel(file_path, sheet_name="1.交易量价数据信息", header=1)

            # 添加公司名称列
            df["公司名称"] = company_name

            # 添加到列表中
            all_data.append(df)

            print(f"已处理: {company_name}")
        except Exception as e:
            print(f"处理文件 {file_path.name} 时出错: {e}")

    if all_data:
        # 合并所有数据
        merged_df = pd.concat(all_data, ignore_index=True)

        # 保存到新的Excel文件
        output_path = "合并交易量价数据.xlsx"
        merged_df.to_excel(output_path, index=False, sheet_name="交易量价数据汇总")

        print(f"\n合并完成！共处理 {len(all_data)} 个公司的数据")
        print(f"总行数: {len(merged_df)}")
        print(f"输出文件: {output_path}")
        print(f"\n各公司数据行数统计:")
        print(merged_df["公司名称"].value_counts().sort_index())
    else:
        print("未找到任何数据！")


if __name__ == "__main__":
    main()
