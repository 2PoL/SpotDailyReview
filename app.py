import io
import pandas as pd
import streamlit as st
from pathlib import Path
import re


st.set_page_config(page_title="数据处理工具", layout="wide")
st.title("📊 数据处理工具")

st.markdown("---")

# 侧边栏选择处理模式
with st.sidebar:
    st.header("选择处理模式")
    mode = st.radio(
        "",
        ["合并交易量价数据", "预处理边界数据"],
        label_visibility="collapsed"
    )

st.subheader(f"当前模式: {mode}")

st.markdown("---")


def extract_online_capacity(text):
    """从出清概况中提取在线机组容量"""
    if pd.isna(text):
        return None
    match = re.search(r'运行机组容量(\d+\.?\d*)\s*MW', str(text))
    if match:
        return float(match.group(1))
    return None


def process_trading_files(uploaded_files):
    """处理交易量价数据文件"""
    all_data = []

    for uploaded_file in uploaded_files:
        # 从文件名中提取公司名称
        company_name = Path(uploaded_file.name).stem.split("-")[0]

        try:
            # 读取Excel文件
            df = pd.read_excel(uploaded_file, sheet_name="1.交易量价数据信息", header=1)
            df["公司名称"] = company_name
            all_data.append(df)
        except Exception as e:
            st.error(f"处理文件 {uploaded_file.name} 时出错: {e}")

    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        return merged_df
    return None


def preprocess_boundary_files(files_dict):
    """预处理边界数据文件"""
    result = None

    try:
        # 1. 读取日前统调系统负荷预测
        if "日前统调系统负荷预测_REPORT0.xlsx" in files_dict:
            df_load = pd.read_excel(files_dict["日前统调系统负荷预测_REPORT0.xlsx"], header=0)
            df_load = df_load.iloc[1:].reset_index(drop=True)
            df_load['日期'] = pd.to_datetime(df_load.iloc[:, 1]).dt.date
            df_load['时点'] = df_load.iloc[:, 2].astype(str)
            df_load['省调负荷(MW)'] = pd.to_numeric(df_load.iloc[:, 3], errors='coerce')
        else:
            return None, "缺少文件: 日前统调系统负荷预测_REPORT0.xlsx"

        # 2. 读取日前新能源负荷预测
        if "日前新能源负荷预测_REPORT0.xlsx" in files_dict:
            df_renewable = pd.read_excel(files_dict["日前新能源负荷预测_REPORT0.xlsx"], header=0)
            df_renewable = df_renewable.iloc[1:].reset_index(drop=True)
            df_renewable['日期'] = pd.to_datetime(df_renewable.iloc[:, 1]).dt.date
            df_renewable['时点'] = df_renewable.iloc[:, 2].astype(str)
            df_renewable['风电(MW)'] = pd.to_numeric(df_renewable.iloc[:, 4], errors='coerce')
            df_renewable['光伏(MW)'] = pd.to_numeric(df_renewable.iloc[:, 5], errors='coerce')
            df_renewable['新能源负荷(MW)'] = pd.to_numeric(df_renewable.iloc[:, 3], errors='coerce')
        else:
            return None, "缺少文件: 日前新能源负荷预测_REPORT0.xlsx"

        # 3. 读取披露信息96点数据
        if "披露信息96点数据_REPORT0.xlsx" in files_dict:
            df_disclosure = pd.read_excel(files_dict["披露信息96点数据_REPORT0.xlsx"], header=0)
            df_disclosure = df_disclosure.iloc[1:].reset_index(drop=True)
            df_disclosure['日期'] = pd.to_datetime(df_disclosure.iloc[:, 1]).dt.date
            df_disclosure['时点'] = df_disclosure.iloc[:, 2].astype(str)
            df_disclosure['非市场化出力(MW)'] = pd.to_numeric(df_disclosure.iloc[:, 3], errors='coerce')
        else:
            return None, "缺少文件: 披露信息96点数据_REPORT0.xlsx"

        # 4. 读取日前联络线计划
        if "日前联络线计划_REPORT0.xlsx" in files_dict:
            df_tie_line = pd.read_excel(files_dict["日前联络线计划_REPORT0.xlsx"], header=0)
            df_tie_line = df_tie_line.iloc[1:].reset_index(drop=True)
            df_tie_line = df_tie_line[df_tie_line.iloc[:, 1] == '总加']
            df_tie_line['日期'] = pd.to_datetime(df_tie_line.iloc[:, 2]).dt.date
            df_tie_line['时点'] = df_tie_line.iloc[:, 3].astype(str)
            df_tie_line['联络线计划(MW)'] = pd.to_numeric(df_tie_line.iloc[:, 4], errors='coerce')
        else:
            return None, "缺少文件: 日前联络线计划_REPORT0.xlsx"

        # 5. 读取日前市场出清情况
        online_capacity = None
        if "日前市场出清情况_TABLE.xlsx" in files_dict:
            df_clearing = pd.read_excel(files_dict["日前市场出清情况_TABLE.xlsx"], header=0)
            df_clearing = df_clearing.iloc[1:].reset_index(drop=True)
            online_capacity = extract_online_capacity(df_clearing.iloc[0, 2])
        else:
            return None, "缺少文件: 日前市场出清情况_TABLE.xlsx"

        # 6. 读取日前水电计划
        if "日前水电计划发电总出力预测_REPORT0.xlsx" in files_dict:
            df_hydro = pd.read_excel(files_dict["日前水电计划发电总出力预测_REPORT0.xlsx"], header=0)
            df_hydro = df_hydro.iloc[1:].reset_index(drop=True)
            df_hydro['日期'] = pd.to_datetime(df_hydro.iloc[:, 1]).dt.date
            df_hydro['时点'] = df_hydro.iloc[:, 2].astype(str)
            df_hydro['水电出力(MW)'] = pd.to_numeric(df_hydro.iloc[:, 3], errors='coerce')
        else:
            return None, "缺少文件: 日前水电计划发电总出力预测_REPORT0.xlsx"

        # 7. 读取96点电网运行实际值
        if "96点电网运行实际值_REPORT0.xlsx" in files_dict:
            df_actual = pd.read_excel(files_dict["96点电网运行实际值_REPORT0.xlsx"], header=0)
            df_actual = df_actual.iloc[1:].reset_index(drop=True)
            df_actual['日期'] = pd.to_datetime(df_actual.iloc[:, 1]).dt.date
            df_actual['时点'] = df_actual.iloc[:, 2].astype(str)
            df_actual['省调负荷(MW)'] = pd.to_numeric(df_actual.iloc[:, 3], errors='coerce')
            df_actual['风电(MW)'] = pd.to_numeric(df_actual.iloc[:, 5], errors='coerce')
            df_actual['光伏(MW)'] = pd.to_numeric(df_actual.iloc[:, 6], errors='coerce')
            df_actual['新能源负荷(MW)'] = pd.to_numeric(df_actual.iloc[:, 7], errors='coerce')
            df_actual['水电出力(MW)'] = pd.to_numeric(df_actual.iloc[:, 8], errors='coerce')
            df_actual['非市场化出力(MW)'] = pd.to_numeric(df_actual.iloc[:, 11], errors='coerce')
        else:
            return None, "缺少文件: 96点电网运行实际值_REPORT0.xlsx"

        # 8. 读取实时联络线计划
        if "实时联络线计划_REPORT0.xlsx" in files_dict:
            df_tie_line_rt = pd.read_excel(files_dict["实时联络线计划_REPORT0.xlsx"], header=0)
            df_tie_line_rt = df_tie_line_rt.iloc[1:].reset_index(drop=True)
            df_tie_line_rt = df_tie_line_rt[df_tie_line_rt.iloc[:, 1] == '总加']
            df_tie_line_rt['日期'] = pd.to_datetime(df_tie_line_rt.iloc[:, 2]).dt.date
            df_tie_line_rt['时点'] = df_tie_line_rt.iloc[:, 3].astype(str)
            df_tie_line_rt['联络线计划(MW)'] = pd.to_numeric(df_tie_line_rt.iloc[:, 4], errors='coerce')
        else:
            return None, "缺少文件: 实时联络线计划_REPORT0.xlsx"

        # 9. 读取现货出清电价
        if "现货出清电价_REPORT0.xlsx" in files_dict:
            df_price = pd.read_excel(files_dict["现货出清电价_REPORT0.xlsx"])
            df_price = df_price[pd.to_numeric(df_price['序号'], errors='coerce').notna()]
            df_price['日期'] = pd.to_datetime(df_price['日期']).dt.date
            df_price['时点'] = df_price['时点'].astype(str)
            df_price['实时出清价格(元/MWh)'] = pd.to_numeric(df_price['实时出清价格(元/MWh)'], errors='coerce')
            df_price['日前出清价格(元/MWh)'] = pd.to_numeric(df_price['日前出清价格(元/MWh)'], errors='coerce')
        else:
            return None, "缺少文件: 现货出清电价_REPORT0.xlsx"

        # 合并所有日前数据
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

        day_ahead_data['边界数据类型'] = '日前'
        day_ahead_data['在线机组容量(MW)'] = online_capacity

        # 合并所有实时数据
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
        result_df = pd.concat([day_ahead_data, real_time_data], ignore_index=True)

        # 添加缺失的列
        columns = ['日期', '时点', '边界数据类型', '竞价空间(MW)', '省调负荷(MW)', '风电(MW)',
                   '光伏(MW)', '新能源负荷(MW)', '非市场化出力(MW)', '水电出力(MW)',
                   '联络线计划(MW)', '在线机组容量(MW)', '日前出清价格(元/MWh)',
                   '实时出清价格(元/MWh)', '负荷率(%)']

        for col in columns:
            if col not in result_df.columns:
                result_df[col] = None

        result_df = result_df[columns]

        # 排序
        result_df['时点_排序'] = pd.to_datetime(result_df['时点'], format='%H:%M', errors='coerce')
        result_df['边界数据类型_排序'] = result_df['边界数据类型'].map({'日前': 0, '实时': 1})
        result_df = result_df.sort_values(['边界数据类型_排序', '日期', '时点_排序']).reset_index(drop=True)
        result_df = result_df.drop(columns=['时点_排序', '边界数据类型_排序'])

        return result_df, None

    except Exception as e:
        return None, f"处理出错: {str(e)}"


def to_excel(df):
    """将DataFrame转换为Excel文件字节流"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name="合并数据")
    return output


# ==================== 模式1: 合并交易量价数据 ====================
if mode == "合并交易量价数据":
    st.markdown("### 📤 上传交易量价数据文件")
    st.info("请上传包含 '1.交易量价数据信息' sheet 的Excel文件，文件名格式如：公司名-电力营销信息统计日期.xlsx")

    uploaded_files = st.file_uploader(
        "选择Excel文件",
        type=['xlsx'],
        accept_multiple_files=True,
        help="支持多文件上传"
    )

    if uploaded_files:
        st.markdown(f"✅ 已选择 {len(uploaded_files)} 个文件：")
        for file in uploaded_files:
            st.write(f"  - {file.name}")

        if st.button("🔄 开始处理", type="primary"):
            with st.spinner("正在处理数据..."):
                result_df = process_trading_files(uploaded_files)

                if result_df is not None:
                    st.success("✅ 处理完成！")
                    st.session_state['trading_result'] = result_df
                    st.session_state['trading_filename'] = "合并交易量价数据.xlsx"

                    # 显示结果统计
                    st.markdown("### 📊 处理结果统计")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("总行数", len(result_df))
                    col2.metric("公司数量", result_df["公司名称"].nunique())
                    col3.metric("列数", len(result_df.columns))

                    # 显示数据预览
                    st.markdown("### 👀 数据预览")
                    st.dataframe(result_df.head(20), use_container_width=True)
                else:
                    st.error("❌ 处理失败，请检查文件格式")

    # 下载按钮（如果有结果）
    if 'trading_result' in st.session_state:
        st.markdown("---")
        st.markdown("### 📥 下载数据")
        excel_data = to_excel(st.session_state['trading_result'])
        st.download_button(
            label="📥 下载合并后的Excel文件",
            data=excel_data.getvalue(),
            file_name=st.session_state['trading_filename'],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


# ==================== 模式2: 预处理边界数据 ====================
else:
    st.markdown("### 📤 上传边界数据文件")
    st.warning("⚠️ 请上传以下9个必需的Excel文件：")
    st.markdown("""
    1. 日前统调系统负荷预测_REPORT0.xlsx
    2. 日前新能源负荷预测_REPORT0.xlsx
    3. 披露信息96点数据_REPORT0.xlsx
    4. 日前联络线计划_REPORT0.xlsx
    5. 日前市场出清情况_TABLE.xlsx
    6. 日前水电计划发电总出力预测_REPORT0.xlsx
    7. 96点电网运行实际值_REPORT0.xlsx
    8. 实时联络线计划_REPORT0.xlsx
    9. 现货出清电价_REPORT0.xlsx
    """)

    uploaded_files = st.file_uploader(
        "选择Excel文件（支持多选）",
        type=['xlsx'],
        accept_multiple_files=True,
        help="请上传上述9个必需文件"
    )

    if uploaded_files:
        st.markdown(f"✅ 已选择 {len(uploaded_files)} 个文件：")
        # 检查必需文件
        required_files = {
            "日前统调系统负荷预测_REPORT0.xlsx": False,
            "日前新能源负荷预测_REPORT0.xlsx": False,
            "披露信息96点数据_REPORT0.xlsx": False,
            "日前联络线计划_REPORT0.xlsx": False,
            "日前市场出清情况_TABLE.xlsx": False,
            "日前水电计划发电总出力预测_REPORT0.xlsx": False,
            "96点电网运行实际值_REPORT0.xlsx": False,
            "实时联络线计划_REPORT0.xlsx": False,
            "现货出清电价_REPORT0.xlsx": False
        }

        files_dict = {}
        for file in uploaded_files:
            files_dict[file.name] = file
            if file.name in required_files:
                required_files[file.name] = True
            st.write(f"  - {file.name}")

        missing_files = [name for name, found in required_files.items() if not found]
        if missing_files:
            st.warning(f"⚠️ 还缺少 {len(missing_files)} 个必需文件：")
            for name in missing_files:
                st.write(f"  - {name}")
        else:
            st.success("✅ 所有必需文件已上传！")

        if st.button("🔄 开始处理", type="primary", disabled=len(missing_files) > 0):
            with st.spinner("正在处理数据..."):
                result_df, error = preprocess_boundary_files(files_dict)

                if result_df is not None:
                    st.success("✅ 处理完成！")
                    st.session_state['boundary_result'] = result_df
                    st.session_state['boundary_filename'] = "预处理结果_新版.xlsx"

                    # 显示结果统计
                    st.markdown("### 📊 处理结果统计")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("总行数", len(result_df))
                    col2.metric("日前数据行数", len(result_df[result_df['边界数据类型'] == '日前']))
                    col3.metric("实时数据行数", len(result_df[result_df['边界数据类型'] == '实时']))

                    # 显示在线机组容量
                    if '在线机组容量(MW)' in result_df.columns:
                        online_cap = result_df['在线机组容量(MW)'].dropna().iloc[0] if not result_df['在线机组容量(MW)'].dropna().empty else "未找到"
                        st.info(f"💡 提取到在线机组容量: {online_cap} MW")

                    # 显示数据预览
                    st.markdown("### 👀 数据预览")
                    st.dataframe(result_df.head(30), use_container_width=True)
                else:
                    st.error(f"❌ {error}")

    # 显示下载按钮（如果有结果）
    if 'boundary_result' in st.session_state:
        st.markdown("---")
        st.markdown("### 📥 下载预处理结果")
        excel_data = to_excel(st.session_state['boundary_result'])
        st.download_button(
            label="📥 下载预处理后的Excel文件",
            data=excel_data.getvalue(),
            file_name=st.session_state['boundary_filename'],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.markdown("---")
st.caption("💡 提示：上传的文件不会被永久保存，仅用于当前会话")
