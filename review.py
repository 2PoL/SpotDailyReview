"""
电力交易量价数据分析脚本

根据用户筛选的价格区间，计算以下指标：
1. 日前小时数
2. 实时小时数
3. 日前价格均价
4. 实时价格均价
5. 省间价格均价
6. 省间中标电量
7. 日前中标电量
8. 实际出力电量
9. 中长期持仓电量均值
10. 中长期持仓加权均价
"""

import re
import pandas as pd
from typing import Tuple
from datetime import datetime


class PowerDataAnalyzer:
    """电力交易数据分析类"""

    def __init__(self, excel_path: str):
        """
        初始化数据分析器

        Args:
            excel_path: Excel文件路径
        """
        self.excel_path = excel_path
        self.power_conversion_factors = {
            '同华': 660/660,
            '塔山': 660/600,
            '阳高': 660/350,
            '同达': 660/330,
            '王坪': 660/200,
            '蒲洲': 660/350,
            '河津': 660/350,
            '临汾': 660/300,
            '侯马': 660/300,
        }
        self.default_power_conversion_factor = 1.0
        self.df = None
        self.load_data()

    def _get_power_conversion_factor(self, company_name: str | None) -> float:
        if company_name is None:
            return self.default_power_conversion_factor
        return self.power_conversion_factors.get(company_name, self.default_power_conversion_factor)

    def load_data(self):
        """加载Excel数据"""
        self.df = pd.read_excel(self.excel_path, sheet_name='交易量价数据汇总')
        self._add_unit_dimension_column()
        print(f"数据加载完成，共 {len(self.df)} 行 {len(self.df.columns)} 列")

    def _add_unit_dimension_column(self):
        """根据机组名称添加机组维度列"""
        if '机组名称' not in self.df.columns:
            print("警告: 数据中不包含'机组名称'列，无法生成机组维度")
            return

        def determine_dimension(unit_name: str) -> str:
            if pd.isna(unit_name):
                return '未知'

            match = re.search(r'(\d+)', str(unit_name))
            if not match:
                return '未知'

            unit_no = int(match.group(1))
            if unit_no in (1, 3):
                return '机组1&3组'
            if unit_no in (2, 4):
                return '机组2&4组'
            return '其他机组'

        self.df['机组维度'] = self.df['机组名称'].apply(determine_dimension)

    def filter_by_price_range(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        price_column: str = '省间日前出清价格',
        start_date: str | None = None,
        end_date: str | None = None,
        date_column: str = '日期',
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> pd.DataFrame:
        """
        根据价格区间和日期范围筛选数据

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            price_column: 用于筛选的价格列名
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            date_column: 用于筛选的日期列名
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称（机组1&3组/机组2&4组），用于合并筛选
            include_min_boundary: 是否包含最小价格边界值（默认False，使用>；True使用>=）
            include_max_boundary: 是否包含最大价格边界值（默认False，使用<；True使用<=）

        Returns:
            筛选后的DataFrame
        """
        filtered = self.df.copy()

        # 价格筛选
        if min_price is not None:
            if include_min_boundary:
                filtered = filtered[filtered[price_column] >= min_price]
            else:
                filtered = filtered[filtered[price_column] > min_price]

        if max_price is not None:
            if include_max_boundary:
                filtered = filtered[filtered[price_column] <= max_price]
            else:
                filtered = filtered[filtered[price_column] < max_price]

        # 日期筛选
        if date_column in filtered.columns:
            dates = pd.to_datetime(filtered[date_column], errors='coerce')

            if start_date is not None:
                start_ts = pd.to_datetime(start_date)
                mask = dates >= start_ts
                filtered = filtered[mask]
                dates = dates[mask]

            if end_date is not None:
                end_ts = pd.to_datetime(end_date)
                dates = pd.to_datetime(filtered[date_column], errors='coerce')
                mask = dates <= end_ts
                filtered = filtered[mask]
        elif start_date is not None or end_date is not None:
            print(f"警告: 日期列 '{date_column}' 不存在，无法应用日期筛选")

        # 公司筛选
        if company_name is not None and '公司名称' in filtered.columns:
            filtered = filtered[filtered['公司名称'] == company_name]

        # 机组筛选
        if unit_name is not None and '机组名称' in filtered.columns:
            filtered = filtered[filtered['机组名称'] == unit_name]

        # 机组维度筛选
        if unit_dimension is not None:
            if '机组维度' in filtered.columns:
                filtered = filtered[filtered['机组维度'] == unit_dimension]
            else:
                print("警告: 数据中不包含'机组维度'列，无法应用机组维度筛选")

        return filtered

    def calculate_daily_forward_hours(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        1. 日前小时数
        基于日前出清节点价格筛选，计算筛选后的行数/4（每行代表15分钟）

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            小时数
        """
        filtered = self.filter_by_price_range(min_price, max_price, '日前出清节点价格', start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)
        hours = len(filtered) / 4  # 每行15分钟，4行=1小时
        return hours

    def calculate_realtime_hours(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        2. 实时小时数
        基于日内出清节点统计小时数，价格区间仍按日前出清节点价格筛选

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            小时数
        """
        filtered = self.filter_by_price_range(
            min_price,
            max_price,
            '日前出清节点价格',
            start_date,
            end_date,
            company_name=company_name,
            unit_name=unit_name,
            unit_dimension=unit_dimension,
            include_min_boundary=include_min_boundary,
            include_max_boundary=include_max_boundary
        )

        if '日内出清节点价格' in filtered.columns:
            if min_price is not None:
                if include_min_boundary:
                    filtered = filtered[filtered['日内出清节点价格'] >= min_price]
                else:
                    filtered = filtered[filtered['日内出清节点价格'] > min_price]

            if max_price is not None:
                if include_max_boundary:
                    filtered = filtered[filtered['日内出清节点价格'] <= max_price]
                else:
                    filtered = filtered[filtered['日内出清节点价格'] < max_price]

        hours = len(filtered) / 4
        return hours

    def calculate_daily_forward_avg_price(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        3. 日前价格均价
        筛选价格区间内的日前节点均价

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            平均价格
        """
        filtered = self.filter_by_price_range(min_price, max_price, '日前出清节点价格', start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)
        avg_price = filtered['日前出清节点价格'].mean()
        return avg_price

    def calculate_realtime_avg_price(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        4. 实时价格均价
        价格筛选沿用日前出清节点价格，但均价来自日内出清节点价格

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            平均价格
        """
        filtered = self.filter_by_price_range(min_price, max_price, '日前出清节点价格', start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)
        avg_price = filtered['日内出清节点价格'].mean()
        return avg_price

    def calculate_inter_provincial_avg_price(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        price_column: str = '日前出清节点价格',
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        5. 省间价格均价
        计算公式：(省间日前电量 * 省间日前价格 + 省间实时电量 * 省间实时价格)
                  / (省间实时电量 + 省间日前电量)

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            price_column: 用于筛选的价格列名
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            加权平均价格
        """
        filtered = self.filter_by_price_range(min_price, max_price, price_column, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)

        # 提取相关列
        power_daily = filtered['省间日前出清电力'].fillna(0)
        price_daily = filtered['省间日前出清价格'].fillna(0)
        power_realtime = filtered['省间实时出清电力'].fillna(0)
        price_realtime = filtered['省间实时出清价格'].fillna(0)

        # 计算加权平均
        numerator = (power_daily * price_daily + power_realtime * price_realtime).sum()
        denominator = (power_realtime + power_daily).sum()

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def calculate_inter_provincial_power(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        price_column: str = '日前出清节点价格',
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        6. 省间中标电量
        筛选价格区间内的省间日前出清电力与省间实时出清电力求和

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            price_column: 用于筛选的价格列名
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            省间中标电量总和
        """
        filtered = self.filter_by_price_range(min_price, max_price, price_column, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)

        if {'省间日前出清电力', '省间实时出清电力'}.issubset(filtered.columns):
            total_power = (
                filtered['省间日前出清电力'].fillna(0) +
                filtered['省间实时出清电力'].fillna(0)
            ).sum()
        elif '省间中标总量' in filtered.columns:
            total_power = filtered['省间中标总量'].sum()
        else:
            print("警告: 数据中没有省间电力列，无法计算省间中标电量")
            return 0.0

        factor = self._get_power_conversion_factor(company_name)
        return total_power * factor

    def calculate_daily_forward_power(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        7. 日前中标电量
        筛选价格区间内的日前中标电量求和

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            日前中标电量平均值
        """
        filtered = self.filter_by_price_range(min_price, max_price, '日前出清节点价格', start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)
        if filtered.empty:
            return 0.0

        hours = len(filtered) / 4  # 每行15分钟，4行=1小时
        if hours == 0:
            return 0.0

        total_power = filtered['日前中标出力'].sum() / 4  # 换算成小时电量
        factor = self._get_power_conversion_factor(company_name)
        return (total_power * factor) / hours

    def calculate_actual_output_power(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        8. 实际出力电量
        筛选价格区间内的实际中标电量求和

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            实际出力电量平均值
        """
        filtered = self.filter_by_price_range(min_price, max_price, '日前出清节点价格', start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)
        if filtered.empty:
            return 0.0

        hours = len(filtered) / 4
        if hours == 0:
            return 0.0

        total_power = filtered['日内实际出力'].sum() / 4  # 换算成小时电量
        factor = self._get_power_conversion_factor(company_name)
        return (total_power * factor) / hours

    def calculate_medium_long_avg_power(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        price_column: str = '日前出清节点价格',
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        9. 中长期持仓电量均值
        先计算每个时点（省内中长期上网电量 + 省间中长期上网电量），再根据筛选条件将这些值累加

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            price_column: 用于筛选的价格列名
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            满足条件的中长期持仓电量平均值
        """
        filtered = self.filter_by_price_range(min_price, max_price, price_column, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)
        if filtered.empty:
            return 0.0

        hours = len(filtered) / 4
        if hours == 0:
            return 0.0

        # 计算每个时点的中长期持仓电量
        medium_long_power = (
            filtered['省内中长期上网电量'].fillna(0) +
            filtered['省间中长期上网电量'].fillna(0)
        )

        factor = self._get_power_conversion_factor(company_name)
        return (medium_long_power.sum() * factor) / hours

    def calculate_medium_long_weighted_avg_price(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        price_column: str = '日前出清节点价格',
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> float:
        """
        10. 中长期持仓加权均价
        先计算每个时点：
        (省内中长期上网电量 * 省内中长期均价 + 省间中长期上网电量 * 省间中长期均价)
        再根据筛选价格区间内计算均值

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            price_column: 用于筛选的价格列名
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度名称，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            中长期持仓加权均价
        """
        filtered = self.filter_by_price_range(min_price, max_price, price_column, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary)

        # 计算每个时点的中长期加权价格
        intra_power = filtered['省内中长期上网电量'].fillna(0)
        intra_price = filtered['省内中长期均价'].fillna(0)
        inter_power = filtered['省间中长期上网电量'].fillna(0)
        inter_price = filtered['省间中长期均价'].fillna(0)

        weighted_price = intra_power * intra_price + inter_power * inter_price
        total_power = intra_power + inter_power

        numerator = weighted_price.sum()
        denominator = total_power.sum()

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def analyze_all_metrics(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        company_name: str | None = None,
        unit_name: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> dict:
        """
        计算所有指标

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            company_name: 公司名称，用于筛选特定公司
            unit_name: 机组名称，用于筛选特定机组
            unit_dimension: 机组维度，用于筛选合并后的机组
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            包含所有指标的字典
        """
        results = {
            '1. 日前小时数': self.calculate_daily_forward_hours(min_price, max_price, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '2. 实时小时数': self.calculate_realtime_hours(min_price, max_price, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '3. 日前价格均价': self.calculate_daily_forward_avg_price(min_price, max_price, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '4. 实时价格均价': self.calculate_realtime_avg_price(min_price, max_price, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '5. 省间价格均价': self.calculate_inter_provincial_avg_price(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '6. 省间中标电量': self.calculate_inter_provincial_power(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '7. 日前中标电量': self.calculate_daily_forward_power(min_price, max_price, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '8. 实际出力电量': self.calculate_actual_output_power(min_price, max_price, start_date, end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '9. 中长期持仓电量均值': self.calculate_medium_long_avg_power(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            '10. 中长期持仓加权均价': self.calculate_medium_long_weighted_avg_price(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company_name, unit_name=unit_name, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
        }

        return results

    def print_results(self, results: dict):
        """
        打印分析结果

        Args:
            results: 分析结果字典
        """
        print("\n" + "=" * 60)
        print("电力交易量价数据分析结果")
        print("=" * 60)

        for key, value in results.items():
            print(f"{key:20s}: {value:>15.4f}")

        print("=" * 60)

    def export_results(self, results: dict, output_path: str = '分析结果.xlsx'):
        """
        导出分析结果到Excel

        Args:
            results: 分析结果字典
            output_path: 输出文件路径
        """
        # 转换为DataFrame
        df = pd.DataFrame.from_dict(results, orient='index', columns=['数值'])

        # 导出到Excel
        df.to_excel(output_path)
        print(f"\n分析结果已导出到: {output_path}")

    def analyze_all_metrics_by_company(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        unit_dimension: str | None = None,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> pd.DataFrame:
        """
        按公司分组的指标分析

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            unit_dimension: 机组维度过滤（可选）
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            按公司分组的指标DataFrame
        """
        if '公司名称' not in self.df.columns:
            print("警告: 数据中不包含'公司名称'列，无法按公司分组")
            return pd.DataFrame()

        companies = self.df['公司名称'].unique()
        results_list = []

        for company in companies:
            # 为 计算所有指标
            results = {
                '公司名称': company,
                '1. 日前小时数': self.calculate_daily_forward_hours(min_price, max_price, start_date, end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '2. 实时小时数': self.calculate_realtime_hours(min_price, max_price, start_date, end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '3. 日前价格均价': self.calculate_daily_forward_avg_price(min_price, max_price, start_date, end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '4. 实时价格均价': self.calculate_realtime_avg_price(min_price, max_price, start_date, end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '5. 省间价格均价': self.calculate_inter_provincial_avg_price(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '6. 省间中标电量': self.calculate_inter_provincial_power(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '7. 日前中标电量': self.calculate_daily_forward_power(min_price, max_price, start_date, end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '8. 实际出力电量': self.calculate_actual_output_power(min_price, max_price, start_date, end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '9. 中长期持仓电量均值': self.calculate_medium_long_avg_power(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
                '10. 中长期持仓加权均价': self.calculate_medium_long_weighted_avg_price(min_price, max_price, start_date=start_date, end_date=end_date, company_name=company, unit_dimension=unit_dimension, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary),
            }
            results_list.append(results)

        df = pd.DataFrame(results_list)
        return df

    def export_company_results(
        self,
        df: pd.DataFrame,
        output_path: str = '分析结果_按公司汇总.xlsx'
    ):
        """
        导出按公司分组的结果到Excel

        Args:
            df: 按公司分组的结果DataFrame
            output_path: 输出文件路径
        """
        # 设置公司名称为索引
        if '公司名称' in df.columns:
            df_export = df.set_index('公司名称')
        else:
            df_export = df

        # 导出到Excel（不保存索引列，因为我们已经用公司名称作为索引）
        df_export.to_excel(output_path)
        print(f"\n按公司汇总的分析结果已导出到: {output_path}")

    def analyze_all_metrics_by_unit(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        use_unit_dimension: bool = True,
        include_min_boundary: bool = False,
        include_max_boundary: bool = False
    ) -> pd.DataFrame:
        """
        按机组分组的指标分析
        默认根据新增“机组维度”列（1&3组、2&4组）进行合并汇总；
        若use_unit_dimension=False或无该列，则按机组名称逐个统计。

        Args:
            min_price: 最小价格（默认不包含，设置include_min_boundary=True则包含）
            max_price: 最大价格（默认不包含，设置include_max_boundary=True则包含）
            start_date: 开始日期（包含），格式：YYYY-MM-DD
            end_date: 结束日期（包含），格式：YYYY-MM-DD
            use_unit_dimension: 是否启用机组维度合并
            include_min_boundary: 是否包含最小价格边界值
            include_max_boundary: 是否包含最大价格边界值

        Returns:
            按机组分组的指标DataFrame
        """
        if '公司名称' not in self.df.columns or '机组名称' not in self.df.columns:
            print("警告: 数据中不包含'公司名称'或'机组名称'列，无法按机组分组")
            return pd.DataFrame()

        results_list = []

        for company in self.df['公司名称'].unique():
            company_df = self.df[self.df['公司名称'] == company]
            has_dimension = use_unit_dimension and '机组维度' in company_df.columns
            if has_dimension:
                targets = company_df['机组维度'].dropna().unique()
            else:
                targets = company_df['机组名称'].unique()

            for target in targets:
                filter_kwargs = {'company_name': company}
                base_info = {'公司名称': company}

                if has_dimension:
                    filter_kwargs['unit_dimension'] = target
                    base_info['机组维度'] = target
                    unit_names = company_df[company_df['机组维度'] == target]['机组名称'].unique()
                    base_info['机组名称列表'] = ','.join(map(str, unit_names))
                else:
                    filter_kwargs['unit_name'] = target
                    base_info['机组名称'] = target

                results_list.append({
                    **base_info,
                    '1. 日前小时数': self.calculate_daily_forward_hours(min_price, max_price, start_date, end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '2. 实时小时数': self.calculate_realtime_hours(min_price, max_price, start_date, end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '3. 日前价格均价': self.calculate_daily_forward_avg_price(min_price, max_price, start_date, end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '4. 实时价格均价': self.calculate_realtime_avg_price(min_price, max_price, start_date, end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '5. 省间价格均价': self.calculate_inter_provincial_avg_price(min_price, max_price, start_date=start_date, end_date=end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '6. 省间中标电量': self.calculate_inter_provincial_power(min_price, max_price, start_date=start_date, end_date=end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '7. 日前中标电量': self.calculate_daily_forward_power(min_price, max_price, start_date, end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '8. 实际出力电量': self.calculate_actual_output_power(min_price, max_price, start_date, end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '9. 中长期持仓电量均值': self.calculate_medium_long_avg_power(min_price, max_price, start_date=start_date, end_date=end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                    '10. 中长期持仓加权均价': self.calculate_medium_long_weighted_avg_price(min_price, max_price, start_date=start_date, end_date=end_date, include_min_boundary=include_min_boundary, include_max_boundary=include_max_boundary, **filter_kwargs),
                })

        df = pd.DataFrame(results_list)
        return df

    def export_unit_results(
        self,
        df: pd.DataFrame,
        output_path: str = '分析结果_按机组汇总.xlsx'
    ):
        """
        导出按机组分组的结果到Excel

        Args:
            df: 按机组分组的结果DataFrame
            output_path: 输出文件路径
        """
        # 设置公司名称和机组维度/机组名称为复合索引
        if '公司名称' in df.columns:
            index_cols = ['公司名称']
            if '机组维度' in df.columns:
                index_cols.append('机组维度')
            elif '机组名称' in df.columns:
                index_cols.append('机组名称')
            df_export = df.set_index(index_cols)
        else:
            df_export = df

        # 导出到Excel
        df_export.to_excel(output_path)
        print(f"\n按机组汇总的分析结果已导出到: {output_path}")


def main():
    """主函数"""
    excel_path = '合并交易量价数据.xlsx'

    # 初始化分析器
    analyzer = PowerDataAnalyzer(excel_path)

    # # 示例1: 按公司汇总分析价格区间 200-300 元/MWh
    # print("\n【示例1】按公司汇总分析价格区间: 200 < 价格 < 300")
    # results_by_company1 = analyzer.analyze_all_metrics_by_company(min_price=200, max_price=300)
    # print(results_by_company1)
    # analyzer.export_company_results(results_by_company1, '分析结果_按公司汇总_价格区间200-300.xlsx')

    # # 示例2: 按公司汇总分析价格 > 300 元/MWh
    # print("\n\n【示例2】按公司汇总分析价格区间: 价格 > 300")
    # results_by_company2 = analyzer.analyze_all_metrics_by_company(min_price=300)
    # print(results_by_company2)
    # analyzer.export_company_results(results_by_company2, '分析结果_按公司汇总_价格大于300.xlsx')

    # # 示例3: 按公司汇总分析价格 < 200 元/MWh
    # print("\n\n【示例3】按公司汇总分析价格区间: 价格 < 200")
    # results_by_company3 = analyzer.analyze_all_metrics_by_company(max_price=200)
    # print(results_by_company3)
    # analyzer.export_company_results(results_by_company3, '分析结果_按公司汇总_价格小于200.xlsx')

    # # 示例4: 按公司汇总分析指定日期范围内的数据
    # print("\n\n【示例4】按公司汇总分析日期范围: 2026-01-01 至 2026-01-01，价格区间: 0 <= 价格 < 200")
    # results_by_company4 = analyzer.analyze_all_metrics_by_company(min_price=0, max_price=200, start_date='2026-01-01', end_date='2026-01-01', include_min_boundary=True, include_max_boundary=True)
    # print(results_by_company4)
    # analyzer.export_company_results(results_by_company4, '分析结果_按公司汇总_日期范围2026-01-01至2026-01-01.xlsx')

    # # 示例5: 按公司汇总分析指定日期范围内且价格区间的数据
    # print("\n\n【示例5】按公司汇总分析日期范围: 2026-01-01 至 2026-01-31，价格区间: 200 < 价格 < 300")
    # results_by_company5 = analyzer.analyze_all_metrics_by_company(min_price=200, max_price=300, start_date='2026-01-01', end_date='2026-01-31')
    # print(results_by_company5)
    # analyzer.export_company_results(results_by_company5, '分析结果_按公司汇总_日期价格组合筛选.xlsx')

    # 示例6: 按机组维度汇总分析
    print("\n\n【示例6】按机组维度汇总分析日期范围: 2026-01-01 至 2026-01-01，价格区间: 0 <= 价格 < 200")
    results_by_unit = analyzer.analyze_all_metrics_by_unit(min_price=0, max_price=200, start_date='2026-01-01', end_date='2026-01-01', include_min_boundary=True, include_max_boundary=True)
    print(results_by_unit)
    analyzer.export_unit_results(results_by_unit, '分析结果_按机组汇总.xlsx')


if __name__ == "__main__":
    main()
