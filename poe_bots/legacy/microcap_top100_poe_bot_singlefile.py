#!/usr/bin/env python
# -*- coding: utf-8 -*-
# poe: name=Microcap-Top100-Signal-Singlefile
# poe: privacy_shield=half
"""Single-file POE bot for Top100 signal and realtime signal."""

import base64
import gzip
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

try:
    from fastapi_poe.types import SettingsResponse
except Exception:
    class SettingsResponse:
        def __init__(self, introduction_message: str = "") -> None:
            self.introduction_message = introduction_message

try:
    poe
except NameError:
    try:
        import fastapi_poe as poe
    except Exception:
        poe = None

CMD_SIGNAL = "\u4fe1\u53f7"
CMD_REALTIME = "\u5b9e\u65f6\u4fe1\u53f7"
CMD_FORCE = "\u5f3a\u5236\u5237\u65b0\u5b9e\u65f6\u4fe1\u53f7"

EMBEDDED_PAYLOAD = """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"""
LOOKBACK = 16
FUTURES_DRAG = 3.0 / 10000.0
TAIL_JITTER_WARNING_GAP = 0.001
TAIL_JITTER_CAUTION_GAP = 0.002

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
})


def load_payload() -> dict[str, object]:
    raw = gzip.decompress(base64.b64decode(EMBEDDED_PAYLOAD.encode('ascii')))
    return json.loads(raw.decode('utf-8'))


PAYLOAD = load_payload()


def update_settings() -> None:
    if poe is None or not hasattr(poe, 'update_settings'):
        return
    intro = (
        'Top100 microcap signal bot (single-file embedded-data version)\n\n'
        f'- send {CMD_SIGNAL} for the embedded close-confirmed signal\n'
        f'- send {CMD_REALTIME} for a live intraday signal using embedded effective members\n'
        f'- send {CMD_FORCE} for the same live refresh\n\n'
        'This file carries its own baseline data and does not import local strategy modules.'
    )
    poe.update_settings(SettingsResponse(introduction_message=intro))


def build_close_df() -> pd.DataFrame:
    proxy = pd.DataFrame(PAYLOAD['proxy_close'])
    hedge = pd.DataFrame(PAYLOAD['hedge_close'])
    proxy['date'] = pd.to_datetime(proxy['date'])
    hedge['date'] = pd.to_datetime(hedge['date'])
    proxy['close'] = pd.to_numeric(proxy['close'], errors='coerce')
    hedge['close'] = pd.to_numeric(hedge['close'], errors='coerce')
    close_df = proxy.rename(columns={'close': 'microcap'}).merge(
        hedge.rename(columns={'close': 'hedge'}), on='date', how='inner'
    )
    return close_df.sort_values('date').set_index('date').dropna()


def calc_momentum(series: pd.Series, lookback: int) -> pd.Series:
    return series.div(series.shift(lookback)).sub(1.0)


def calc_bias_momentum(series: pd.Series, bias_n: int, mom_day: int) -> pd.Series:
    prices = series.values.astype(float)
    n = len(prices)
    result = np.full(n, np.nan)
    ma = series.rolling(bias_n).mean().values
    total_lookback = bias_n + mom_day - 1
    x = np.arange(mom_day, dtype=float)
    for i in range(total_lookback, n):
        bias_window = np.empty(mom_day)
        valid = True
        for j in range(mom_day):
            idx = i - mom_day + 1 + j
            if np.isnan(ma[idx]) or ma[idx] < 1e-10 or np.isnan(prices[idx]):
                valid = False
                break
            bias_window[j] = prices[idx] / ma[idx]
        if not valid or bias_window[0] < 1e-10:
            continue
        bias_norm = bias_window / bias_window[0]
        slope = np.polyfit(x, bias_norm, 1)[0]
        result[i] = slope * 10000
    return pd.Series(result, index=series.index)


def calc_rolling_r2(series: pd.Series, window: int) -> pd.Series:
    values = series.values.astype(float)
    result = np.full(len(values), np.nan)
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    ss_x = ((x - x_mean) ** 2).sum()
    for i in range(window - 1, len(values)):
        y = values[i - window + 1 : i + 1]
        if np.any(np.isnan(y)):
            continue
        y_mean = y.mean()
        ss_y = ((y - y_mean) ** 2).sum()
        if ss_y < 1e-12:
            result[i] = 0.0
            continue
        ss_xy = ((x - x_mean) * (y - y_mean)).sum()
        result[i] = (ss_xy ** 2) / (ss_x * ss_y)
    return pd.Series(result, index=series.index)


def run_backtest(close_df: pd.DataFrame) -> pd.DataFrame:
    work = close_df.copy()
    work['microcap_ret'] = work['microcap'].pct_change(fill_method=None)
    work['hedge_ret'] = work['hedge'].pct_change(fill_method=None)
    work['microcap_mom'] = calc_momentum(work['microcap'], LOOKBACK)
    work['hedge_mom'] = calc_momentum(work['hedge'], LOOKBACK)
    work['momentum_gap'] = work['microcap_mom'] - work['hedge_mom']
    work['ratio'] = work['microcap'] / work['hedge']
    work['ratio_bias_mom'] = calc_bias_momentum(work['ratio'], 60, 20)
    work['ratio_r2'] = calc_rolling_r2(work['ratio'], 5)
    valid_start = work[['microcap_mom', 'hedge_mom']].dropna().index.min()
    work = work.loc[valid_start:].copy()
    rows = []
    holding = False
    for i in range(1, len(work)):
        active_ret = 0.0
        drag = FUTURES_DRAG if holding else 0.0
        if holding:
            microcap_ret = work['microcap_ret'].iloc[i]
            hedge_ret = work['hedge_ret'].iloc[i]
            if pd.notna(microcap_ret) and pd.notna(hedge_ret):
                active_ret = float(microcap_ret - hedge_ret)
        signal_on = bool(
            pd.notna(work['microcap_mom'].iloc[i]) and
            pd.notna(work['hedge_mom'].iloc[i]) and
            work['microcap_mom'].iloc[i] > work['hedge_mom'].iloc[i]
        )
        next_holding = 'long_microcap_short_zz1000' if signal_on else 'cash'
        rows.append({
            'date': work.index[i],
            'return_raw': active_ret - drag,
            'holding': 'long_microcap_short_zz1000' if holding else 'cash',
            'next_holding': next_holding,
            'microcap_close': float(work['microcap'].iloc[i]),
            'hedge_close': float(work['hedge'].iloc[i]),
            'microcap_mom': float(work['microcap_mom'].iloc[i]),
            'hedge_mom': float(work['hedge_mom'].iloc[i]),
            'momentum_gap': float(work['momentum_gap'].iloc[i]),
            'ratio_bias_mom': float(work['ratio_bias_mom'].iloc[i]) if pd.notna(work['ratio_bias_mom'].iloc[i]) else np.nan,
            'ratio_r2': float(work['ratio_r2'].iloc[i]) if pd.notna(work['ratio_r2'].iloc[i]) else np.nan,
            'weight': 1.0,
            'futures_drag': drag,
        })
        holding = signal_on
    result = pd.DataFrame(rows).set_index('date')
    result['return'] = result['return_raw']
    result['nav'] = (1.0 + result['return']).cumprod()
    return result


def build_latest_signal(result: pd.DataFrame) -> pd.DataFrame:
    last = result.iloc[[-1]].copy().reset_index()
    last['signal_label'] = np.where(last['next_holding'] == 'cash', 'cash', 'long_microcap_short_zz1000')
    current_holding = str(result.iloc[-1]['holding'])
    next_holding = str(result.iloc[-1]['next_holding'])
    if current_holding == next_holding:
        trade_state = 'hold'
    elif current_holding == 'cash' and next_holding != 'cash':
        trade_state = 'open'
    elif current_holding != 'cash' and next_holding == 'cash':
        trade_state = 'close'
    else:
        trade_state = 'switch'
    last['current_holding'] = current_holding
    last['trade_state'] = trade_state
    return last[[
        'date', 'signal_label', 'next_holding', 'microcap_close', 'hedge_close',
        'microcap_mom', 'hedge_mom', 'momentum_gap', 'ratio_bias_mom', 'ratio_r2',
        'weight', 'futures_drag', 'current_holding', 'trade_state'
    ]]


def assess_history_anchor_freshness() -> dict[str, object]:
    latest_trade_date = pd.Timestamp(PAYLOAD['anchor_trade_date']).normalize()
    current_date = pd.Timestamp.now().normalize()
    stale_days = max(0, int((current_date - latest_trade_date).days))
    return {
        'latest_trade_date': str(latest_trade_date.date()),
        'current_date': str(current_date.date()),
        'stale_calendar_days': stale_days,
        'status': 'fresh' if stale_days == 0 else 'stale',
    }


def eastmoney_secid(symbol: str, prefer_index: bool = False) -> str:
    code = str(symbol).zfill(6)
    if prefer_index:
        return f'1.{code}'
    if code.startswith(('5', '6', '9')):
        return f'1.{code}'
    return f'0.{code}'


def fetch_eastmoney_spot(symbol: str, prefer_index: bool = False):
    secid = eastmoney_secid(symbol, prefer_index=prefer_index)
    url = 'https://push2.eastmoney.com/api/qt/stock/get' + f'?secid={secid}&fields=f43,f44,f45,f46,f57,f58,f60'
    try:
        response = SESSION.get(url, timeout=10, headers={'Referer': 'https://quote.eastmoney.com/'})
        response.raise_for_status()
        data = response.json().get('data') or {}
        latest = pd.to_numeric(data.get('f43'), errors='coerce')
        high = pd.to_numeric(data.get('f44'), errors='coerce')
        low = pd.to_numeric(data.get('f45'), errors='coerce')
        open_ = pd.to_numeric(data.get('f46'), errors='coerce')
        prev = pd.to_numeric(data.get('f60'), errors='coerce')
        latest = float(latest) / 100.0 if pd.notna(latest) else np.nan
        high = float(high) / 100.0 if pd.notna(high) else np.nan
        low = float(low) / 100.0 if pd.notna(low) else np.nan
        open_ = float(open_) / 100.0 if pd.notna(open_) else np.nan
        prev = float(prev) / 100.0 if pd.notna(prev) else np.nan
        rt_price = latest if pd.notna(latest) and latest > 0 else prev
        if pd.isna(rt_price) or rt_price <= 0:
            return None
        return {
            'code': str(data.get('f57') or symbol).zfill(6),
            'name': str(data.get('f58') or ''),
            'rt_price': float(rt_price),
            'prev_close': float(prev) if pd.notna(prev) else np.nan,
            'open': float(open_) if pd.notna(open_) else np.nan,
            'high': float(high) if pd.notna(high) else np.nan,
            'low': float(low) if pd.notna(low) else np.nan,
        }
    except Exception:
        return None


def fetch_batch_spot(symbols: list[str], max_workers: int = 16) -> pd.DataFrame:
    rows = []
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 16))) as pool:
        futures = {pool.submit(fetch_eastmoney_spot, symbol): symbol for symbol in symbols}
        for fut in as_completed(futures):
            row = fut.result()
            if row is not None:
                rows.append(row)
    return pd.DataFrame(rows)


def classify_tail_jitter_risk(momentum_gap: float) -> tuple[str, str]:
    abs_gap = abs(float(momentum_gap))
    if abs_gap < TAIL_JITTER_WARNING_GAP:
        return 'warning', 'gap very close to zero; confirm again near the close'
    if abs_gap < TAIL_JITTER_CAUTION_GAP:
        return 'caution', 'gap is narrow; close-time recheck is recommended'
    return 'normal', ''


def format_pct(value: float) -> str:
    return f'{float(value):+.2%}'


def format_num(value: float, digits: int = 2) -> str:
    return f'{float(value):,.{digits}f}'


def render_holding(value: str) -> str:
    if value == 'cash':
        return '空仓'
    if value == 'long_microcap_short_zz1000':
        return '多微盘 / 空中证1000'
    return str(value)


def render_trade_state(value: str) -> str:
    mapping = {
        'hold': '不变',
        'open': '开仓',
        'close': '平仓',
        'switch': '切换',
    }
    return mapping.get(str(value), str(value))


def render_jitter(value: str) -> str:
    mapping = {
        'normal': '正常',
        'caution': '注意',
        'warning': '警告',
    }
    return mapping.get(str(value), str(value))


def render_anchor_status(value: str) -> str:
    mapping = {
        'fresh': '正常',
        'stale': '过期',
    }
    return mapping.get(str(value), str(value))


def format_threshold_text(momentum_gap: float) -> str:
    gap = float(momentum_gap)
    if gap >= 0:
        return f'高于翻多阈值 {abs(gap):.2%}'
    return f'低于翻多阈值 {abs(gap):.2%}'


def format_signal_summary(row: pd.Series, freshness: dict[str, object]) -> str:
    return '\n'.join(
        [
            '信号结论',
            f"- 当前持仓：{render_holding(str(row['current_holding']))}",
            f"- 下期状态：{render_holding(str(row['next_holding']))}",
            f"- 操作建议：{render_trade_state(str(row['trade_state']))}",
            f"- 信号日期：{pd.Timestamp(row['date']).strftime('%Y-%m-%d')}",
            f"- 阈值位置：{format_threshold_text(row['momentum_gap'])}",
            '',
            '关键指标',
            f"- 微盘收盘：{format_num(row['microcap_close'])}",
            f"- 对冲收盘：{format_num(row['hedge_close'])}",
            f"- 微盘动量：{format_pct(row['microcap_mom'])}",
            f"- 对冲动量：{format_pct(row['hedge_mom'])}",
            f"- 动量差：{format_pct(row['momentum_gap'])}",
            f"- 比值 R2：{float(row['ratio_r2']):.3f}" if pd.notna(row['ratio_r2']) else '- 比值 R2：N/A',
            '',
            '调仓快照',
            f"- 最新调仓日：{PAYLOAD['latest_rebalance']}",
            f"- 当前生效名单：{PAYLOAD['effective_rebalance']}",
            f"- 调仓生效日：{PAYLOAD['rebalance_effective_date']}",
            '- 说明：调仓日是发出信号的日期，调仓生效日是信号后第一个交易日',
            '',
            '数据状态',
            f"- 历史锚点：{render_anchor_status(str(freshness['status']))}",
            f"- 最新历史交易日：{freshness['latest_trade_date']}",
            f"- 当前日期：{freshness['current_date']}",
            f"- 滞后天数：{freshness['stale_calendar_days']} 天",
        ]
    )


def format_realtime_summary(row: pd.Series, available_rows: int, total_rows: int) -> str:
    lines = [
        '实时信号结论',
        f"- 当前持仓：{render_holding(str(row['current_holding']))}",
        f"- 下期状态：{render_holding(str(row['next_holding']))}",
        f"- 操作建议：{render_trade_state(str(row['trade_state']))}",
        f"- 快照时间：{pd.Timestamp(row['date']).strftime('%Y-%m-%d %H:%M:%S')}",
        '',
        '关键指标',
        f"- 微盘估算价格：{format_num(row['microcap_close'])}",
        f"- 对冲实时价格：{format_num(row['hedge_close'])}",
        f"- 微盘动量：{format_pct(row['microcap_mom'])}",
        f"- 对冲动量：{format_pct(row['hedge_mom'])}",
        f"- 动量差：{format_pct(row['momentum_gap'])}",
        f"- 阈值位置：{format_threshold_text(row['momentum_gap'])}",
        f"- 尾盘抖动风险：{render_jitter(str(row['tail_jitter_risk']))}",
        '',
        '实时数据',
        f"- 成分股有效报价：{available_rows} / {total_rows}",
        f"- 微盘报价源：{row['quote_source']}",
        f"- 对冲报价源：{row['hedge_quote_source']}",
        f"- 历史锚点交易日：{row['latest_anchor_trade_date']}",
        '',
        '调仓快照',
        f"- 最新调仓日：{PAYLOAD['latest_rebalance']}",
        f"- 当前生效名单：{PAYLOAD['effective_rebalance']}",
        f"- 调仓生效日：{PAYLOAD['rebalance_effective_date']}",
        '- 说明：若调仓日当天收盘发出新信号，通常在下一个交易日执行',
    ]
    note = str(row.get('tail_jitter_note') or '').strip()
    if note:
        lines.insert(11, f"- 风险提示：{note}")
    return '\n'.join(lines)


def handle_signal() -> tuple[str, bytes]:
    close_df = build_close_df()
    result = run_backtest(close_df)
    signal_df = build_latest_signal(result)
    freshness = assess_history_anchor_freshness()
    csv_bytes = signal_df.to_csv(index=False).encode('utf-8-sig')
    body = format_signal_summary(signal_df.iloc[0], freshness)
    return body, csv_bytes


def handle_realtime_signal() -> tuple[str, bytes]:
    close_df = build_close_df()
    member_symbols = [row['symbol'] for row in PAYLOAD['effective_members']]
    last_close_map = {k: float(v) for k, v in PAYLOAD['last_close_map'].items()}
    quotes_df = fetch_batch_spot(member_symbols)
    if quotes_df.empty:
        raise RuntimeError('failed to fetch realtime quotes for effective members')
    quotes_df = quotes_df.set_index('code')
    member_returns = []
    available_rows = 0
    for symbol in member_symbols:
        last_close = last_close_map.get(symbol)
        if last_close is None or last_close <= 0 or symbol not in quotes_df.index:
            continue
        rt_price = pd.to_numeric(quotes_df.at[symbol, 'rt_price'], errors='coerce')
        if pd.isna(rt_price) or rt_price <= 0:
            continue
        member_returns.append(float(rt_price / last_close - 1.0))
        available_rows += 1
    if not member_returns:
        raise RuntimeError('no usable realtime prices for effective members')
    microcap_rt_close = float(close_df['microcap'].iloc[-1]) * (1.0 + float(np.mean(member_returns)))
    hedge_quote = fetch_eastmoney_spot('000852', prefer_index=True)
    hedge_rt_close = float(hedge_quote['rt_price']) if hedge_quote else float(close_df['hedge'].iloc[-1])
    snapshot_ts = pd.Timestamp.now()
    anchor_trade_date = pd.Timestamp(PAYLOAD['anchor_trade_date'])
    if snapshot_ts <= anchor_trade_date:
        snapshot_ts = anchor_trade_date + pd.Timedelta(seconds=1)
    rt_close_df = close_df.copy()
    rt_close_df.loc[snapshot_ts, ['microcap', 'hedge']] = [microcap_rt_close, hedge_rt_close]
    rt_close_df = rt_close_df.sort_index()
    rt_result = run_backtest(rt_close_df)
    signal_df = build_latest_signal(rt_result)
    jitter_level, jitter_note = classify_tail_jitter_risk(float(signal_df.iloc[0]['momentum_gap']))
    signal_df['quote_source'] = 'eastmoney_stock_get'
    signal_df['hedge_quote_source'] = 'eastmoney_stock_get'
    signal_df['member_price_count'] = available_rows
    signal_df['member_count'] = len(member_symbols)
    signal_df['latest_anchor_trade_date'] = str(anchor_trade_date.date())
    signal_df['tail_jitter_risk'] = jitter_level
    signal_df['tail_jitter_note'] = jitter_note
    csv_bytes = signal_df.to_csv(index=False).encode('utf-8-sig')
    row = signal_df.iloc[0]
    body = format_realtime_summary(row, available_rows, len(member_symbols))
    return body, csv_bytes


def normalize_command(query_text: str) -> str:
    text = (query_text or '').strip()
    if not text:
        return CMD_SIGNAL
    if '\u5b9e\u65f6' in text and '\u4fe1\u53f7' in text:
        return CMD_REALTIME
    if '\u4fe1\u53f7' in text:
        return CMD_SIGNAL
    return 'help'


def send_message(text: str, file_name: str | None = None, file_bytes: bytes | None = None) -> None:
    if poe is None or not hasattr(poe, 'start_message'):
        print(text)
        if file_name and file_bytes is not None:
            print(f'[file] {file_name} ({len(file_bytes)} bytes)')
        return
    with poe.start_message() as msg:
        msg.write(text)
        if file_name and file_bytes is not None:
            msg.attach_file(name=file_name, contents=file_bytes, content_type='text/csv')


def main() -> None:
    if poe is not None and hasattr(poe, 'query') and getattr(poe, 'query', None) is not None:
        query_text = (poe.query.text or '').strip()
    else:
        query_text = ' '.join(sys.argv[1:]).strip()
    command = normalize_command(query_text)
    if command == 'help':
        send_message(
            'Supported commands:\n'
            f'1. {CMD_SIGNAL}\n'
            f'2. {CMD_REALTIME}\n'
            f'3. {CMD_FORCE}\n\n'
            f"embedded anchor date: {PAYLOAD['anchor_trade_date']}\n"
            f"latest rebalance snapshot: {PAYLOAD['latest_rebalance']}\n"
            f"effective member snapshot: {PAYLOAD['effective_rebalance']}"
        )
        return
    try:
        if command == CMD_REALTIME:
            body, csv_bytes = handle_realtime_signal()
            send_message(f"## Realtime Signal\n\n```text\n{body}\n```", 'microcap_top100_realtime_signal.csv', csv_bytes)
        else:
            body, csv_bytes = handle_signal()
            send_message(f"## Confirmed Signal\n\n```text\n{body}\n```", 'microcap_top100_signal.csv', csv_bytes)
    except Exception as exc:
        send_message(f'Computation failed: {exc}')


update_settings()

if __name__ == '__main__':
    main()
