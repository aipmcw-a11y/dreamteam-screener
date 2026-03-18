import streamlit as st
from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(page_title="드림팀 스크리너", page_icon="📈", layout="wide")

# ─────────────────────────────────────────────
# pykrx 버전 호환 헬퍼 함수
# ─────────────────────────────────────────────
def normalize_ohlcv(df):
    """영문 컬럼(신버전) → 한글 컬럼으로 통일"""
    col_map = {
        'Open':   '시가',
        'High':   '고가',
        'Low':    '저가',
        'Close':  '종가',
        'Volume': '거래량',
    }
    # 영문 컬럼이 존재할 때만 rename
    rename_targets = {k: v for k, v in col_map.items() if k in df.columns}
    if rename_targets:
        df = df.rename(columns=rename_targets)
    return df

def normalize_market_cap(df):
    """시총 컬럼명 통일 (Marcap → 시가총액)"""
    if 'Marcap' in df.columns and '시가총액' not in df.columns:
        df = df.rename(columns={'Marcap': '시가총액'})
    return df

def get_last_trading_day(market="KOSPI", lookback=10):
    """오늘이 휴장일이면 가장 최근 거래일을 반환"""
    for i in range(lookback):
        date_str = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = stock.get_market_cap(date_str, market=market)
            if df is not None and len(df) > 0:
                return date_str
        except:
            continue
    return datetime.now().strftime("%Y%m%d")


class DreamTeamScreener:
    def __init__(self):
        self.stock_symbols = []
        self.stock_info = {}
        
    def get_company_name(self, symbol):
        return self.stock_info.get(symbol, symbol)
        
    def calculate_dmi_adx(self, data, period=14):
        """DMI와 ADX 계산 (EMA 방식)"""
        high = data['고가']
        low = data['저가'] 
        close = data['종가']
        
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        up_move = high.diff()
        down_move = -low.diff()
        
        dm_plus = pd.Series(0.0, index=data.index)
        dm_minus = pd.Series(0.0, index=data.index)
        
        dm_plus[(up_move > down_move) & (up_move > 0)] = up_move
        dm_minus[(down_move > up_move) & (down_move > 0)] = down_move
        
        atr = tr.ewm(span=period, adjust=False).mean()
        smoothed_dm_plus = dm_plus.ewm(span=period, adjust=False).mean()
        smoothed_dm_minus = dm_minus.ewm(span=period, adjust=False).mean()
        
        di_plus = 100 * smoothed_dm_plus / atr
        di_minus = 100 * smoothed_dm_minus / atr
        
        dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
        dx = dx.replace([np.inf, -np.inf], 0)
        
        adx = dx.ewm(span=period, adjust=False).mean()
        
        return di_plus, di_minus, adx
    
    def calculate_macd(self, data, fast=12, slow=26, signal=9):
        """MACD 계산"""
        close = data['종가']
        exp1 = close.ewm(span=fast).mean()
        exp2 = close.ewm(span=slow).mean()
        macd_line = exp1 - exp2
        signal_line = macd_line.ewm(span=signal).mean()
        macd_oscillator = macd_line - signal_line
        return macd_oscillator
    
    def check_condition1_dmi_adx(self, di_plus, di_minus, adx):
        """조건 1 확인"""
        results = []
        signal_dates = []
        signal_adx_values = []
        signal_di_minus_values = []
        adx_decline_dates = []
        
        lookback_period = 30
        
        for i in range(lookback_period, len(adx)):
            breakthrough = False
            adx_decline = False
            signal_date = None
            signal_adx = None
            signal_di_minus = None
            decline_date = None
            
            for j in range(i, max(i-lookback_period, 0), -1):
                if j > 0:
                    if (di_minus.iloc[j-1] > adx.iloc[j-1] and 
                        di_minus.iloc[j] < adx.iloc[j] and
                        adx.iloc[j-1] >= 30):
                        
                        breakthrough = True
                        signal_date = adx.index[j]
                        signal_adx = adx.iloc[j]
                        signal_di_minus = di_minus.iloc[j]
                        
                        for k in range(j+1, min(j+4, len(adx))):
                            if k < len(adx) and adx.iloc[k] < adx.iloc[k-1]:
                                adx_decline = True
                                decline_date = adx.index[k]
                                break
                        break
            
            results.append(breakthrough and adx_decline)
            signal_dates.append(signal_date)
            signal_adx_values.append(signal_adx)
            signal_di_minus_values.append(signal_di_minus)
            adx_decline_dates.append(decline_date)
        
        result_series = pd.Series(results, index=adx.index[lookback_period:])
        signal_info = {
            'dates': pd.Series(signal_dates, index=adx.index[lookback_period:]),
            'adx_values': pd.Series(signal_adx_values, index=adx.index[lookback_period:]),
            'di_minus_values': pd.Series(signal_di_minus_values, index=adx.index[lookback_period:]),
            'adx_decline_dates': pd.Series(adx_decline_dates, index=adx.index[lookback_period:])
        }
        
        return result_series, signal_info
    
    def check_condition2_macd(self, weekly_macd):
        """조건 2 확인"""
        if len(weekly_macd) < 2:
            return False
        return weekly_macd.iloc[-1] > weekly_macd.iloc[-2]
    
    def get_weekly_data(self, daily_data):
        """일봉을 주봉으로 변환 (실제 거래일 기준)"""
        daily_data_copy = daily_data.copy()
        daily_data_copy['week'] = daily_data_copy.index.to_series().dt.to_period('W-FRI')
        
        weekly = daily_data_copy.groupby('week').agg({
            '시가': 'first',
            '고가': 'max',
            '저가': 'min',
            '종가': 'last',
            '거래량': 'sum'
        })
        
        weekly.index = daily_data_copy.groupby('week').apply(lambda x: x.index[-1])
        
        return weekly.dropna()
    
    def analyze_stock(self, symbol):
        """개별 주식 분석"""
        try:
            company_name = self.get_company_name(symbol)
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            
            daily_data = stock.get_market_ohlcv_by_date(
                start_date.strftime("%Y%m%d"), 
                end_date.strftime("%Y%m%d"), 
                symbol
            )

            # ── [방법 B] 컬럼명 정규화 ──
            daily_data = normalize_ohlcv(daily_data)
            
            if len(daily_data) < 50:
                return None
            
            di_plus, di_minus, adx = self.calculate_dmi_adx(daily_data)
            
            weekly_data = self.get_weekly_data(daily_data)
            if len(weekly_data) < 10:
                return None
                
            weekly_macd = self.calculate_macd(weekly_data)
            
            condition1_results, signal_info = self.check_condition1_dmi_adx(di_plus, di_minus, adx)
            condition1_met = condition1_results.iloc[-1] if len(condition1_results) > 0 else False
            
            signal_date = None
            signal_adx = None
            days_ago = None
            
            if condition1_met and len(condition1_results) > 0:
                last_signal_date = signal_info['dates'].iloc[-1]
                if last_signal_date is not None:
                    signal_date = last_signal_date.strftime('%Y-%m-%d')
                    signal_adx = signal_info['adx_values'].iloc[-1]
                    days_ago = (daily_data.index[-1] - last_signal_date).days
            
            condition2_met = self.check_condition2_macd(weekly_macd)
            
            current_adx = adx.iloc[-1]
            current_di_plus = di_plus.iloc[-1] 
            current_di_minus = di_minus.iloc[-1]
            current_weekly_macd = weekly_macd.iloc[-1]
            prev_weekly_macd = weekly_macd.iloc[-2] if len(weekly_macd) > 1 else 0
            
            return {
                'symbol': symbol,
                'company_name': company_name,
                'current_price': daily_data['종가'].iloc[-1],
                'condition1_met': condition1_met,
                'condition2_met': condition2_met,
                'both_conditions_met': condition1_met and condition2_met,
                'current_adx': current_adx,
                'current_di_plus': current_di_plus,
                'current_di_minus': current_di_minus,
                'weekly_macd_current': current_weekly_macd,
                'weekly_macd_previous': prev_weekly_macd,
                'macd_change': current_weekly_macd - prev_weekly_macd,
                'signal_date': signal_date,
                'signal_adx': signal_adx,
                'days_ago': days_ago,
                'date': daily_data.index[-1].strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            return None

# Streamlit UI
st.title("📈 드림팀 주식 스크리너")
st.markdown("**KRX 공식 데이터 기반 - EMA 방식 ADX/DMI 분석**")

st.sidebar.header("설정")

# 시장 선택
market_options = st.sidebar.multiselect(
    "시장 선택 (복수 선택 가능)",
    ["KOSPI", "KOSDAQ"],
    default=["KOSPI"]
)

# 분석 종목 수
max_stocks = st.sidebar.number_input(
    "최대 분석 종목 수",
    min_value=10,
    max_value=3000,
    value=200,
    step=50
)

# 예상 시간 표시
if "KOSPI" in market_options and "KOSDAQ" in market_options:
    total_stocks = 2400
elif "KOSPI" in market_options:
    total_stocks = 900
elif "KOSDAQ" in market_options:
    total_stocks = 1500
else:
    total_stocks = 0

estimated_stocks = min(max_stocks, total_stocks)
estimated_time = estimated_stocks * 0.8
st.sidebar.warning(f"⚠️ 예상: {estimated_stocks}개 종목 / 약 {int(estimated_time/60)}분")

if st.sidebar.button("🔍 스크리닝 시작", type="primary"):
    all_symbols = []
    
    from pykrx import stock as pykrx_stock
    today = datetime.now().strftime("%Y%m%d")
    
    if "KOSPI" in market_options:
        kospi_symbols = pykrx_stock.get_market_ticker_list(today, market="KOSPI")
        all_symbols.extend(kospi_symbols)
        st.info(f"KOSPI {len(kospi_symbols)}개 종목 로드")
    
    if "KOSDAQ" in market_options:
        kosdaq_symbols = pykrx_stock.get_market_ticker_list(today, market="KOSDAQ")
        all_symbols.extend(kosdaq_symbols)
        st.info(f"KOSDAQ {len(kosdaq_symbols)}개 종목 로드")
    
    all_symbols = list(set(all_symbols))
    
    st.info("시가총액 순으로 정렬 중...")
    
    try:
        cap_df = None
        # ── 휴장일 대비: 가장 최근 거래일 자동 탐색 ──
        trading_day_kospi  = get_last_trading_day("KOSPI")
        trading_day_kosdaq = get_last_trading_day("KOSDAQ")

        if "KOSPI" in market_options and "KOSDAQ" in market_options:
            cap_kospi  = pykrx_stock.get_market_cap(trading_day_kospi,  market="KOSPI")
            cap_kosdaq = pykrx_stock.get_market_cap(trading_day_kosdaq, market="KOSDAQ")
            cap_df = pd.concat([cap_kospi, cap_kosdaq])
        elif "KOSPI" in market_options:
            cap_df = pykrx_stock.get_market_cap(trading_day_kospi,  market="KOSPI")
        elif "KOSDAQ" in market_options:
            cap_df = pykrx_stock.get_market_cap(trading_day_kosdaq, market="KOSDAQ")

        # ── [방법 B] 시총 컬럼명 정규화 ──
        cap_df = normalize_market_cap(cap_df)

        if cap_df is None or len(cap_df) == 0 or '시가총액' not in cap_df.columns:
            raise ValueError("시총 데이터를 불러올 수 없습니다.")

        cap_df = cap_df.sort_values('시가총액', ascending=False)
        sorted_symbols = [s for s in cap_df.index if s in all_symbols][:max_stocks]
        
    except Exception as e:
        st.warning(f"시총 정렬 실패: {str(e)}. 원본 순서로 진행합니다.")
        sorted_symbols = all_symbols[:max_stocks]
    
    screener = DreamTeamScreener()
    screener.stock_symbols = sorted_symbols[:max_stocks]
    
    st.success(f"시총 상위 {len(screener.stock_symbols)}개 종목 분석 시작")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    results = []
    total = len(screener.stock_symbols)
    
    st.info("종목명 로딩 중...")
    name_progress = st.progress(0)
    for idx, symbol in enumerate(screener.stock_symbols):
        screener.get_company_name(symbol)
        if idx % 10 == 0:
            name_progress.progress((idx + 1) / len(screener.stock_symbols))
    name_progress.empty()
    
    for i, symbol in enumerate(screener.stock_symbols):
        company_name = screener.get_company_name(symbol)
        status_text.text(f"분석 중... {i+1}/{total} - {company_name} ({symbol})")
        progress_bar.progress((i + 1) / total)
        
        result = screener.analyze_stock(symbol)
        if result:
            results.append(result)
    
    status_text.text("✅ 분석 완료!")
    
    if len(results) > 0:
        df = pd.DataFrame(results)
        
        st.info("종목명 업데이트 중...")
        for idx, row in df.iterrows():
            if not row['company_name'] or row['company_name'] == row['symbol']:
                try:
                    name = stock.get_market_ticker_name(row['symbol'])
                    if name and name != row['symbol']:
                        df.at[idx, 'company_name'] = name
                except:
                    pass
        
        df = df.sort_values(['both_conditions_met', 'condition1_met', 'condition2_met'], 
                          ascending=False)
        
        both = df[df['both_conditions_met'] == True]
        if len(both) > 0:
            st.success(f"🎯 두 조건 모두 충족: {len(both)}개 종목")
            
            for _, row in both.iterrows():
                try:
                    company_name = stock.get_market_ticker_name(row['symbol'])
                    if not company_name or company_name == row['symbol']:
                        company_name = row['company_name']
                except:
                    company_name = row['company_name']
                
                with st.expander(f"✨ {company_name} ({row['symbol']}) - {row['current_price']:,.0f}원"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**📊 조건 1 (DMI/ADX)**")
                        if row['signal_date']:
                            st.write(f"- 교차일: {row['signal_date']} ({row['days_ago']}일 전)")
                            st.write(f"- 교차시 ADX: {row['signal_adx']:.2f}")
                        st.write(f"- 현재 ADX: {row['current_adx']:.2f}")
                        st.write(f"- 현재 +DI: {row['current_di_plus']:.2f}")
                        st.write(f"- 현재 -DI: {row['current_di_minus']:.2f}")
                    
                    with col2:
                        st.markdown("**📈 조건 2 (주봉 MACD)**")
                        st.write(f"- 금주 MACD: {row['weekly_macd_current']:.4f}")
                        st.write(f"- 전주 MACD: {row['weekly_macd_previous']:.4f}")
                        st.write(f"- 변화량: {row['macd_change']:.4f}")
        else:
            st.info("두 조건 모두 충족하는 종목이 없습니다.")
        
        cond1 = df[(df['condition1_met'] == True) & (df['condition2_met'] == False)]
        if len(cond1) > 0:
            st.warning(f"⚠️ 조건 1만 충족: {len(cond1)}개 종목")
            display_df = cond1[['company_name', 'symbol', 'current_price', 'signal_date', 'days_ago']].copy()
            display_df.columns = ['종목명', '코드', '현재가', '신호일', '경과일']
            st.dataframe(display_df.head(10))
        
        cond2 = df[(df['condition1_met'] == False) & (df['condition2_met'] == True)]
        if len(cond2) > 0:
            st.info(f"ℹ️ 조건 2만 충족: {len(cond2)}개 종목")
            display_df = cond2[['company_name', 'symbol', 'current_price', 'macd_change']].copy()
            display_df.columns = ['종목명', '코드', '현재가', 'MACD변화']
            st.dataframe(display_df.head(10))
        
        df_download = df.copy()
        
        bool_columns = ['condition1_met', 'condition2_met', 'both_conditions_met']
        for col in bool_columns:
            df_download[col] = df_download[col].map({True: '충족', False: '미충족'})
        
        float_columns = ['current_price', 'current_adx', 'current_di_plus', 'current_di_minus', 
                        'weekly_macd_current', 'weekly_macd_previous', 'macd_change', 'signal_adx']
        for col in float_columns:
            if col in df_download.columns:
                df_download[col] = df_download[col].round(2)
        
        csv = df_download.to_csv(index=False, encoding='utf-8-sig', sep=',', lineterminator='\n')
        
        st.download_button(
            label="📥 전체 결과 CSV 다운로드 (Excel용)",
            data=csv.encode('utf-8-sig'),
            file_name=f"dreamteam_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.error("분석 결과가 없습니다.")

st.sidebar.markdown("---")
st.sidebar.info("""
**사용 방법:**
1. 분석할 종목 수 선택
2. '스크리닝 시작' 버튼 클릭
3. 결과 확인 및 CSV 다운로드

""")
