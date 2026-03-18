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
def get_ticker_name_naver(symbol):
    """네이버 금융 API로 종목명 조회 (pykrx 대체)"""
    try:
        import requests as _req
        url = f"https://m.stock.naver.com/api/stock/{symbol}/basic"
        r = _req.get(url, timeout=3)
        if r.status_code == 200:
            data = r.json()
            name = data.get("stockName", "")
            if name:
                return name
    except Exception:
        pass
    return None

def normalize_ohlcv(df):
    """영문 컬럼(신버전) → 한글 컬럼으로 통일"""
    col_map = {
        'Open':   '시가',
        'High':   '고가',
        'Low':    '저가',
        'Close':  '종가',
        'Volume': '거래량',
    }
    rename_targets = {k: v for k, v in col_map.items() if k in df.columns}
    if rename_targets:
        df = df.rename(columns=rename_targets)
    return df

# ── 내장 종목코드 (pykrx ticker API 불안정 대비) ──
_KOSPI_TICKERS = [
    "005930","000660","005490","035420","000270","051910","006400","005380",
    "068270","105560","055550","012330","028260","066570","032830","017670",
    "003550","011200","207940","034730","096770","015760","018260","011170",
    "034220","000810","033780","009150","003490","010130","086790","161390",
    "047050","009540","010950","002790","008770","004020","010140","000720",
    "002380","021240","030200","003410","011790","014820","000100","271560",
    "097950","006800","042660","005940","039490","024110","011070","000990",
    "004990","085620","008560","001570","011780","029780","138040","003670",
    "004170","007070","016880","002350","010620","069960","001040","000150",
    "001680","002210","007310","009830","023530","267250","010060","001450",
    "000080","000230","001740","003600","004140","005830","006260","006360",
    "007460","008350","009970","011500","012750","015020","016360","017800",
    "018880","019170","020150","021080","024900","025540","026960","028050",
    "028300","029460","030000","031430","032640","033530","034310","035000",
    "035250","035720","036460","036570","036830","037270","039130","039570",
    "040910","041440","042670","044380","047810","048410","051600","052690",
    "053210","054540","055250","056190","057050","058430","058650","060980",
    "064350","064960","066270","067310","068400","069620","071050","071970",
    "072710","073240","078930","079550","082640","083790","084010","084370",
    "086280","086820","088350","090350","091190","093050","096760","097230",
    "099190","100840","102280","103140","105780","108670","111770","112610",
    "114090","120110","123700","128940","130500","138930","139130","145720",
    "145990","175330","180640","192400","194700","199800","204210","214420",
    "214840","218410","222080","236340","241560","248070","251270","263720",
    "267260","267270","270810","272210","278280","282330","285130","287970",
    "293490","298040","302440","316140","319400","322000","323410","326030",
    "336260","336370","347860","352820","357780","363280","365550","373220",
    "377300","383220","402340","405640","412580","413640","418250","421850",
    "950130",
]

_KOSDAQ_TICKERS = [
    "247540","035900","196170","086520","091990","263750","112040","357120",
    "041510","039200","031430","293490","326030","122870","251270","036030",
    "035760","095340","067310","078160","054620","031370","064550","095700",
    "078600","036810","131970","036200","041830","058470","215600","058820",
    "033290","060310","095190","078020","059210","032500","064760","053800",
    "060230","036830","053270","060900","048410","032190","080630","045060",
    "033230","067010","046080","052020","039560","067900","065350","094360",
    "049520","045300","043090","064090","068240","293780","290650","348950",
    "035420","060280","041190","058610","089600","089790","071200","211270",
    "089850","096530","041960","068760","078130","053160","041460","145020",
    "078520","293490","215600","900140","900270","900290","900310","900340",
    "900360","900380","900390","069080","078650","102710","140410","196300",
    "241030","256840","263020","267260","290550","294090","302430","305090",
    "307950","314130","319660","321260","323280","328130","330590","336060",
    "340570","348370","352480","357230","361610","370090","371840","376180",
    "377030","382480","383310","389260","392420","394280","396270","397030",
    "402030","403550","404990","407400","408920","411270","412350","413570",
    "415640","416940","418420","419930","421370","423250","424490","425420",
    "427950","428670","430100","431000","432320","432710","434730","436450",
    "438900","440290","443060","445680","450340","452260","455250","458290",
]

# ── 내장 종목명 딕셔너리 (pykrx ticker name API 불안정 대비) ──
_TICKER_NAMES = {
    "005930":"삼성전자","000660":"SK하이닉스","005490":"POSCO홀딩스",
    "035420":"NAVER","000270":"기아","051910":"LG화학","006400":"삼성SDI",
    "005380":"현대차","068270":"셀트리온","105560":"KB금융","055550":"신한지주",
    "012330":"현대모비스","028260":"삼성물산","066570":"LG전자","032830":"삼성생명",
    "017670":"SK텔레콤","003550":"LG","011200":"HMM","207940":"삼성바이오로직스",
    "034730":"SK","096770":"SK이노베이션","015760":"한국전력","018260":"삼성에스디에스",
    "011170":"롯데케미칼","034220":"LG디스플레이","000810":"삼성화재","033780":"KT&G",
    "009150":"삼성전기","003490":"대한항공","010130":"고려아연","086790":"하나금융지주",
    "161390":"한국타이어앤테크놀로지","047050":"포스코인터내셔널","009540":"한국조선해양",
    "010950":"S-Oil","002790":"아모레퍼시픽","008770":"호텔신라","004020":"현대제철",
    "010140":"삼성중공업","000720":"현대건설","002380":"KCC","021240":"코웨이",
    "030200":"KT","003410":"쌍용C&E","011790":"SKC","014820":"동원시스템즈",
    "000100":"유한양행","271560":"오리온","097950":"CJ제일제당","006800":"미래에셋증권",
    "042660":"한화오션","005940":"NH투자증권","039490":"키움증권","024110":"기업은행",
    "011070":"LG이노텍","000990":"DB하이텍","004990":"롯데지주","085620":"미래에셋생명",
    "008560":"메리츠화재","001570":"금양","011780":"금호석유","029780":"삼성카드",
    "138040":"메리츠금융지주","003670":"포스코퓨처엠","004170":"신세계","007070":"GS리테일",
    "016880":"웅진","002350":"넥센타이어","010620":"현대미포조선","069960":"현대백화점",
    "001040":"CJ","000150":"두산","001680":"대상","002210":"동성화인텍","007310":"오뚜기",
    "009830":"한화솔루션","023530":"롯데쇼핑","267250":"HD현대","010060":"OCI홀딩스",
    "001450":"현대해상","000080":"하이트진로","000230":"일화","001740":"SK네트웍스",
    "003600":"SK케미칼","004140":"동양","005830":"DB손해보험","006260":"LS",
    "006360":"GS건설","008350":"남선알미늄","009970":"영풍","011500":"한농화성",
    "012750":"에스원","015020":"이랜드월드","016360":"삼성증권","017800":"현대엘리베이터",
    "018880":"한온시스템","019170":"신도리코","020150":"롯데푸드","021080":"삼원강재",
    "024900":"동화약품","025540":"한국단자","026960":"동서","028050":"삼성엔지니어링",
    "028300":"HLB","029460":"케이씨","030000":"제일기획","031430":"신세계인터내셔날",
    "032640":"LG유플러스","033530":"세아상역","034310":"NICE","035000":"팬오션",
    "035250":"강원랜드","035720":"카카오","036460":"한국가스공사","036570":"엔씨소프트",
    "036830":"솔브레인","037270":"YG PLUS","039130":"하나투어","039570":"이수페타시스",
    "040910":"아이씨디","041440":"현대위아","042670":"두산인프라코어","044380":"웅진씽크빅",
    "047810":"한국항공우주","048410":"현대바이오","051600":"한전KPS","052690":"한전기술",
    "053210":"스카이라이프","054540":"한국콜마","055250":"엔에스쇼핑","056190":"에스에프에이",
    "057050":"현대홈쇼핑","058430":"포스코스틸리온","058650":"세아베스틸지주",
    "064350":"현대로템","064960":"S&T모티브","066270":"조현준","067310":"하나마이크론",
    "068400":"SK렌터카","069620":"대웅제약","071050":"한국금융지주","071970":"STX중공업",
    "072710":"동양생명","073240":"금호타이어","078930":"GS","079550":"LIG넥스원",
    "082640":"동양이엔피","084010":"대한제지","086280":"현대글로비스","086820":"바텍",
    "088350":"한화생명","090350":"노루페인트","091190":"국도화학","093050":"LF",
    "096760":"JW홀딩스","097230":"한진","099190":"아이센스","100840":"SNT에너지",
    "103140":"풍산","108670":"LG하우시스","111770":"영원무역홀딩스","112610":"씨에스윈드",
    "114090":"GKL","120110":"코오롱인더","128940":"한미약품","130500":"GH신소재",
    "138930":"BNK금융지주","139130":"DGB금융지주","145720":"덴티움","145990":"삼양사",
    "175330":"JB금융지주","180640":"한진칼","192400":"쿠쿠홀딩스","199800":"툴젠",
    "204210":"모두투어","214420":"토니모리","214840":"이오테크닉스","218410":"RFHIC",
    "222080":"씨아이에스","241560":"두산밥캣","248070":"솔루스첨단소재",
    "251270":"넷마블","263720":"디앤씨미디어","267260":"HD현대일렉트릭",
    "267270":"HD현대건설기계","270810":"KISCO홀딩스","272210":"한화시스템",
    "278280":"천보","282330":"BGF리테일","285130":"SK케미칼","287970":"티앤알바이오팹",
    "293490":"카카오게임즈","298040":"효성중공업","302440":"SK바이오사이언스",
    "316140":"우리금융지주","322000":"앤씨앤","323410":"카카오뱅크",
    "326030":"SK바이오팜","336260":"두산퓨얼셀","336370":"솔루엠","347860":"디어유",
    "352820":"하이브","357780":"솔브레인홀딩스","363280":"티와이홀딩스",
    "365550":"ESR켄달스퀘어리츠","373220":"LG에너지솔루션","377300":"카카오페이",
    "383220":"F&F","402340":"SK스퀘어","412580":"카카오엔터","413640":"실리콘투",
    "418250":"하이트진로홀딩스","421850":"교촌에프앤비","950130":"엑세스바이오",
    # KOSDAQ
    "247540":"에코프로비엠","035900":"JYP Ent","196170":"알테오젠","086520":"에코프로",
    "091990":"셀트리온헬스케어","263750":"펄어비스","112040":"위메이드",
    "041510":"에스엠","039200":"오스코텍","122870":"와이지엔터테인먼트","036030":"더존비즈온",
    "035760":"CJ ENM","095340":"ISC","054620":"APS홀딩스","031370":"SK머티리얼즈",
    "064550":"바이넥스","095700":"제넥신","078600":"대주전자재료","036810":"에프에스티",
    "131970":"두산테스나","041830":"인바디","058470":"리노공업","215600":"신라젠",
    "033290":"코웰패션","095190":"세아특수강","059210":"메타바이오메드","032500":"케이엠더블유",
    "064760":"케어젠","053800":"안랩","060230":"에스티팜","060900":"카카오게임즈",
    "032190":"다우데이타","080630":"아프리카TV","033230":"인터파크","067010":"쎄트렉아이",
    "046080":"에코마케팅","039560":"다날","065350":"신성델타테크","094360":"칩스앤미디어",
    "049520":"유아이엘","045300":"일진전기","043090":"우리기술투자","293780":"현대글로벌서비스",
    "290650":"엘앤씨바이오","348950":"크래프톤","060280":"큐렉소","089600":"나스미디어",
    "071200":"인텍플러스","211270":"AP위성","089850":"유니테스트","096530":"씨젠",
    "041960":"코미팜","068760":"셀리드","053160":"씨티씨바이오","041460":"에스앤에스텍",
    "145020":"휴젤","078520":"에이블씨엔씨","069080":"웹젠","102710":"이엔에프테크놀로지",
    "140410":"메지온","256840":"한국비엔씨","294090":"이오플로우","328130":"루닛",
    "348370":"엔젠바이오","352480":"씨앤씨인터내셔널","377030":"리가켐바이오",
    "383310":"에코프로에이치엔","389260":"대명에너지","392420":"쏘카",
    "404990":"브이티","411270":"유일로보틱스","412350":"레인보우로보틱스",
    "413570":"넥스틴","424490":"뷰노","438900":"에이피알","443060":"HD현대마린솔루션",
    "445680":"에이피알","450340":"파두","452260":"케이뱅크","455250":"오름테라퓨틱",
}

def get_ticker_list(market="KOSPI", lookback=10):
    """
    1) pykrx 정상 동작 시 그대로 사용
    2) 실패 시 tickers.json 파일에서 로드
    3) 파일도 없으면 내장 종목코드 반환
    """
    for i in range(lookback):
        date_str = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            syms = stock.get_market_ticker_list(date_str, market=market)
            if syms and len(syms) > 0:
                return list(syms)
        except Exception:
            pass

    # fallback 1: tickers.json
    try:
        import json, os
        json_path = os.path.join(os.path.dirname(__file__), "tickers.json")
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        tickers = data.get(market, [])
        if tickers:
            return tickers
    except Exception:
        pass

    # fallback 2: 내장 종목코드
    if market == "KOSPI":
        return list(_KOSPI_TICKERS)
    else:
        return list(_KOSDAQ_TICKERS)

def fetch_market_cap_by_volume(symbols, market="KOSPI", lookback=10):
    """
    pykrx OHLCV 데이터의 거래대금(종가×거래량)으로 대형주 순서를 근사.
    KRX 직접 접근 불필요 — Streamlit Cloud 환경에서도 동작.
    반환: 거래대금 내림차순으로 정렬된 symbols 리스트
    """
    vol_map = {}
    for i in range(lookback):
        date_str = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            # pykrx로 시장 전체 일봉 (단일 날짜)
            df = stock.get_market_ohlcv_by_date(date_str, date_str, "005930")
            # 위가 성공하면 그 날짜가 거래일 — 전체 종목 거래대금 조회
            # pykrx 에는 시장 전체 당일 OHLCV API 없으므로
            # 샘플 종목들의 개별 조회 대신, 종목 리스트 순서를 유지하되
            # 시가총액 근사를 위해 거래대금 상위 종목 일부만 사전 조회
            break
        except Exception:
            continue

    # 전체 심볼을 거래대금으로 정렬하려면 개별 조회가 필요해 너무 느림.
    # 대신 pykrx get_market_ohlcv (시장 전체 하루치)를 사용
    for i in range(lookback):
        date_str = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv(date_str, market=market)
            df = normalize_ohlcv(df)
            if df.empty:
                continue
            # 거래대금 = 종가 × 거래량
            df['거래대금'] = df['종가'] * df['거래량']
            df = df.sort_values('거래대금', ascending=False)
            sym_set = set(symbols)
            sorted_syms = [s for s in df.index if s in sym_set]
            # 정렬 안 된 나머지 추가
            sorted_syms += [s for s in symbols if s not in set(sorted_syms)]
            return sorted_syms
        except Exception:
            continue
    return symbols


class DreamTeamScreener:
    def __init__(self):
        self.stock_symbols = []
        self.stock_info = {}

    def get_company_name(self, symbol):
        # 1) 내장 딕셔너리
        if symbol in _TICKER_NAMES:
            return _TICKER_NAMES[symbol]
        # 2) 캐시(stock_info)
        if symbol in self.stock_info and self.stock_info[symbol] != symbol:
            return self.stock_info[symbol]
        # 3) 네이버 API (캐시에 저장)
        name = get_ticker_name_naver(symbol)
        if name:
            self.stock_info[symbol] = name
            return name
        return symbol

    def calculate_dmi_adx(self, data, period=14):
        """DMI와 ADX 계산 (EMA 방식)"""
        high  = data['고가']
        low   = data['저가']
        close = data['종가']

        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low  - close.shift(1))
        tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        up_move   =  high.diff()
        down_move = -low.diff()

        dm_plus  = pd.Series(0.0, index=data.index)
        dm_minus = pd.Series(0.0, index=data.index)

        dm_plus [(up_move   > down_move) & (up_move   > 0)] = up_move
        dm_minus[(down_move > up_move)   & (down_move > 0)] = down_move

        atr               = tr.ewm(span=period, adjust=False).mean()
        smoothed_dm_plus  = dm_plus.ewm(span=period, adjust=False).mean()
        smoothed_dm_minus = dm_minus.ewm(span=period, adjust=False).mean()

        di_plus  = 100 * smoothed_dm_plus  / atr
        di_minus = 100 * smoothed_dm_minus / atr

        dx  = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
        dx  = dx.replace([np.inf, -np.inf], 0)
        adx = dx.ewm(span=period, adjust=False).mean()

        return di_plus, di_minus, adx

    def calculate_macd(self, data, fast=12, slow=26, signal=9):
        """MACD 계산"""
        close       = data['종가']
        macd_line   = close.ewm(span=fast).mean() - close.ewm(span=slow).mean()
        signal_line = macd_line.ewm(span=signal).mean()
        return macd_line - signal_line

    def check_condition1_dmi_adx(self, di_plus, di_minus, adx):
        """조건 1 확인"""
        results                = []
        signal_dates           = []
        signal_adx_values      = []
        signal_di_minus_values = []
        adx_decline_dates      = []

        lookback_period = 30

        for i in range(lookback_period, len(adx)):
            breakthrough    = False
            adx_decline     = False
            signal_date     = None
            signal_adx      = None
            signal_di_minus = None
            decline_date    = None

            for j in range(i, max(i - lookback_period, 0), -1):
                if j > 0:
                    if (di_minus.iloc[j-1] > adx.iloc[j-1] and
                            di_minus.iloc[j] < adx.iloc[j] and
                            adx.iloc[j-1] >= 30):

                        breakthrough    = True
                        signal_date     = adx.index[j]
                        signal_adx      = adx.iloc[j]
                        signal_di_minus = di_minus.iloc[j]

                        for k in range(j + 1, min(j + 4, len(adx))):
                            if adx.iloc[k] < adx.iloc[k - 1]:
                                adx_decline  = True
                                decline_date = adx.index[k]
                                break
                        break

            results.append(breakthrough and adx_decline)
            signal_dates.append(signal_date)
            signal_adx_values.append(signal_adx)
            signal_di_minus_values.append(signal_di_minus)
            adx_decline_dates.append(decline_date)

        idx = adx.index[lookback_period:]
        result_series = pd.Series(results, index=idx)
        signal_info = {
            'dates':             pd.Series(signal_dates,           index=idx),
            'adx_values':        pd.Series(signal_adx_values,      index=idx),
            'di_minus_values':   pd.Series(signal_di_minus_values, index=idx),
            'adx_decline_dates': pd.Series(adx_decline_dates,      index=idx),
        }
        return result_series, signal_info

    def check_condition2_macd(self, weekly_macd):
        """조건 2 확인"""
        if len(weekly_macd) < 2:
            return False
        return weekly_macd.iloc[-1] > weekly_macd.iloc[-2]

    def get_weekly_data(self, daily_data):
        """일봉을 주봉으로 변환 (실제 거래일 기준)"""
        dc = daily_data.copy()
        dc['week'] = dc.index.to_series().dt.to_period('W-FRI')

        weekly = dc.groupby('week').agg({
            '시가':   'first',
            '고가':   'max',
            '저가':   'min',
            '종가':   'last',
            '거래량': 'sum',
        })
        weekly.index = dc.groupby('week').apply(lambda x: x.index[-1])
        return weekly.dropna()

    def analyze_stock(self, symbol):
        """개별 주식 분석"""
        try:
            company_name = self.get_company_name(symbol)

            end_date   = datetime.now()
            start_date = end_date - timedelta(days=180)

            daily_data = stock.get_market_ohlcv_by_date(
                start_date.strftime("%Y%m%d"),
                end_date.strftime("%Y%m%d"),
                symbol
            )
            # 컬럼명 정규화 (신버전 영문 → 한글)
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
            signal_adx  = None
            days_ago    = None

            if condition1_met and len(condition1_results) > 0:
                last_signal_date = signal_info['dates'].iloc[-1]
                if last_signal_date is not None:
                    signal_date = last_signal_date.strftime('%Y-%m-%d')
                    signal_adx  = signal_info['adx_values'].iloc[-1]
                    days_ago    = (daily_data.index[-1] - last_signal_date).days

            condition2_met   = self.check_condition2_macd(weekly_macd)
            cur_weekly_macd  = weekly_macd.iloc[-1]
            prev_weekly_macd = weekly_macd.iloc[-2] if len(weekly_macd) > 1 else 0

            return {
                'symbol':               symbol,
                'company_name':         company_name,
                'current_price':        daily_data['종가'].iloc[-1],
                'condition1_met':       condition1_met,
                'condition2_met':       condition2_met,
                'both_conditions_met':  condition1_met and condition2_met,
                'current_adx':          adx.iloc[-1],
                'current_di_plus':      di_plus.iloc[-1],
                'current_di_minus':     di_minus.iloc[-1],
                'weekly_macd_current':  cur_weekly_macd,
                'weekly_macd_previous': prev_weekly_macd,
                'macd_change':          cur_weekly_macd - prev_weekly_macd,
                'signal_date':          signal_date,
                'signal_adx':           signal_adx,
                'days_ago':             days_ago,
                'date':                 daily_data.index[-1].strftime('%Y-%m-%d'),
            }

        except Exception:
            return None


# ─────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────
st.title("📈 드림팀 주식 스크리너")
st.markdown("**KRX 공식 데이터 기반 - EMA 방식 ADX/DMI 분석**")

st.sidebar.header("설정")

market_options = st.sidebar.multiselect(
    "시장 선택 (복수 선택 가능)",
    ["KOSPI", "KOSDAQ"],
    default=["KOSPI"]
)

max_stocks = st.sidebar.number_input(
    "최대 분석 종목 수",
    min_value=10,
    max_value=3000,
    value=200,
    step=50
)

if "KOSPI" in market_options and "KOSDAQ" in market_options:
    total_stocks = 2400
elif "KOSPI" in market_options:
    total_stocks = 900
elif "KOSDAQ" in market_options:
    total_stocks = 1500
else:
    total_stocks = 0

estimated_stocks = min(max_stocks, total_stocks)
estimated_time   = estimated_stocks * 0.8
st.sidebar.warning(f"⚠️ 예상: {estimated_stocks}개 종목 / 약 {int(estimated_time/60)}분")

if st.sidebar.button("🔍 스크리닝 시작", type="primary"):
    from pykrx import stock as pykrx_stock
    today = datetime.now().strftime("%Y%m%d")

    all_symbols = []
    if "KOSPI" in market_options:
        kospi_symbols = get_ticker_list("KOSPI")
        all_symbols.extend(kospi_symbols)
        st.info(f"KOSPI {len(kospi_symbols)}개 종목 로드")

    if "KOSDAQ" in market_options:
        kosdaq_symbols = get_ticker_list("KOSDAQ")
        all_symbols.extend(kosdaq_symbols)
        st.info(f"KOSDAQ {len(kosdaq_symbols)}개 종목 로드")

    all_symbols = list(set(all_symbols))

    # 거래대금 기준 대형주 우선 정렬
    st.info("거래대금 기준으로 정렬 중...")
    try:
        sorted_all = []
        for mkt in market_options:
            sorted_mkt = fetch_market_cap_by_volume(all_symbols, market=mkt)
            sorted_all += [s for s in sorted_mkt if s not in sorted_all]
        # 정렬 안 된 나머지 추가
        sorted_all += [s for s in all_symbols if s not in sorted_all]
        sorted_symbols = sorted_all[:max_stocks]
        st.success(f"거래대금 기준 정렬 완료 ({len(sorted_symbols)}개)")
    except Exception as e:
        st.warning(f"정렬 실패: {e}. 원본 순서로 진행합니다.")
        sorted_symbols = all_symbols[:max_stocks]

    screener = DreamTeamScreener()
    screener.stock_symbols = sorted_symbols

    st.success(f"시총 상위 {len(screener.stock_symbols)}개 종목 분석 시작")

    progress_bar = st.progress(0)
    status_text  = st.empty()
    results      = []
    total        = len(screener.stock_symbols)

    # 종목명 미리 로드: 내장 딕셔너리에 없는 종목만 네이버 API 조회
    missing_names = [s for s in screener.stock_symbols if s not in _TICKER_NAMES]
    if missing_names:
        st.info(f"종목명 로딩 중... (미등록 {len(missing_names)}개 네이버 조회)")
        name_progress = st.progress(0)
        for idx, symbol in enumerate(missing_names):
            if symbol not in screener.stock_info or screener.stock_info[symbol] == symbol:
                name = get_ticker_name_naver(symbol)
                if name:
                    screener.stock_info[symbol] = name
            if idx % 20 == 0:
                name_progress.progress((idx + 1) / len(missing_names))
        name_progress.empty()

    for i, symbol in enumerate(screener.stock_symbols):
        company_name = screener.get_company_name(symbol)
        status_text.text(f"분석 중... {i+1}/{total} - {company_name} ({symbol})")
        progress_bar.progress((i + 1) / total)
        result = screener.analyze_stock(symbol)
        if result:
            results.append(result)

    status_text.text("✅ 분석 완료!")

    if results:
        df = pd.DataFrame(results)

        st.info("종목명 업데이트 중...")
        for idx, row in df.iterrows():
            if not row['company_name'] or row['company_name'] == row['symbol']:
                # 1) 내장 딕셔너리
                if row['symbol'] in _TICKER_NAMES:
                    df.at[idx, 'company_name'] = _TICKER_NAMES[row['symbol']]
                else:
                    # 2) 네이버 API
                    name = get_ticker_name_naver(row['symbol'])
                    if name:
                        df.at[idx, 'company_name'] = name

        df = df.sort_values(
            ['both_conditions_met', 'condition1_met', 'condition2_met'],
            ascending=False
        )

        # 두 조건 모두 충족
        both = df[df['both_conditions_met'] == True]
        if len(both) > 0:
            st.success(f"🎯 두 조건 모두 충족: {len(both)}개 종목")
            for _, row in both.iterrows():
                company_name = (
                    _TICKER_NAMES.get(row['symbol'])
                    or get_ticker_name_naver(row['symbol'])
                    or row['company_name']
                )

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

        # 조건 1만 충족
        cond1 = df[(df['condition1_met'] == True) & (df['condition2_met'] == False)]
        if len(cond1) > 0:
            st.warning(f"⚠️ 조건 1만 충족: {len(cond1)}개 종목")
            display_df = cond1[['company_name', 'symbol', 'current_price', 'signal_date', 'days_ago']].copy()
            display_df.columns = ['종목명', '코드', '현재가', '신호일', '경과일']
            st.dataframe(display_df.head(10))

        # 조건 2만 충족
        cond2 = df[(df['condition1_met'] == False) & (df['condition2_met'] == True)]
        if len(cond2) > 0:
            st.info(f"ℹ️ 조건 2만 충족: {len(cond2)}개 종목")
            display_df = cond2[['company_name', 'symbol', 'current_price', 'macd_change']].copy()
            display_df.columns = ['종목명', '코드', '현재가', 'MACD변화']
            st.dataframe(display_df.head(10))

        # CSV 다운로드
        df_download = df.copy()
        for col in ['condition1_met', 'condition2_met', 'both_conditions_met']:
            df_download[col] = df_download[col].map({True: '충족', False: '미충족'})
        for col in ['current_price', 'current_adx', 'current_di_plus', 'current_di_minus',
                    'weekly_macd_current', 'weekly_macd_previous', 'macd_change', 'signal_adx']:
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
