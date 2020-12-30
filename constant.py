#########
# Garbage reason
#########

STOP_PROCESS = "stop process"
BENEISH_LOW_RATE = "beneish low rate"
STOCK_CHANGED_NAME = "stock changed name"
NONE_STANDARD_AUDIT_REPORT = "none standard audit report"
NAG_PROFIT_RECENT_2_YEARS = "negative profit recent two years"
DEBIT_GT_ASSETS = "debit great than assets"
ST_STOCK = "ST stock"

VERY_STONG_DOWN_TREND = "very strong down trend"
VERY_STONG_UP_TREND = "very strong up trend"


###############
# Bar style
###############
BAR_SMALL_BODY = "bar small body"
BAR_CROSS_STAR = "bar cross star"
BAR_GUANG_TOU = "bar guang tou"
BAR_GUANG_JIAO = "bar guang jiao"
BAR_LONG_UPPER_SHADOW = "bar long upper shadow"
BAR_LONG_LOWER_SHADOW = "bar long lower shadow"
BAR_YUNXIAN_BUY = "bar yunxian buy"
BAR_YUNXIAN_SELL = "bar yunxian sell"

BAR_STEADY_UP = "bar steady up"




##############
#  Operation
#############

BUY_EARLY = "BUY_EARLY"
SELL_EARLY = "SELL_EARLY"

BUY_MUST = "BUY_MUST"
SELL_MUST = "SELL_MUST"

BUY_CHECK = "BUY_CHECK" 
SELL_CHECK = "SELL_CHECK"

##############
#  Criterial Price
#############
PRICE_HIGH = "price high"
PRICE_LOW  = "price low"

##############
#  Criterial MA. Close Price CROSS OVER/DOWN
#############
CROSS_OVER_SMA5 = "cross over sma5"
CROSS_OVER_SMA21 = "cross over sma21"
CROSS_OVER_SMA60 = "cross over sma60"

CROSS_DOWN_SMA5  = "cross down sma5"
CROSS_DOWN_SMA21 = "cross down sma21"
CROSS_DOWN_SMA60 = "cross down sma60"

CLOSE_ABOVE_SMA60 = "close above sma60"
CLOSE_UNDER_SMA60 = "close under sma60"
CLOSE_GT_SMA60_5_perc = "close great than 1.05 sma60"
CLOSE_GT_SMA200_20_perc = "close great than 1.2 sma200"

SMA21_UNDER_SMA60 = "sma21 under sma60"


MA_JIN_CHA_MINOR = "ma jin cha minor"
MA_JIN_CHA_MAJOR = "ma jin cha major"

MA_SI_CHA_MINOR = "ma si cha minor"
MA_SI_CHA_MAJOR = "ma si cha major"


MA_DUO_TOU_PAI_LIE = "ma duo tou pai lie"
MA_DUO_TOU_PAI_LIE_N_days = "ma duo tou pai lie n days"
MA_LAST_KONG_TOU_PAI_LIE_N_days = "current is ma dtpl, last ktpl is n days before "


MA_KONG_TOU_PAI_LIE = "ma kong tou pai lie"
MA_KONG_TOU_PAI_LIE_N_days = "ma kong tou pai lie n days"
MA_LAST_DUO_TOU_PAI_LIE_N_days = "current is ma ktpl, last dtpl is n days before "



SHORT_TREND_UP = "short trend up"
SHORT_TREND_DOWN = "short trend down"

MIDDLE_TREND_UP = "middle trend up"
MIDDLE_TREND_DOWN = "middle trend down"

LONG_TREND_UP = "long trend up"
LONG_TREND_DOWN = "long trend down"


##############
# CLOSE above MA5.
# small number indicates starting stage of burst
# large number indicates middle stage of burst
##############

CLOSE_ABOVE_MA5_N_DAYS = "close above ma5 n days"
CLOSE_NEAR_MA5_N_DAYS = "close near ma5 n days"
MA21_NEAR_MA55_N_DAYS = "ma21 near ma55 n days"



##############
# MA55 closing to MA21
##############
MA55_NEAR_MA21 = "ma55 near ma21"

##############
# Critical MA fast cross MA slow
##############
SMA_CROSS_OVER = "SMA FAST cross over SMA SlOW"

##############
#  Criterial MACD
#############

DIF_LT_0 = "dif less than 0"
DIF_GT_0 = "dif great than 0"

SIG_LT_0 = "sig less than 0"
SIG_GT_0 = "sig great than 0"

DIF_CROSS_OVER_0 = "dif cross over 0"
DIF_CROSS_DOWN_0 = "dif cross down 0"

SIG_CROSS_OVER_0 = "sig cross over 0"
SIG_CROSS_DOWN_0 = "sig cross down 0"

MACD_CROSS_OVER_0 = "macd cross over 0"
MACD_CROSS_DOWN_0 = "macd cross down 0"

MACD_DECLINE_NEAR_0 = "macd decline near 0"
MACD_CLIMB_NEAR_0 = "macd climb near 0"

MACD_DIF_MAIN_OVER_0_N_DAYS = "macd dif_main over 0 n days"
MACD_DEA_SIGNAL_OVER_0_N_DAYS = "macd dea_signal over 0 n days"
MACD_HISTOGRAM_OVER_0_N_DAYS = "macd historgram over 0 n days"


DIF_LT_SIG = "dif less than sig" #down trend
DIF_GT_SIG = "dif great than sig" #up trend

DIF_CROSS_OVER_SIG = "dif cross over sig"
DIF_CROSS_DOWN_SIG = "dif cross down sig"

##############
#  Criterial KDJ
#############



#####################
# index candidates
#####################
TO_BE_KEPT = "To_Be_Kept"
TO_BE_REMOVED = "To_Be_Removed"
TO_BE_ADDED = "To_Be_Added"