#https://quant.stackexchange.com/questions/26078/how-can-one-query-the-google-finance-api-for-dow-jones-and-sp-500-values
cd /home/ryan/DATA/pickle/DOW_SP
curl -k -o dow.csv 'https://stooq.com/q/d/l/?s=^dji&i=d'
curl -k -o sp500.csv 'https://stooq.com/q/d/l/?s=^spx&i=d'
