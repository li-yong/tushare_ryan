#https://quant.stackexchange.com/questions/26078/how-can-one-query-the-google-finance-api-for-dow-jones-and-sp-500-values
mkdir /home/ryan/DATA/DAY_Global/US_INDEX
cd /home/ryan/DATA/DAY_Global/US_INDEX
curl -k -o dow.csv 'https://stooq.com/q/d/l/?s=^dji&i=d'
curl -k -o sp500.csv 'https://stooq.com/q/d/l/?s=^spx&i=d'
