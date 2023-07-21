import requests

res = requests.get("https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=1657738157&period2=1689274157&interval=1d&events=history&includeAdjustedClose=true", headers={
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
})
print(res.content)

