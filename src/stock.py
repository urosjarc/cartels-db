import yfinance as yf
import matplotlib.pyplot as plt

def plotStock(tickerCode: list):
    ticker = yf.Ticker(tickerCode)
    ticker.history('5y')['Close'].plot()

    title = tickerCode
    if isinstance(ticker, list):
        title = ' '.join(tickerCode)

    plt.title(title, fontsize=16)

    # Define the labels for x-axis and y-axis
    plt.ylabel('Close', fontsize=14)
    plt.xlabel('Date', fontsize=14)

    # Plot the grid lines
    plt.grid(which="major", color='k', linestyle='-.', linewidth=0.5)
    plt.show()
