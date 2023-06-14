#working visual.py
import plotly.graph_objs as go
import plotly.offline as pyo

# Declare figure and trace outside the function
fig = go.Figure()
trace = go.Candlestick(x=[], open=[], high=[], low=[], close=[], name='market data')
fig.add_trace(trace)

# Set up web server
web_dir = "web"
html_file = "/Users/Rohaan/Desktop/DE/Data_Engineering_Project/chart.html"

def update_chart(df):
    if not df.empty:
        # Update the trace data with the new candlestick data
        fig.data[0].x = df.index
        fig.data[0].open = df['Open']
        fig.data[0].high = df['High']
        fig.data[0].low = df['Low']
        fig.data[0].close = df['Close']

        # Update the layout if necessary
        fig.update_layout(
            title='Bitcoin live share price evolution',
            yaxis_title='Bitcoin Price (kUS Dollars)'
        )

        # Update the X-Axes if necessary
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=15, label="15m", step="minute", stepmode="backward"),
                    dict(count=45, label="45m", step="minute", stepmode="backward"),
                    dict(count=1, label="HTD", step="hour", stepmode="todate"),
                    dict(count=6, label="6h", step="hour", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )

        # Save the updated graph as an HTML file
        pyo.plot(fig, filename = html_file, auto_open=False)

    else:
        print('No data available')
