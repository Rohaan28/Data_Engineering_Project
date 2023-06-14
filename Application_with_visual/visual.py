import plotly.graph_objs as go

def update_chart(data):
    # Declare figure
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(x=data['Date'],
                    open=data['Open'],
                    high=data['High'],
                    low=data['Low'],
                    close=data['Close'], name='market data'))

    # Add titles
    fig.update_layout(
        title='Uber live share price evolution',
        yaxis_title='Uber Price (kUS Dollars)')

    # X-Axes
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

    # Show the chart
    fig.show()
