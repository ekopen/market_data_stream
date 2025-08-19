import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import base64
from dashboard_func import load_ticks_db, load_diagnostics, plot_price, plot_ticks_per_second, plot_websocket_lag, plot_processing_lag
from config import DIAGNOSTIC_FREQUENCY

# Create Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Ethereum Price Dashboard"

# Layout
app.layout = dbc.Container([
    html.H1("Real Time Ethereum Price Feed", className="my-3"),

    dcc.Interval(
        id='interval-component',
        interval=3000,  # refresh every 3 sec
        n_intervals=0
    ),

    dcc.Tabs(id="tabs", value='ticks', children=[
        dcc.Tab(label='Ticks Data', value='ticks'),
        dcc.Tab(label='Diagnostics', value='diagnostics'),
    ]),

    html.Div(id='tab-content', className="mt-4")
], fluid=True)


@app.callback(
    Output('tab-content', 'children'),
    Input('tabs', 'value'),
    Input('interval-component', 'n_intervals')
)
def render_content(tab, n):
    if tab == 'ticks':
        df, df_display = load_ticks_db()
        if df.empty:
            return html.Div("No data available")

        stats_text = f"Rows: {len(df):,} | Memory: {df.memory_usage(deep=True).sum() / 1_048_576:.2f} MB"
        csv_string = df.to_csv(index=False, encoding='utf-8')
        b64 = base64.b64encode(csv_string.encode()).decode()
        href = f"data:text/csv;base64,{b64}"

        return dbc.Container([
            html.P("After ingestion from the websocket/Kafka, our data gets staged in ClickHouse, "
                   "a database management system optimized for speed and real time analytics."),
            html.P("Data stays in this table and is eventually moved to cold storage."),
            html.P(stats_text, className="fw-bold"),

            dcc.Graph(figure=plot_price(df_display, "Live Data Feed")),

            html.A("Download Tick Data as CSV", href=href, download="tick_data.csv",
                   className="btn btn-primary my-3"),

            html.H4("Recent Data Sample"),
            dash_table.DataTable(
                data=df.head(10).to_dict('records'),
                columns=[{"name": c, "id": c} for c in df.head(10).columns],
                page_size=10,
                style_table={'overflowX': 'auto'},
                style_cell={'textAlign': 'left'}
            )
        ], fluid=True)

    elif tab == 'diagnostics':
        websocket_df = load_diagnostics("websocket_diagnostics", limit=100, order_col="diagnostics_timestamp")
        processing_df = load_diagnostics("processing_diagnostics", limit=100, order_col="diagnostics_timestamp")

        components = []
        if not websocket_df.empty:
            ticks_per_sec = websocket_df['message_count'].mean() / DIAGNOSTIC_FREQUENCY
            avg_websocket_lag = websocket_df['avg_websocket_lag'].mean()

            components.append(dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardHeader("Average Ticks per Second"),
                    dbc.CardBody(html.H4(f"{round(ticks_per_sec, 2)}"))
                ]), width=4),
                dbc.Col(dbc.Card([
                    dbc.CardHeader("Average WebSocket Lag (s)"),
                    dbc.CardBody(html.H4(f"{round(avg_websocket_lag, 2)}"))
                ]), width=4),
            ], className="mb-4"))

            components.append(dcc.Graph(figure=plot_ticks_per_second(websocket_df, "Average Ticks per Second")))
            components.append(dcc.Graph(figure=plot_websocket_lag(websocket_df, "Average WebSocket Lag (in Seconds)")))

        if not processing_df.empty:
            avg_processing_lag = processing_df['avg_processing_lag'].mean()

            components.append(dbc.Card([
                dbc.CardHeader("Average Processing Lag (s)"),
                dbc.CardBody(html.H4(f"{round(avg_processing_lag, 2)}"))
            ], className="mb-4"))

            components.append(dcc.Graph(figure=plot_processing_lag(processing_df, "Average Processing Lag (in Seconds)")))

        return dbc.Container(components, fluid=True)


if __name__ == '__main__':
    app.run(debug=True)
