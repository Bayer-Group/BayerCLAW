import datetime as dt
import logging
from textwrap import dedent

import awswrangler as wr
import boto3
import dash
from dash import html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
import pandas as pd

logger = logging.getLogger()

TS_TABLE = "bclawTest.bclawTable"
SESSION = boto3.Session(profile_name="bclaw-public")


def get_workflow_df() -> pd.DataFrame:
    qry = dedent(f"""\
        SELECT DISTINCT workflow_name, min(time) AS first, max(time) AS last
        FROM {TS_TABLE}
        GROUP BY workflow_name 
        ORDER BY workflow_name ASC
    """)
    ret = wr.timestream.query(qry, boto3_session=SESSION)
    ret.set_index("workflow_name", inplace=True)
    # times received are UTC, convert to local for display
    ret["first"] = ret["first"] #.dt.floor("d")
    ret["last"] = ret["last"] #.dt.floor("d")
    return ret
    # return ret["workflow_name"].tolist()


workflow_df = get_workflow_df()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "todo"

wf_picker = dmc.Select(
    id="wf_picker",
    label="workflow",
    data=workflow_df.index.tolist(),
    # data=[
    #     "work1",
    #     "thang2",
    #     "zener3",
    # ],
    searchable=True,
    clearable=True
)


# how to combine date picker and hour picker to get a datetime
def date_time_thing() -> dt.datetime:
    d = dt.date.today()
    t = dt.time(12, 34)
    ret = dt.datetime.combine(d, t)  # todo: timezone
    return ret


def hour_picker_items() -> list:
    hours = (dt.time(hour=h) for h in range(24))
    ret = [{"label": h.strftime("%H:%M"), "value": h} for h in hours]
    return ret


hour_picker = dmc.Select(
    id="hour_picker",
    label="time",
    data=hour_picker_items(),
    searchable=True,
    clearable=True
)


app.layout = dmc.Container(
    dmc.Group(
        [
            wf_picker,
            hour_picker,
            html.Div(id="dummy")
        ],
        direction="column"
    )
)


@app.callback(
    [
        Output("dummy", "children"),
    ],
    [
        Input("wf_picker", "value"),
        Input("hour_picker", "value"),
    ]
)
def handle_it(wf_name, hour):
    logger.info(f"wf: {wf_name}")
    logger.info(f"hour: {str(hour)}")
    if isinstance(hour, dt.time):
        logger.info("time!")
    elif isinstance(hour, str):
        logger.info("string.")
    return ["haha"]


if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)

    app.run_server(debug=True, port=8050, host="127.0.0.1")
