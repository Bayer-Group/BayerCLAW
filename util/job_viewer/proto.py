import datetime as dt
import logging
from textwrap import dedent
from typing import Generator

import awswrangler as wr
import boto3
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
import pandas as pd

logger = logging.getLogger()

# https://dash-mantine-components.herokuapp.com/

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

# date_input = dbc.Col(
#     dmc.DatePicker(label="date", firstDayOfWeek="sunday")
# )

# time_input = dbc.Col(
#     dmc.TimeInput(label="time", format="12")
# )

wf_picker = dmc.Select(
    id="wf_picker",
    label="workflow",
    data = workflow_df.index.tolist(),
    # data=[
    #     "work1",
    #     "thang2",
    #     "zener3",
    # ],
    searchable=True,
    clearable=True
)

bgn = dmc.Group(
    [
        dmc.DatePicker(id="begin_date", label="date", firstDayOfWeek="sunday"),
        dmc.TimeInput(id="begin_time", label="time", format="12")
    ],
    direction="row"
)

end = dmc.Group(
    [
        dmc.DatePicker(id="end_date", label="date", firstDayOfWeek="sunday"),
        dmc.TimeInput(id="end_time", label="time", format="12")
    ],
    direction="row"
)

# row1 = dbc.Row(
#     [
#         date_input,
#         time_input
#     ]
# )

app.layout = dmc.Container(
    dmc.Group(
        [
            wf_picker,
            html.Div(
                [
                    dbc.Label("begin"),
                    bgn
                    # row1
                ]
            ),
            html.Div(
                [
                    dbc.Label("end"),
                    end
                ]
            ),
            html.Div(
                dmc.Button("Submit", id="submit")
            ),
            html.Div(id="dummy")
        ],
        direction="column"
    )
)


@app.callback(
    [
        Output("begin_date", "minDate"),
        Output("begin_date", "maxDate"),
        Output("begin_date", "value"),
        Output("begin_time", "value"),
        Output("end_date", "minDate"),
        Output("end_date", "maxDate"),
        Output("end_date", "value"),
        Output("end_time", "value"),
        Output("submit", "disabled"),
    ],
        Input("wf_picker", "value"),
)
def handle_stuff(wf_name):
    logger.info(wf_name)

    if wf_name is not None:
        # chained callbax to update time when date changes?
        # https://dash.plotly.com/basic-callbacks
        min_date = workflow_df.loc[wf_name, "first"]
        max_date = workflow_df.loc[wf_name, "last"]
        # begin_time = max_date
        # end_time = begin_time + dt.timedelta(seconds=86399)

        # todo: might be better to leave times optional
        # todo: or use a select box to pick an hour
        begin_time = min_date
        end_time = max_date

        # logger.info(f"\t{min_date}")
        # logger.info(f"\t{max_date}")
        submit_disabled = False
    else:
        min_date = max_date = begin_time = end_time = None
        submit_disabled = True

    # logger.info(str(bgn_date))
    # logger.info(str(bgn_time))
    # logger.info(str(end_date))
    # logger.info(str(end_time))

    return (
        min_date, max_date, min_date, begin_time,
        min_date, max_date, max_date, end_time,
        submit_disabled
    )


# @app.callback(
#     Output("begin_time", "value"),
#     [
#         Input("begin_date", "value"),
#         State("begin_time", "value"),
#     ]
# )
# def handle_begin_date(bgn_date: dt.date, old_bgn_time: dt.datetime) -> dt.datetime:
#     if old_bgn_time is not None:
#         nu_bgn_time = old_bgn_time.replace(year=bgn_date.year, month=bgn_date.month, day=bgn_date.day)
#         logger.info(f"nu_bgn_time: {nu_bgn_time.isoformat()}")
#     else:
#         nu_bgn_time = None
#     return nu_bgn_time


@app.callback(
    Output("dummy", "children"),
    [
        Input("submit", "n_clicks"),
        State("wf_picker", "value"),
        State("begin_date", "value"),
        State("begin_time", "value"),
        State("end_date", "value"),
        State("end_time", "value"),
    ]
)
def handle_moar_stuff(clix, wf_name, bgn_date, bgn_time, end_date, end_time):
    logger.info(clix)
    logger.info(wf_name)
    logger.info(bgn_date)
    logger.info(bgn_time)
    logger.info(end_date)
    logger.info(end_time)
    return "hey"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    app.run_server(debug=True, port=8050, host="127.0.0.1")

# maybe
# don't set date values when wf is picked
# on submit:
#   both dates blank: get today's executions (or most recent day)
#   one date blank: get that day's executions
#   two days filled: get all executions in range (if begin <= end)
# time handling
#   both dates blank:
#     no times: get today's executions
#     begin only: get today's executions begin -> now
#     end only: get today's executions midnight -> end
#   one date blank
#     same as both dates blank, but on the specified day
#   both dated filled:
#     no times: get all executions for days in range
#     begin set: set lower bound time
#     end set: set upper bound time
#     both set: both bounds

# moar maybe
#   pick only one date
#   optionally pick an hour 00 - 24
