from netsuite.pipeline.netsuite import ACCOUNTS, TRANSACTION_LINES

static = [
    ACCOUNTS,
]

time_dynamic = [
    TRANSACTION_LINES,
]

id_dynamic = []

static_pipelines = {i.pipeline.name: i.pipeline for i in static}

static_pipelines, time_dynamic_pipelines, id_dynamic_pipelines = [
    {i.pipeline.name: i.pipeline for i in group}
    for group in [
        static,
        time_dynamic,
        id_dynamic,
    ]
]

pipelines = static_pipelines | time_dynamic_pipelines | id_dynamic_pipelines
