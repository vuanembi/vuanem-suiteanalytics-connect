from netsuite.pipeline.netsuite import accounts, transaction_lines
from netsuite.pipeline.netsuite2 import coupon_code

static = [
    accounts,
]

time_dynamic = [
    transaction_lines,
]

id_dynamic = [
    coupon_code,
]

static_pipelines, time_dynamic_pipelines, id_dynamic_pipelines = [
    {i.pipeline.name: i.pipeline for i in group}
    for group in [
        static,
        time_dynamic,
        id_dynamic,
    ]
]

pipelines = static_pipelines | time_dynamic_pipelines | id_dynamic_pipelines
