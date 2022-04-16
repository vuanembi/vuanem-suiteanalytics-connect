from netsuite.pipeline.netsuite import ACCOUNTS, TRANSACTION_LINES

pipelines = {
    i.name: i
    for i in [
        ACCOUNTS.pipeline,
        TRANSACTION_LINES.pipeline,
    ]
}
