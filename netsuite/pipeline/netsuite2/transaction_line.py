from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ns2_transactionLine",
    [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "ACCOUNT_ID", "type": "INTEGER"},
        {"name": "netamount", "type": "FLOAT"},
        {"name": "rate", "type": "FLOAT"},
        {"name": "rateamount", "type": "FLOAT"},
        {"name": "linelastmodifieddate", "type": "TIMESTAMP"},
        {"name": "ratepercent", "type": "FLOAT"},
        {"name": "actualshipdate", "type": "TIMESTAMP"},
        {"name": "amortizationenddate", "type": "TIMESTAMP"},
        {"name": "amortizationresidual", "type": "STRING"},
        {"name": "amortizationsched", "type": "INTEGER"},
        {"name": "amortizstartdate", "type": "TIMESTAMP"},
        {"name": "billeddate    ", "type": "TIMESTAMP"},
        {"name": "billingschedule", "type": "INTEGER"},
        {"name": "billvariancestatus", "type": "STRING"},
        {"name": "blandedcost", "type": "STRING"},
        {"name": "category", "type": "INTEGER"},
        {"name": "class", "type": "INTEGER"},
        {"name": "cleared", "type": "STRING"},
        {"name": "cleareddate", "type": "TIMESTAMP"},
        {"name": "commitinventory", "type": "INTEGER"},
        {"name": "commitmentfirm", "type": "STRING"},
        {"name": "costestimate", "type": "FLOAT"},
        {"name": "costestimaterate", "type": "FLOAT"},
        {"name": "costestimatetype", "type": "STRING"},
        {"name": "createdfrom", "type": "INTEGER"},
        {"name": "creditforeignamount", "type": "FLOAT"},
        {'name': 'custcol1', 'type': 'STRING'},
        {"name": "custcol_5892_eutriangulation", "type": "STRING"},
        {"name": "custcol_adjustment_field", "type": "STRING"},
        {"name": "custcol_adjustment_tax_code", "type": "INTEGER"},
        {"name": "custcol_amount_before_discount", "type": "FLOAT"},
        {"name": "custcol_amount_discount", "type": "FLOAT"},
        {"name": "custcol_asset_type", "type": "INTEGER"},
        {"name": "custcol_counterparty_vat", "type": "STRING"},
        {"name": "custcol_country_of_origin_code", "type": "STRING"},
        {"name": "custcol_country_of_origin_name", "type": "STRING"},
        {"name": "custcol_cseg_new_segment", "type": "INTEGER"},
        {"name": "custcol_deliver_location", "type": "INTEGER"},
        {"name": "custcol_delivery_method", "type": "INTEGER"},
        {"name": "custcol_emirate", "type": "INTEGER"},
        {"name": "custcol_expected_delivery_date_cols", "type": "TIMESTAMP"},
        {"name": "custcol_expense_code_of_supply", "type": "INTEGER"},
        {"name": "custcol_far_trn_relatedasset", "type": "INTEGER"},
        {"name": "custcol_import_shipment_ref", "type": "INTEGER"},
        {"name": "custcol_invoice_date", "type": "TIMESTAMP"},
        {"name": "custcol_item_group_promotion", "type": "INTEGER"},
        {"name": "custcol_item_type", "type": "STRING"},
        {"name": "custcol_ma_hang_hoa", "type": "STRING"},
        {"name": "custcol_memo", "type": "STRING"},
        {"name": "custcol_memo2", "type": "STRING"},
        {"name": "custcol_nature_of_transaction_codes", "type": "INTEGER"},
        {"name": "custcol_nondeductible_account", "type": "INTEGER"},
        {"name": "custcol_old_item_code", "type": "STRING"},
        {"name": "custcol_percent_discount", "type": "STRING"},
        {"name": "custcol_rate_before_discount", "type": "FLOAT"},
        {"name": "custcol_return_selling_price", "type": "FLOAT"},
        {"name": "custcol_scv_odoo_line_id", "type": "INTEGER"},
        {"name": "custcol_statistical_procedure_purc", "type": "INTEGER"},
        {"name": "custcol_statistical_procedure_sale", "type": "INTEGER"},
        {"name": "custcol_statistical_value", "type": "FLOAT"},
        {"name": "custcol_statistical_value_base_curr", "type": "FLOAT"},
        {"name": "custcol_tei_asset_life_time", "type": "FLOAT"},
        {"name": "custcolexpense_report_invoiceno", "type": "STRING"},
        {"name": "custcolexpense_report_invoicepref", "type": "STRING"},
        {"name": "custcolexpense_report_vendor", "type": "INTEGER"},
        {"name": "custcoltype_of_costs", "type": "INTEGER"},
        {"name": "debitforeignamount", "type": "FLOAT"},
        {"name": "department", "type": "INTEGER"},
        {"name": "documentnumber", "type": "STRING"},
        {"name": "donotdisplayline", "type": "STRING"},
        {"name": "eliminate", "type": "STRING"},
        {"name": "entity", "type": "INTEGER"},
        {"name": "estgrossprofit", "type": "FLOAT"},
        {"name": "estgrossprofitpercent", "type": "FLOAT"},
        {"name": "estimatedamount", "type": "FLOAT"},
        {"name": "excludefrompredictiverisk", "type": "STRING"},
        {"name": "expectedreceiptdate", "type": "TIMESTAMP"},
        {"name": "expectedshipdate", "type": "TIMESTAMP"},
        {"name": "expenseaccount", "type": "INTEGER"},
        {"name": "foreignamount", "type": "FLOAT"},
        {"name": "foreignamountpaid", "type": "FLOAT"},
        {"name": "foreignamountunpaid", "type": "FLOAT"},
        {"name": "foreignpaymentamountunused", "type": "FLOAT"},
        {"name": "foreignpaymentamountused", "type": "FLOAT"},
        {"name": "fulfillable", "type": "STRING"},
        {"name": "fxamountlinked", "type": "FLOAT"},
        {"name": "hasfulfillableitems", "type": "STRING"},
        {"name": "id", "type": "INTEGER"},
        {"name": "isbillable", "type": "STRING"},
        {"name": "isclosed", "type": "STRING"},
        {"name": "iscogs", "type": "STRING"},
        {"name": "iscustomglline", "type": "STRING"},
        {"name": "isfullyshipped", "type": "STRING"},
        {"name": "isfxvariance", "type": "STRING"},
        {"name": "isinventoryaffecting", "type": "STRING"},
        {"name": "isrevrectransaction", "type": "STRING"},
        {"name": "item", "type": "INTEGER"},
        {"name": "itemtype", "type": "STRING"},
        {"name": "kitcomponent", "type": "STRING"},
        {"name": "landedcostcategory", "type": "INTEGER"},
        {"name": "landedcostperline", "type": "STRING"},
        {"name": "linelastmodifieddate", "type": "TIMESTAMP"},
        {"name": "linesequencenumber", "type": "INTEGER"},
        {"name": "location", "type": "INTEGER"},
        {"name": "mainline", "type": "STRING"},
        {"name": "matchbilltoreceipt", "type": "STRING"},
        {"name": "memo", "type": "STRING"},
        {"name": "netamount", "type": "FLOAT"},
        {"name": "oldcommitmentfirm", "type": "STRING"},
        {"name": "orderpriority", "type": "FLOAT"},
        {"name": "paymentmethod", "type": "INTEGER"},
        {"name": "price", "type": "INTEGER"},
        {"name": "processedbyrevcommit", "type": "STRING"},
        {"name": "quantity", "type": "FLOAT"},
        {"name": "quantitybackordered", "type": "FLOAT"},
        {"name": "quantitybilled", "type": "FLOAT"},
        {"name": "quantitycommitted", "type": "FLOAT"},
        {"name": "quantitypacked", "type": "FLOAT"},
        {"name": "quantitypicked", "type": "FLOAT"},
        {"name": "quantityrejected", "type": "FLOAT"},
        {"name": "quantityshiprecv", "type": "FLOAT"},
        {"name": "rate", "type": "FLOAT"},
        {"name": "rateamount", "type": "FLOAT"},
        {"name": "ratepercent", "type": "FLOAT"},
        {"name": "requestnote", "type": "STRING"},
        {"name": "revenueelement", "type": "INTEGER"},
        {"name": "subsidiary", "type": "INTEGER"},
        {"name": "taxline", "type": "STRING"},
        {"name": "transaction", "type": "INTEGER"},
        {"name": "transactiondiscount", "type": "STRING"},
        {"name": "transactionlinetype", "type": "STRING"},
        {"name": "transferorderitemlineid", "type": "INTEGER"},
        {"name": "uniquekey", "type": "INTEGER"},
        {"name": "units", "type": "INTEGER"},
        {"name": "vsoeisestimate", "type": "STRING"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda tr: f"""
        SELECT
            transaction AS TRANSACTION_ID,
            expenseaccount AS ACCOUNT_ID,
            netamount,
            rate,
            rateamount,
            linelastmodifieddate,
            ratepercent
        FROM
            transactionLine
        WHERE
            linelastmodifieddate >= TO_DATE('{tr[0]}', 'YYYY-MM-DD HH24:MI:SS')
            AND linelastmodifieddate <= TO_DATE('{tr[1]}', 'YYYY-MM-DD HH24:MI:SS')
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["TRANSACTION_ID"],
        cursor_key=["linelastmodifieddate"],
    ),
    load_callback_fn=update,
)
