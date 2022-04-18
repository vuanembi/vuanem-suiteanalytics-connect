from netsuite.pipeline.netsuite import (
    accounts as netsuite__accounts,
    address as netsuite__address,
    address_book as netsuite__address_book,
    budget as netsuite__budget,
    campaigns as netsuite__campaigns,
    cases as netsuite__cases,
    casetype as netsuite__casetype,
    cityprovince_list as netsuite__cityprovince_list,
    classes as netsuite__classes,
    customers as netsuite__customers,
    deleted_records as netsuite__deleted_records,
    delivery_person as netsuite__delivery_person,
    departments as netsuite__departments,
    employees as netsuite__employees,
    entity as netsuite__entity,
    ir as netsuite__ir,
    items as netsuite__items,
    item_location_map as netsuite__item_location_map,
    locations as netsuite__locations,
    loyalty_customer_group as netsuite__loyalty_customer_group,
    loyalty_transaction as netsuite__loyalty_transaction,
    originating_leads as netsuite__originating_leads,
    promotion_sms_intergration as netsuite__promotion_sms_intergration,
    purchase_order as netsuite__purchase_order,
    quanhuyen_list as netsuite__quanhuyen_list,
    rating as netsuite__rating,
    service_addon_so_map as netsuite__service_addon_so_map,
    service_addon_to_map as netsuite__service_addon_to_map,
    store_traffic as netsuite__store_traffic,
    support_person_map as netsuite__support_person_map,
    system_notes_create as netsuite__system_notes_create,
    system_notes_price as netsuite__system_notes_price,
    transactions as netsuite__transactions,
    transactions_due_date as netsuite__transactions_due_date,
    transaction_lines as netsuite__transaction_lines,
    vendors as netsuite__vendors,
)
from netsuite.pipeline.netsuite2 import (
    coupon_code as netsuite2__coupon_code,
    promotion_code as netsuite2__promotion_code,
    promotion_code_currency as netsuite2__promotion_code_currency,
    transaction as netsuite2__transaction,
    transaction_line as netsuite2__transaction_line,
    tran_promotion as netsuite2__tran_promotion,
)


static = [
    netsuite__accounts,
    netsuite__budget,
    netsuite__campaigns,
    netsuite__casetype,
    netsuite__classes,
    netsuite__delivery_person,
    netsuite__departments,
    netsuite__ir,
    netsuite__item_location_map,
    netsuite__purchase_order,
    netsuite__system_notes_price,
    netsuite__transactions_due_date,
    netsuite__vendors,
    netsuite2__promotion_code_currency,
    netsuite2__promotion_code,
]

time_dynamic = [
    netsuite__address_book,
    netsuite__address,
    netsuite__cases,
    netsuite__cityprovince_list,
    netsuite__customers,
    netsuite__deleted_records,
    netsuite__employees,
    netsuite__entity,
    netsuite__items,
    netsuite__locations,
    netsuite__loyalty_customer_group,
    netsuite__loyalty_transaction,
    netsuite__originating_leads,
    netsuite__promotion_sms_intergration,
    netsuite__rating,
    netsuite__quanhuyen_list,
    netsuite__service_addon_so_map,
    netsuite__service_addon_to_map,
    netsuite__store_traffic,
    netsuite__support_person_map,
    netsuite__system_notes_create,
    netsuite__transaction_lines,
    netsuite__transactions,
    netsuite2__tran_promotion,
    netsuite2__transaction_line,
    netsuite2__transaction,
]

id_dynamic = [
    netsuite2__coupon_code,
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
