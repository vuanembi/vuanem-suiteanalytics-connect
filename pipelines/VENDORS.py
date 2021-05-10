from pipelines import NetSuiteJob

class VENDORS(NetSuiteJob):
    query = """
    SELECT
        VENDORS.VENDOR_ID,
        VENDORS.NAME,
        VENDORS.FULL_NAME,
        VENDOR_TYPES.NAME AS 'VENDOR_TYPE'
    FROM
        "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".VENDORS
    LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".VENDOR_TYPES ON
        VENDORS.VENDOR_TYPE_ID = VENDOR_TYPES.VENDOR_TYPE_ID
    """

    schema = [
        {
            "name": "VENDOR_ID",
            "type": "INTEGER"
        },
        {
            "name": "NAME",
            "type": "STRING"
        },
        {
            "name": "FULL_NAME",
            "type": "STRING"
        },
        {
            "name": "VENDOR_TYPE",
            "type": "STRING"
        }
    ]
