from pipelines import NetSuiteJob


class DELIVERY_PERSON(NetSuiteJob):
    query = """
    SELECT 
        DATE_CREATED,
        DELIVERY_PERSON_EXTID,
        DELIVERY_PERSON_ID,
        DELIVERY_PERSON_NAME,
        IS_INACTIVE,
        LAST_MODIFIED_DATE,
        REF__EMPLOYEE_ID,
        VN_CODE
    FROM "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".DELIVERY_PERSON
    """

    schema = [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DELIVERY_PERSON_EXTID", "type": "STRING"},
        {"name": "DELIVERY_PERSON_ID", "type": "INTEGER"},
        {"name": "DELIVERY_PERSON_NAME", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "REF_EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "VN_CODE", "type": "STRING"},
    ]
