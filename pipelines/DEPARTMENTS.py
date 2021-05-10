from pipelines import NetSuiteJob


class DEPARTMENTS(NetSuiteJob):
    query = """
        SELECT
        DATE_LAST_MODIFIED,
        DEPARTMENT_DESCRIPTION,
        DEPARTMENT_EXTID,
        DEPARTMENT_ID,
        FULL_NAME,
        ISINACTIVE,
        IS_INCLUDING_CHILD_SUBS,
        NAME,
        PARENT_ID
    FROM
        "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".DEPARTMENTS
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
