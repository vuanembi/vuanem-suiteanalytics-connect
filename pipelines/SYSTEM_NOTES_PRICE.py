from pipelines import NetSuiteJob


class SYSTEM_NOTES_PRICE(NetSuiteJob):
    query = """
    SELECT 
        SYSTEM_NOTES.DATE_CREATED, 
        SYSTEM_NOTES.ITEM_ID,
        SYSTEM_NOTES.VALUE_NEW,
        SYSTEM_NOTES.OPERATION
    FROM "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".SYSTEM_NOTES
    WHERE SYSTEM_NOTES.STANDARD_FIELD = 'RATE'
    AND SYSTEM_NOTES.ITEM_ID IS NOT NULL
    """

    schema = [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "VALUE_NEW", "type": "STRING"},
        {"name": "OPERATION", "type": "STRING"},
    ]
