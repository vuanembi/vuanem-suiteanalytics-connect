SELECT
	VENDORS.VENDOR_ID,
	VENDORS.NAME,
	VENDORS.FULL_NAME,
	VENDOR_TYPES.NAME AS 'VENDOR_TYPE'
FROM
	"Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".VENDORS
	LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".VENDOR_TYPES ON VENDORS.VENDOR_TYPE_ID = VENDOR_TYPES.VENDOR_TYPE_ID
