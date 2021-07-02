SELECT 
	a.couponcode,
	a.eligiblefreegifts,
	a.freegiftsadded,
	a.promocode,
	a.promotiontype,
	a.purchasediscount,
	a.shippingdiscount,
	a.transaction,
	b.lastmodifieddate
FROM tranPromotion a
LEFT JOIN transaction b on b.id = a.transaction
