-- set interval for margin calculation
-- set @from_date := "";
-- set @to_date := "";


SELECT ad_type, payment_type, concat(round(((price-payment_cost)/price )*100, 2),'%') AS margin,  @from_date as Date_from , @to_date as Date_to
FROM Classifieds 
WHERE (created_at <= @to_date AND created_at >= @from_date) AND payment_type IS NOT NULL 
GROUP BY ad_type,payment_type;

