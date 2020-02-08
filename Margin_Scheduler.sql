--  SCHEDULER 

CREATE EVENT `Margin_per_hour` ON SCHEDULE
        EVERY 1 HOUR
    STARTS CONVERT_TZ(NOW(),'SYSTEM','EET') + INTERVAL 1 HOUR 
    COMMENT 'Calculate the Margin per ad type and classified type every hour'
    DO BEGIN
INSERT INTO Margin_per_hour(ad_type, payment_type, margin, date)
SELECT ad_type, payment_type, concat(round (((price-payment_cost)/price )*100,2),'%') AS margin, CONVERT_TZ(NOW(),'SYSTEM','EET')
FROM Classifieds
WHERE (created_at >= CONVERT_TZ(NOW(),'SYSTEM','EET') - INTERVAL 1 HOUR AND created_at <= CONVERT_TZ(NOW(),'SYSTEM','EET')) AND payment_type IS NOT NULL
END