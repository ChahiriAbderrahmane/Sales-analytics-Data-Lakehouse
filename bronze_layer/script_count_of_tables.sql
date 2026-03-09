spark-sql (silver)> SELECT 'person_address' AS table_name, COUNT(*) AS row_count FROM person_address UNION ALL
                  > SELECT 'person_countryregion', COUNT(*) FROM person_countryregion UNION ALL
                  > SELECT 'person_person', COUNT(*) FROM person_person UNION ALL
                  > SELECT 'person_stateprovince', COUNT(*) FROM person_stateprovince UNION ALL
                  > SELECT 'production_product', COUNT(*) FROM production_product UNION ALL
                  > SELECT 'production_productcategory', COUNT(*) FROM production_productcategory UNION ALL
                  > SELECT 'production_productsubcategory', COUNT(*) FROM production_productsubcategory UNION ALL
                  > SELECT 'sales_currency', COUNT(*) FROM sales_currency UNION ALL
                  > SELECT 'sales_currencyrate', COUNT(*) FROM sales_currencyrate UNION ALL
                  > SELECT 'sales_customer', COUNT(*) FROM sales_customer UNION ALL
                  > SELECT 'sales_salesorderdetail', COUNT(*) FROM sales_salesorderdetail UNION ALL
                  > SELECT 'sales_salesorderheader', COUNT(*) FROM sales_salesorderheader UNION ALL
                  > SELECT 'sales_salesorderheadersalesreason', COUNT(*) FROM sales_salesorderheadersalesreason UNION ALL
                  > SELECT 'sales_salesreason', COUNT(*) FROM sales_salesreason UNION ALL
                  > SELECT 'sales_salesterritory', COUNT(*) FROM sales_salesterritory UNION ALL
                  > SELECT 'sales_specialoffer', COUNT(*) FROM sales_specialoffer UNION ALL
                  > SELECT 'ids_to_close', COUNT(*) FROM ids_to_close UNION ALL
                  > SELECT 'person_address_ranked', COUNT(*) FROM person_address_ranked UNION ALL
                  > SELECT 'person_countryregion_ranked', COUNT(*) FROM person_countryregion_ranked UNION ALL
                  > SELECT 'person_person_ranked', COUNT(*) FROM person_person_ranked UNION ALL
                  > SELECT 'person_stateprovince_ranked', COUNT(*) FROM person_stateprovince_ranked UNION ALL
                  > SELECT 'production_product_ranked', COUNT(*) FROM production_product_ranked UNION ALL
                  > SELECT 'production_productcategory_ranked', COUNT(*) FROM production_productcategory_ranked UNION ALL
                  > SELECT 'production_productsubcategory_ranked', COUNT(*) FROM production_productsubcategory_ranked UNION ALL
                  > SELECT 'sales_customer_ranked', COUNT(*) FROM sales_customer_ranked UNION ALL
                  > SELECT 'sales_salesreason_ranked', COUNT(*) FROM sales_salesreason_ranked UNION ALL
                  > SELECT 'sales_salesterritory_ranked', COUNT(*) FROM sales_salesterritory_ranked UNION ALL
                  > SELECT 'sales_specialoffer_ranked', COUNT(*) FROM sales_specialoffer_ranked;




-- person_address  19614
-- person_countryregion    238
-- person_person   19972
-- person_stateprovince    181
-- production_product      504
-- production_productcategory      4
-- production_productsubcategory   37
-- sales_currency  105
-- sales_currencyrate      13532
-- sales_customer  19820
-- sales_salesorderdetail  121317
-- sales_salesorderheader  31465
-- sales_salesorderheadersalesreason       27647
-- sales_salesreason       10
-- sales_salesterritory    10
-- sales_specialoffer      16
-- ids_to_close    0
-- person_address_ranked   19614
-- person_countryregion_ranked     238
-- person_person_ranked    19972
-- person_stateprovince_ranked     181
-- production_product_ranked       504
-- production_productcategory_ranked       4
-- production_productsubcategory_ranked    37
-- sales_customer_ranked   19820
-- sales_salesreason_ranked        10
-- sales_salesterritory_ranked     10
-- sales_specialoffer_ranked       16
-- Time taken: 28.22 seconds, Fetched 28 row(s)
--...
-- spark-sql (silver)>