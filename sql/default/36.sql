-- query36
SELECT sum(ss_net_profit) / sum(ss_ext_sales_price)                 AS
               gross_margin,
               i_category,
               i_class,
               rank()
                 OVER (
                   PARTITION BY i_category, i_class
                   ORDER BY sum(ss_net_profit)/sum(ss_ext_sales_price) ASC) AS
               rank_within_parent
FROM   tpcds.store_sales,
       tpcds.date_dim d1,
       tpcds.item,
       tpcds.store
WHERE  d1.d_year = 2000
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN ( 'TN', 'TN', 'TN', 'TN',
                        'TN', 'TN', 'TN', 'TN' )
GROUP  BY i_category, i_class
ORDER  BY i_category,
          rank_within_parent
LIMIT 100
