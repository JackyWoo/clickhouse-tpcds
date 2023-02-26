-- query86
SELECT sum(ws_net_paid)                         AS total_sum,
               i_category,
               i_class,
               rank()
                 OVER (
                   PARTITION BY i_category, i_class
                   ORDER BY sum(ws_net_paid) DESC)      AS rank_within_parent
FROM   tpcds.web_sales,
       tpcds.date_dim d1,
       tpcds.item
WHERE  d1.d_month_seq BETWEEN 1183 AND 1183 + 11
       AND d1.d_date_sk = ws_sold_date_sk
       AND i_item_sk = ws_item_sk
GROUP  BY i_category, i_class
ORDER  BY i_category,
          rank_within_parent
LIMIT 100
