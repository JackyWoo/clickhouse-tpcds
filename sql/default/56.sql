-- query56
WITH ss
     AS (SELECT i_item_id,
                sum(ss_ext_sales_price) total_sales
         FROM   tpcds.store_sales,
                tpcds.date_dim,
                tpcds.customer_address,
                tpcds.item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   tpcds.item
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                             )
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     cs
     AS (SELECT i_item_id,
                sum(cs_ext_sales_price) total_sales
         FROM   tpcds.catalog_sales,
                tpcds.date_dim,
                tpcds.customer_address,
                tpcds.item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   tpcds.item
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                             )
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     ws
     AS (SELECT i_item_id,
                sum(ws_ext_sales_price) total_sales
         FROM   tpcds.web_sales,
                tpcds.date_dim,
                tpcds.customer_address,
                tpcds.item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   tpcds.item
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' )
                             )
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id)
SELECT i_item_id,
               sum(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_item_id
ORDER  BY total_sales
LIMIT 100
