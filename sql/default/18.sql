-- query18
SELECT i_item_id,
               ca_country,
               ca_state,
               ca_county,
               avg(cs_quantity)      agg1,
               avg(cs_list_price)    agg2,
               avg(cs_coupon_amt)    agg3,
               avg(cs_sales_price)   agg4,
               avg(cs_net_profit)    agg5,
               avg(c_birth_year)     agg6,
               avg(cd1.cd_dep_count) agg7
FROM   tpcds.catalog_sales,
       tpcds.customer_demographics cd1,
       tpcds.customer_demographics cd2,
       tpcds.customer,
       tpcds.customer_address,
       tpcds.date_dim,
       tpcds.item
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd1.cd_demo_sk
       AND cs_bill_customer_sk = c_customer_sk
       AND cd1.cd_gender = 'F'
       AND cd1.cd_education_status = 'Secondary'
       AND c_current_cdemo_sk = cd2.cd_demo_sk
       AND c_current_addr_sk = ca_address_sk
       AND c_birth_month IN ( 8, 4, 2, 5,
                              11, 9 )
       AND d_year = 2001
       AND ca_state IN ( 'KS', 'IA', 'AL', 'UT',
                         'VA', 'NC', 'TX' )
GROUP  BY i_item_id, ca_country, ca_state, ca_county
ORDER  BY ca_country,
          ca_state,
          ca_county,
          i_item_id
LIMIT 100
