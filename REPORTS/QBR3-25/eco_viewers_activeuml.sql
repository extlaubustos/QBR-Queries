-- description: Análisis de penetración de Mercado Play sobre el ecosistema: Compara la audiencia de visualización (Viewers) frente a los usuarios con sesiones en el Marketplace y compradores únicos (Unique Buyers) por sitio y mes. 
-- domain: commercial / ecosystem 
-- product: mplay 
-- use_case: market share / ecosystem penetration analysis 
-- grain: sit_site_id, month_id 
-- time_grain: monthly 
-- date_column: ORD_CREATED_DT / DS / MONTH_ID 
-- date_filter: >= '2023-01-01' 
-- threshold_rule: playback_time >= 20s 
-- metrics: 
-- - UNIQUE_USERS_MKTPLCE: Usuarios únicos con sesión en el marketplace (TM) 
-- - UNIQUE_VIEWERS: Usuarios logueados únicos con reproducción en MPlay 
-- - UNIQUE_VIEWERS_NL: Usuarios totales (logueados y no logueados) con reproducción 
-- - UNIQUE_BUYERS: Compradores únicos con órdenes cerradas en el marketplace 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_ORD_ORDERS 
-- - meli-bi-data.SBOX_LOYALTY.MPLAY_NEGOCIO_MARKETPLACE_SESSIONS 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- joins: 
-- - MPLAY_SESSIONS (M) LEFT JOIN PLAYS (P): Para cruzar tráfico de marketplace con visualización efectiva. 
-- - MPLAY_SESSIONS (M) LEFT JOIN BUYERS_MARKETPLACE (B): Para comparar tráfico con conversión transaccional en el ecosistema. 
-- owner: data_team
WITH BUYERS_MARKETPLACE AS (
  select distinct date_trunc(bd.ord_created_dt,month) as MONTH_ID,
    bd.sit_site_id,
    count(distinct bd.ord_buyer.id) as UNIQUE_BUYERS
  from `meli-bi-data.WHOWNER.BT_ORD_ORDERS` as bd
  where bd.ord_tgmv_flg = true
    and coalesce(bd.ord_auto_offer_flg,False) = False
    and bd.sit_site_id in ('MLA','MLM','MLC','MCO','MLU','MPE','MLB', 'MEC')
    and bd.ord_closed_dt is not null
    and bd.ord_category.marketplace_id='TM'
    and ord_order_mshops_flg=false
    and ord_order_proximity_flg=false
    and bd.ord_created_dt >= '2023-01-01'
  group by 1,2
)
SELECT DISTINCT
  M.SIT_SITE_ID,
  M.MONTH_ID,
  M.UNIQUE_USERS AS UNIQUE_USERS_MKTPLCE,
  COUNT(DISTINCT P.CUS_CUST_ID) AS UNIQUE_VIEWERS,
  count (DISTINCT p.user_id) as unique_viewers_nl,
  UNIQUE_BUYERS
FROM `meli-bi-data.SBOX_LOYALTY.MPLAY_NEGOCIO_MARKETPLACE_SESSIONS` M
LEFT JOIN `WHOWNER.BT_MKT_MPLAY_PLAYS` P
  ON M.SIT_SITE_ID = P.SIT_SITE_ID
  AND M.MONTH_ID = DATE_TRUNC(P.DS, MONTH)
  AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
LEFT JOIN Buyers_Marketplace as B
  ON M.SIT_SITE_ID =B.SIT_SITE_ID
  AND M.MONTH_ID = B.MONTH_ID
GROUP BY ALL
;