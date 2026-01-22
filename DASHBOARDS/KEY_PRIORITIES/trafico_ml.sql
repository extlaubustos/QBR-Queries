INSERT INTO meli-sbox.MPLAY.TRAFFIC_MP_ML (
       BUSINESS,
       TIMEFRAME_TYPE,
       TIMEFRAME_ID,
       SIT_SITE_ID,
       DEVICE_PLATFORM,
       PATH,
       TYPE,
       Q_SESSIONS
)
select
'ML' AS BUSINESS,
'MONTHLY' AS TIMEFRAME_TYPE,
DATE_TRUNC(ds,MONTH)  AS TIMEFRAME_ID,
SITE,
device.platform,
path,
type,
count (distinct usr.melidata_session_id) as Q_Sesiones
from meli-bi-data.MELIDATA.TRACKS
where ds BETWEEN '2025-08-01' AND '2025-08-31'
and TYPE IN ('view')
and path in ('/home')
and site in ('MLA','MLB','MLM','MLC', 'MCO', 'MEC', 'MLU', 'MPE')
group by 1,2,3,4,5,6,7


---------------------------
INSERT INTO meli-sbox.MPLAY.TRAFFIC_MP_ML (
       BUSINESS,
       TIMEFRAME_TYPE,
       TIMEFRAME_ID,
       SIT_SITE_ID,
       DEVICE_PLATFORM,
       PATH,
       TYPE,
       Q_SESSIONS
)
select
'MP' AS BUSINESS,
'MONTHLY' AS TIMEFRAME_TYPE,
DATE_TRUNC(ds,MONTH)  AS TIMEFRAME_ID,
SITE,
device.platform,
path,
type,
count (distinct usr.melidata_session_id) as Q_Sesiones
from meli-bi-data.MELIDATA.TRACKS
where ds BETWEEN '2025-11-01' AND '2025-11-30'
and TYPE IN ('view')
and path in ('/wallet_home/home')
and site in ('MLA','MLB','MLM','MLC', 'MCO', 'MEC', 'MLU', 'MPE')
group by 1,2,3,4,5,6,7