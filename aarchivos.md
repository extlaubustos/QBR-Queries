# Detalle de archivos

## Queries

### analisis_fidelizacion_m.sql
Query escalable para analizar la fidelización hasta Mx. Las filas son mes y las columnas son las clasificaciones M. Ahora algo a tener en cuenta es que si el usuario deberia ser clasificado en un M post a Mx va a quedar clasificado en Mx.

### analisis_fidelizacion_post.sql
Query escalable para analizar la fidelización partiendo desde una cohorte inicial, analizando los meses siguientes y analizando meses hacia delante. Los usuarios que vieron en X mes, cuantos vieron en X+1, en X+2, ..., hasta M11 (x+11 meses). CH EQUEAR CON CAMI

### analisis_fidelizacion_prev.sql
Query escalable para analizar la fidelización partiendo desde una cohorte inicial analizando los meses previos. Los usuarios que vieron en X mes, cuantos vieron en X-1, en X-2, ..., hasta M11 (x-11 meses).

### cast.sql
Realiza la clasificación/calculo en TVM_TIMEFRAME, CUST_TYPE, FLAG_LOG, PLATFORM, TVM_TOTAL, TOTAL_USERS por SIT_SITE_ID.

### clasificacion.sql
Mismo archivo que cast.sql solamente que no calcula el TVM_TOTAL y calcula el AHA_MOMENT.

 ### extraccion-sessions_views.sql
Se realiza el calculo de la cantidad de sesiones y viewers.

### prev_platform.sql
Query para calcular de aquellos usuarios clasificados como SMART ver cual era su DEVICE, su TVM, LIFE CYCLE y FREC del mes anterior.

### smart_SO.sql
Clasifica por SIT_SITE_ID y el mes. Analiza el tiempo de visualización en TV y el tiempo de visualización por CAST. Realiza una concatenación, cuenta la cantidad de viewers y calcula TOTAL_TV_TVM. 




