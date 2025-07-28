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

### prev_platform_atv_frec_no_filter.sql (TODOS)
Misma query que prev_platform.sql pero se agrega el calculo de ATV, FREC. Queda pendiente comprobar el ATV y FREC. CHEQUEAR CON CAMI

### prev_platform_atv_frec.sql (RETAINED)
Misma query que prev_platform.sql pero se agrega el calculo de ATV, FREC. Queda pendiente comprobar el ATV y FREC. CHEQUEAR CON CAMI

### prev_platform.sql
Query para calcular de aquellos usuarios clasificados como SMART ver el mes anterior cual era su DEVICE. Tiene un filtro en la CTE USERS_SMART para filtrar por retenidos si es necesario.

### smart_SO.sql
Clasifica por SIT_SITE_ID y el mes. Analiza el tiempo de visualización en TV y el tiempo de visualización por CAST. Realiza una concatenación, cuenta la cantidad de viewers y calcula TOTAL_TV_TVM. 




## Excels

### Analisis de M: 
Analiza cada M posicionandose en ese mes en particular. Ejemplo:
En ENE vieron 685276. En FEB de esos 685276 vieron 270442. Quizas de estos 270442 vieron otros meses mas adelante pero analizando M2 desde el punto de vista de ENE puedo saber que continuaron viendo 270442.
Y asi el análisis es MES A MES viendo una evolución de la M.

### Fidelización hacia adelante:
Analiza la continuidad de los usuarios que vieron en un mes. Ejemplo:
En ENE vieron 685294. De esos 685294 en FEB vieron un total de 270459 pero algunos siguieron viendo pero hay 111259 que vieron solo en ENE y FEB. Y asi continua.
Si miramos porcentajes vemos que hay un salto considerable por ejemplo en M2 de 2025-05 (cant. que vieron en JUN) a 2025-06 (cant. que vieron en JUL). En M3 vemos hay que un salto de 2025-04 (cant. que vieron en JUN) a 2025-05 (cant. que vieron en JUL). Al tener 3 meses posibles de analisis vemos que se mentiene entre 17 y 19. Si tenemos 4 meses para analizar vemos en M3 que se mantiene entre 8 y 9 y asi.
