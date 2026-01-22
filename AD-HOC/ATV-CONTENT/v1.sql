WITH 
-- 1. Identificamos el primer play absoluto de cada usuario en la historia
primer_play_ever AS (
  SELECT 
    PLAY_ID
  FROM `WHOWNER.BT_MKT_MPLAY_PLAYS`
  WHERE PLAYBACK_TIME_MILLISECONDS >= 20000
  QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY START_PLAY_TIMESTAMP ASC) = 1
),


new_viewers_ventana as (
  select
    pl.sit_site_id,
    pl.content_id,
    user_id
  from WHOWNER.BT_MKT_MPLAY_PLAYS AS PL
  INNER JOIN primer_play_ever PPE ON PL.PLAY_ID = PPE.PLAY_ID
  WHERE PL.DS BETWEEN CURRENT_DATE -15 AND CURRENT_DATE() - 1
    AND PL.PLAYBACK_TIME_MILLISECONDS >= 20000
  GROUP BY 1, 2, 3
) ,

-- 3. Limpieza de Catálogo y unificación de géneros (Español/Portugués)
catalogo_unificado AS (
  SELECT 
    CONTENT_ID,
    SIT_SITE_ID,
    TITLE_ADJUSTED,
    CONTENT_PROVIDER,
    -- Limpieza: Quita tildes, pasa a Mayúsculas y unifica términos comunes PT/ES
    UPPER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          NORMALIZE(GENRE, NFD), r'\pM', ''
        ), 
        r'ACAO|ACCION', 'ACCION' -- Ejemplo de unificación PT/ES
      )
    ) as genre_norm,
    CASE 
      WHEN TITLE_ADJUSTED = 'Haven' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Vice' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Babylon' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Total Recall' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Das Haus' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'I am Ren' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Monsters: Dark Continent' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Aniara' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Sensation' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'Éternelle' THEN '2. Horizontes Sintéticos'
      WHEN TITLE_ADJUSTED = 'The Black Labyrinth' THEN '2. Horizontes Sintéticos'

      WHEN TITLE_ADJUSTED = 'Four Christmases' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'The LEGO Movie' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'Alvin and the Chipmunks: Chipwrecked' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'Alvin and the Chipmunks: The Road Chip' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'Jack Hunter: Ep 3' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'Jack Hunter: Ep 5' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'Jack Hunter: Ep 2' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'At Middleton' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'Ingenious' THEN '4. Hogar y Destellos'
      WHEN TITLE_ADJUSTED = 'All Roads Lead to Rome' THEN '4. Hogar y Destellos'

      WHEN TITLE_ADJUSTED = 'Alaska Daily' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'King Arthur' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Cinderella Man' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'True Story' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Amsterdam' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Ray Donovan' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'El César' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'An ordinary man' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Mi Marido Tiene Más Familia' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Valley of the Gods' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Priceless' THEN '6. Relatos Profundos'
      WHEN TITLE_ADJUSTED = 'Chambre 327' THEN '6. Relatos Profundos'
    ELSE 'OTHER'
    END AS pertenece_nube,
    CASE
      WHEN TITLE_ADJUSTED = 'Haven' THEN 1
      WHEN TITLE_ADJUSTED = 'Vice' THEN 2
      WHEN TITLE_ADJUSTED = 'Babylon' THEN 3
      WHEN TITLE_ADJUSTED = 'Total Recall' THEN 4
      WHEN TITLE_ADJUSTED = 'Das Haus' THEN 5
      WHEN TITLE_ADJUSTED = 'I am Ren' THEN 6
      WHEN TITLE_ADJUSTED = 'Monsters: Dark Continent' THEN 7
      WHEN TITLE_ADJUSTED = 'Aniara' THEN 8
      WHEN TITLE_ADJUSTED = 'Sensation' THEN 9
      WHEN TITLE_ADJUSTED = 'Éternelle' THEN 10
      WHEN TITLE_ADJUSTED = 'The Black Labyrinth' THEN 11

      WHEN TITLE_ADJUSTED = 'Four Christmases' THEN 1
      WHEN TITLE_ADJUSTED = 'The LEGO Movie' THEN 2
      WHEN TITLE_ADJUSTED = 'Alvin and the Chipmunks: Chipwrecked' THEN 3
      WHEN TITLE_ADJUSTED = 'Alvin and the Chipmunks: The Road Chip' THEN 4
      WHEN TITLE_ADJUSTED = 'Jack Hunter: Ep 3' THEN 5
      WHEN TITLE_ADJUSTED = 'Jack Hunter: Ep 5' THEN 6
      WHEN TITLE_ADJUSTED = 'Jack Hunter: Ep 2' THEN 7
      WHEN TITLE_ADJUSTED = 'At Middleton' THEN 8
      WHEN TITLE_ADJUSTED = 'Ingenious' THEN 9
      WHEN TITLE_ADJUSTED = 'All Roads Lead to Rome' THEN 10

      WHEN TITLE_ADJUSTED = 'Alaska Daily' THEN 1
      WHEN TITLE_ADJUSTED = 'King Arthur' THEN 2
      WHEN TITLE_ADJUSTED = 'Cinderella Man' THEN 3
      WHEN TITLE_ADJUSTED = 'True Story' THEN 4
      WHEN TITLE_ADJUSTED = 'Amsterdam' THEN 5
      WHEN TITLE_ADJUSTED = 'Ray Donovan' THEN 6
      WHEN TITLE_ADJUSTED = 'El César' THEN 7
      WHEN TITLE_ADJUSTED = 'An ordinary man' THEN 8
      WHEN TITLE_ADJUSTED = 'Mi Marido Tiene Más Familia' THEN 9
      WHEN TITLE_ADJUSTED = 'Valley of the Gods' THEN 10
      WHEN TITLE_ADJUSTED = 'Priceless' THEN 11
      WHEN TITLE_ADJUSTED = 'Chambre 327' THEN 12
      
    ELSE 999
    END AS orden_nube

  FROM `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE`
  WHERE SIT_SITE_ID = 'MLB'
),
base_final as (
  SELECT 
  USER_ID,
  TITLE_ADJUSTED,
    CASE 
        /* 1. Terror / Thriller / Crimen (Prioridad Máxima) */
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%DEEP WATER%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%NO COUNTRY FOR OLD MEN%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%IMPUROS%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%CSI: MIAMI%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%PHONE BOOTH%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%UNFAITHFUL%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%BLACK SWAN%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%TWIN PEAKS%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE GODFATHER%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE BORGIAS%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE LOVELY BONES%' THEN '1. Sombras del Suspenso'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%SCARY MOVIE%' THEN '1. Sombras del Suspenso' -- Parodia pero clasifica aquí por temática
        
        /* 2. Sci-Fi / Fantasía */
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%EDWARD SCISSORHANDS%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%UNDER THE DOME%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%CHARMED%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%MARTIAN, THE%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%VENOM: LET THERE BE CARNAGE%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%ROBOTS%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%SPIDER-MAN: FAR FROM HOME%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%I, ROBOT%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%HALO%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%I AM NUMBER FOUR%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%SAINT SEIYA: LOST CANVAS%' THEN '2. Horizontes Sintéticos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%GRAVITY%' THEN '2. Horizontes Sintéticos'

        /* 3. Acción / Aventura */
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%JOHN WICK%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%NEED FOR SPEED%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%MR. & MRS. SMITH%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%SHOOTER%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%EXODUS: GODS AND KINGS%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%HITMAN: AGENT 47%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%LIFE OF PI%' THEN '3. Justicia Implacable'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%CAST AWAY%' THEN '3. Justicia Implacable'

        /* 4. Infantil / Familia */
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%FOUR CHRISTMASES%' THEN '4. Hogar y Destellos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%CHRISTMAS ALL OVER AGAIN%' THEN '4. Hogar y Destellos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE BEVERLY HILLBILLIES%' THEN '4. Hogar y Destellos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE SPONGEBOB SQUAREPANTS MOVIE%' THEN '4. Hogar y Destellos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%BARNYARD%' THEN '4. Hogar y Destellos'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%HEY ARNOLD! THE MOVIE%' THEN '4. Hogar y Destellos'

        /* 5. Comedia */
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%NACHO LIBRE%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%CHRISTMAS IN THE PINES%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%YOU\'VE GOT MAIL%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE ADDAMS FAMILY%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%PRETTY WOMAN%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%NORBIT%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%ME, MYSELF AND IRENE%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%THE HANGOVER PART II%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%WE\'RE THE MILLERS%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%HOW TO LOSE A GUY IN 10 DAYS%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%CLUELESS%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%GHOST TOWN%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%NANNY, THE%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%MEAN GIRLS%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%SEX AND THE CITY 2%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%SCHOOL OF ROCK%' THEN '5. Ritmo y Carcajadas'
        WHEN UPPER(TITLE_ADJUSTED) LIKE '%WHINDERSSONVERSO%' THEN '5. Ritmo y Carcajadas'
        
        /* 6. Drama (Categoría de Descarte) */
        ELSE '6. Relatos Profundos'
    END AS Nube_Asociada
  FROM new_viewers_ventana as nvv
  left JOIN catalogo_unificado as cu
  on nvv.content_id = cu.content_id
  and nvv.sit_site_id = cu.sit_site_id
  where safe_cast(user_id as int64) is not null
  and nvv.sit_site_id = 'MLB'
),
catalogo_deduplicado_por_titulo AS (
  SELECT
    pertenece_nube,
    title_adjusted,
    content_id,
    orden_nube,
    ROW_NUMBER() OVER (
      PARTITION BY pertenece_nube, title_adjusted
      ORDER BY orden_nube
    ) AS rn_titulo
  FROM catalogo_unificado
),

catalogo_unico_por_titulo AS (
  SELECT
    pertenece_nube,
    title_adjusted,
    content_id,
    orden_nube
  FROM catalogo_deduplicado_por_titulo
  WHERE rn_titulo = 1
),
catalogo_top3_por_nube AS (
  SELECT
    pertenece_nube,
    content_id,
    title_adjusted,
    ROW_NUMBER() OVER (
      PARTITION BY pertenece_nube
      ORDER BY orden_nube
    ) AS rn_nube
  FROM catalogo_unico_por_titulo
),
catalogo_top3_filtrado AS (
  SELECT *
  FROM catalogo_top3_por_nube
  WHERE rn_nube <= 3
)



SELECT
  u.user_id,
  u.nube_asociada,
  c.content_id,
  c.title_adjusted,
  c.rn_nube
FROM base_final u
JOIN catalogo_top3_filtrado c
  ON u.nube_asociada = c.pertenece_nube
WHERE u.nube_asociada IN (
  '2. Horizontes Sintéticos',
  '4. Hogar y Destellos',
  '6. Relatos Profundos'
)
ORDER BY u.user_id, c.rn_nube