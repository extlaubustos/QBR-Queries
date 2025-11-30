with SAMPLE as (
  SELECT
    (FORMAT_DATE("%Y%m", cast(DATE_MONTH as date))) AS SAMPLE_DATE_MONTH,
    SAMPLE_DATE,
    cus_cust_id,
    sit_site_id,
    LIFE_CYCLE_SEGMENT,
    TVM_CLASS,
    NBCU_FLAG,
    ORIGIN_NEGOCIO,
    CONTENT_TYPE,
    ORIGINAL_TITLE,
    GENRE,
    --COMPLETION_RATE,
    --MAX_TMV,
    --MAX_HOUR_VIEW,
    TYPE_USER,
    SEGMENTACION_BUYER,
    FLAG_DISNEY_LAST_STATE,
    FLAG_DISNEY_HISTORICO,
    TOTAL_TMV
  FROM `meli-bi-data.WHOWNER.BT_CX_NPS_XM_TX_MPLAY_SAMPLING`
),
base_manual as (
  SELECT
    NPS_REL_RES_END_DT,
    (FORMAT_DATE("%Y%m", cast(NPS_REL_RES_END_DT as date))) AS END_DATE_MONTH,
    (FORMAT_DATE("%Y0%Q", cast(NPS_REL_RES_END_DT as date))) AS QUARTER,
    NPS_REL_CUS_CUST_ID, SIT_SITE_ID, NPS_REL_QUALTRICS_RESPONSE_ID, AWARENESS, NOTA_NPS, NPS, 
    -- Motivos de promoción (Solo para promotores NPS = 1)
    case 
      when MPROM = ' A plataforma é muito completa' then 'Complete platform' 
      when MPROM = 'La plataforma es muy completa' then 'Complete platform'
      when MPROM = 'A variedade do conteúdo é ótima' then 'Content variety' 
      when MPROM = 'La variedad de contenido es muy buena' then 'Content variety'
      when MPROM = 'A plataforma é fácil de usar' then 'Easy to use' 
      when MPROM = 'Es fácil de usar' then 'Easy to use'
      when MPROM = 'Ótima qualidade de imagem e de conteúdo' then 'Good image quality' 
      when MPROM = 'La calidad de imagen del contenido es muy bueno' then 'Good image quality'
      when MPROM = 'O fato de ser uma plataforma grátis' then 'It¿' 
      when MPROM = 'Que es gratuito' then 'It¿'
    ELSE 'Other' end as MPROM,
    MPROM as MPROM_DETALLE, 
    -- CSAT: Satisfacción con cada variable. Disponible para promotores, neutros y detractores
    CSAT_VARIEDAD, 
    CSAT_PUBLICIDAD, 
    CSAT_FACILIDAD, 
    CSAT_FUNCIONAMIENTO, 
    CSAT_RECOMENDACIONES, 
    SAT_VARIEDAD,
    --  Motivos de insatisfacción cuando están disconformes con variedad (CSAT_VARIEDAD 1 a 3)
    case 
      when INSAT_VARIEDAD = ' La película/serie que buscaba no estaba' then 'What I was looking for was not there' 
      when INSAT_VARIEDAD = 'O filme/série que eu queria ver não estava disponível' then 'What I was looking for was not there'
      when INSAT_VARIEDAD = 'El catálogo está desactualizado' then 'Outdated catalog' 
      when INSAT_VARIEDAD = 'O catálogo está desatualizado' then 'Outdated catalog'
      when INSAT_VARIEDAD = 'El catálogo no es atractivo' then 'Unattractive catalog' 
      when INSAT_VARIEDAD = 'O catálogo não é atrativo' then 'Unattractive catalog'
      when INSAT_VARIEDAD = 'La serie no estaba completa' then 'Incomplete series' 
      when INSAT_VARIEDAD = 'A série estava incompleta' then 'Incomplete series'
      when INSAT_VARIEDAD = 'Me quitaron la serie y no había terminado de verla' then 'Content removed'
      when INSAT_VARIEDAD = 'A série ficou indisponível e eu não tinha terminado de assisti-la.' then 'Content removed' 
    ELSE 'Other' end as INSAT_VARIEDAD,
    INSAT_VARIEDAD AS DETALLE_INSAT_VARIEDAD,
    INSAT_TIPO_VARIEDAD,
    --  Motivos de insatisfacción cuando están disconformes con publicidad (CSAT_PUBLICIDAD 1 a 3)
    case 
      when INSAT_PUBLICIDAD = 'La frecuencia de la publicidad es excesiva' then 'Too many ads' 
      when INSAT_PUBLICIDAD = 'Há muitas propagandas' then 'Too many ads'
      when INSAT_PUBLICIDAD = 'La publicidad corta en momentos claves del contenido' then 'Interrupts at key moments' 
      when INSAT_PUBLICIDAD = 'As propagandas interrompem o conteúdo em momentos importantes' then 'Interrupts at key moments'
      when INSAT_PUBLICIDAD = 'La publicidad es siempre la misma' then 'Same ads repeatdly' 
      when INSAT_PUBLICIDAD = 'As propagandas são sempre as mesmas' then 'Same ads repeatdly'
      when INSAT_PUBLICIDAD = 'La publicidad no se alinea a mis intereses' then 'Not relevant to me'
      when INSAT_PUBLICIDAD = 'As propagandas não têm a ver com meus interesses' then 'Not relevant to me'
      when INSAT_PUBLICIDAD = 'La publicidad me agotó y no terminé de ver el contenido' then 'Made me stop watching' 
      when INSAT_PUBLICIDAD = 'As propagandas me cansaram e não terminei de ver o conteúdo' then 'Made me stop watching'
      when INSAT_PUBLICIDAD = 'Desearía pagar una suscripción para evitar la publicidad' then 'I want a subscription' 
      when INSAT_PUBLICIDAD = 'Gostaria de pagar uma assinatura para evitar propagandas' then 'I want a subscription' 
    ELSE 'Other' end as INSAT_PUBLICIDAD,
    INSAT_PUBLICIDAD AS DETALLE_INSAT_PUBLICIDAD,
    --  Motivos de insatisfacción cuando están disconformes con facilidad de uso (CSAT_FACILIDAD 1 a 3)
    case 
      when INSAT_FACILIDAD  = 'No puedo transmitir mi contenido a un Smart TV' then 'Difficulty casting on TV' 
      when INSAT_FACILIDAD  = 'Não consigo transmitir o conteúdo em uma Smart TV' then 'Difficulty casting on TV'
      when INSAT_FACILIDAD  = 'No puedo hacer mi lista de favoritos/pendientes para ver' then 'Cannot create favorites list' 
      when INSAT_FACILIDAD  = 'Não consigo criar minha lista de favoritos/próximos conteúdos para assistir' then 'Cannot create favorites list'
      when INSAT_FACILIDAD  = 'La búsqueda del contenido para ver no es práctica' then 'Search not practical'
      when INSAT_FACILIDAD  = 'Não é fácil buscar o conteúdo que quero ver' then 'Search not practical'
      when INSAT_FACILIDAD  = 'La navegación en la plataforma no es didáctica' then 'Navigation not intuitive' 
      when INSAT_FACILIDAD  = 'A navegação na plataforma é complicada' then 'Navigation not intuitive'
      when INSAT_FACILIDAD  = 'La configuración de los subtítulos es limitada' then 'Limited subtitle settings' 
      when INSAT_FACILIDAD  = 'As configurações das legendas são limitadas' then 'Limited subtitle settings'
      when INSAT_FACILIDAD  = 'No figura el idioma en que quiero ver el contenido' then 'Desired language not available' 
      when INSAT_FACILIDAD = 'O idioma no qual quero assistir o conteúdo não está disponível' then 'Desired language not available' 
    ELSE 'Other' end as INSAT_FACILIDAD,
    INSAT_FACILIDAD as DETALLE_INSTAT_FACILIDAD,
    --  Motivos de insatisfacción cuando están disconformes con funcionamiento (CSAT_FUNCIONAMIENTO 1 a 3)
    case 
      when INSAT_FUNCIONAMIENTO  = 'Demora de carga al momento de los comerciales' then 'Delay during commercial' 
      when INSAT_FUNCIONAMIENTO  = 'O conteúdo demora para carregar quando as propagandas são transmitidas' then 'Delay during commercial'
      when INSAT_FUNCIONAMIENTO  = 'La plataforma se sale al intentar volver al menú principal o a Mercado Libre' then 'App exits unexpectedly' 
      when INSAT_FUNCIONAMIENTO  = 'A plataforma sai do ar quando tento voltar ao menu principal ou para o Mercado Livre' then 'App exits unexpectedly'
      when INSAT_FUNCIONAMIENTO  = 'Tengo desfasaje de volumen mientras miro el contenido' then 'Audio/video out of sync' 
      when INSAT_FUNCIONAMIENTO  = 'Há instabilidade no volume enquanto estou assistindo ao conteúdo' then 'Audio/video out of sync'
      when INSAT_FUNCIONAMIENTO  = 'No me permite ver el contenido a pantalla completa' then 'Cannot view full screen' 
      when INSAT_FUNCIONAMIENTO  = 'Não consigo ver o conteúdo em tela cheia' then 'Cannot view full screen'
      when INSAT_FUNCIONAMIENTO  = 'Mala calidad de imagen del contenido' then 'Poor image quality' 
      when INSAT_FUNCIONAMIENTO  = 'A imagem é de baixa qualidade' then 'Poor image quality' 
    ELSE 'Other' end as INSAT_FUNCIONAMIENTO,
    INSAT_FUNCIONAMIENTO AS DETALLE_INSAT_FUNCIONAMIENTO,
    --  Motivos de insatisfacción cuando están disconformes con recomendaciones (CSAT_RECOMENDACIONES 1 a 3)
    case 
      when INSAT_RECOMENDACIONES = 'Me notifican contenido que no es de mis preferencias' then 'Notifications not of interest' 
      when INSAT_RECOMENDACIONES  = 'Recebo notificações que não têm a ver com as minhas preferências' then 'Notifications not of interest'
      when INSAT_RECOMENDACIONES  = 'Las notificaciones son demasiadas' then 'Too many notifications' 
      when INSAT_RECOMENDACIONES  = 'Recebo muitas notificações' then 'Too many notifications'
      when INSAT_RECOMENDACIONES  = 'Me notifican contenido que luego debo pagar para verlo' then 'Paid-content notifications' 
      when INSAT_RECOMENDACIONES = 'Recebo notificações de conteúdos pagos' then 'Paid-content notifications'
      when INSAT_RECOMENDACIONES  = 'Sólo ingresé a ver la plataforma y ahora las notificaciones son demasiadas' then 'Just logged in - too many notifications' 
      when INSAT_RECOMENDACIONES  = 'Só acessei a plataforma para conhecê-la e agora recebo muitas notificações' then 'Just logged in - too many notifications'
      when INSAT_RECOMENDACIONES  = 'Las notificaciones llegan en horario inadecuado' then 'Notifications at inappropriate times' 
      when INSAT_RECOMENDACIONES  = 'As notificações chegam em horários inadequados' then 'Notifications at inappropriate times'
      when INSAT_RECOMENDACIONES  = 'Las notificaciones me llegan por canales que no deseo ¿cuáles?' then 'Notifications from undesired channels' 
      when INSAT_RECOMENDACIONES  = 'Recebo notificações em canais que não escolhi. Por favor, indique quais são esses canais.' then 'Notifications from undesired channels' 
    ELSE 'Other' end as INSAT_RECOMENDACIONES,
    INSAT_RECOMENDACIONES AS DETALLE_INSAT_RECOMENDACIONES,
    RESPONDE_ADICIONALES,
    PLATAFORMAS,
    PLATAFORMA_PRINCIPAL,
    BENEFICIOS_OTRAS_PLATAFORMAS,
    LIFE_CYCLE_SEGMENT,
    TVM_CLASS,
    NBCU_FLAG,
    ORIGIN_NEGOCIO,
    CONTENT_TYPE,
    ORIGINAL_TITLE,
    GENRE,
    --COMPLETION_RATE,
    --MAX_TMV,
    --MAX_HOUR_VIEW,
    TYPE_USER,
    SEGMENTACION_BUYER,
    FLAG_DISNEY_LAST_STATE,
    FLAG_DISNEY_HISTORICO,
    TOTAL_TMV,
    COMMENT
  FROM `meli-bi-data.SBOX_NPS_ANALYTICS.NPS_MERCADO_PLAY_TEMPORAL` 
),
base_auto as (
  SELECT
    NPS_REL_RES_END_DT as date,
    (FORMAT_DATE("%Y%m", cast(NPS_REL_RES_END_DT as date))) AS END_DATE_MONTH,
    (FORMAT_DATE("%Y0%Q", cast(NPS_REL_RES_END_DT as date))) AS QUARTER,
    NPS_REL_CUS_CUST_ID,
    RES.SIT_SITE_ID,
    NPS_REL_QUALTRICS_RESPONSE_ID,
    -- Awareness
    case
      when CAST(JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_AWARENESS') as STRING) IN ('Sí','Sim','Si') THEN 'Si'
      when CAST(JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_AWARENESS') as STRING) IN ('No','Não',null) THEN 'No'
    end as AWARENESS,
    -- NPS
    NPS_REL_NOTA_NPS AS NOTA_NPS,
    CASE
      WHEN NPS_REL_NOTA_NPS >=9 THEN 1
      WHEN NPS_REL_NOTA_NPS =7 or NPS_REL_NOTA_NPS =8 THEN 0
      WHEN NPS_REL_NOTA_NPS <=6 THEN -1
    END AS NPS,
    -- Motivos de promoción (Solo para promotores NPS = 1)
    case 
      when NPS_REL_MPROM = ' A plataforma é muito completa' then 'Complete platform' 
      when NPS_REL_MPROM = 'La plataforma es muy completa' then 'Complete platform'
      when NPS_REL_MPROM = 'A variedade do conteúdo é ótima' then 'Content variety' 
      when NPS_REL_MPROM = 'La variedad de contenido es muy buena' then 'Content variety'
      when NPS_REL_MPROM = 'A plataforma é fácil de usar' then 'Easy to use' 
      when NPS_REL_MPROM = 'Es fácil de usar' then 'Easy to use'
      when NPS_REL_MPROM = 'Ótima qualidade de imagem e de conteúdo' then 'Good image quality' 
      when NPS_REL_MPROM = 'La calidad de imagen del contenido es muy bueno' then 'Good image quality'
      when NPS_REL_MPROM = 'O fato de ser uma plataforma grátis' then 'It¿' 
      when NPS_REL_MPROM = 'Que es gratuito' then 'It¿'
    ELSE 'Other' end as MPROM,
    NPS_REL_MPROM as MPROM_DETALLE, 
    -- CSAT: Satisfacción con cada variable. Disponible para promotores, neutros y detractores
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_VARIEDAD') as CSAT_VARIEDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_PUBLICIDAD') as CSAT_PUBLICIDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_FACILIDAD') as CSAT_FACILIDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_FUNCIONAMIENTO') as CSAT_FUNCIONAMIENTO,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_RECOMENDACIONES') as CSAT_RECOMENDACIONES,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_SAT_VARIEDAD') as SAT_VARIEDAD,
    --  Motivos de insatisfacción cuando están disconformes con variedad (CSAT_VARIEDAD 1 a 3)
    case 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = ' La película/serie que buscaba no estaba' then 'What I was looking for was not there' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'O filme/série que eu queria ver não estava disponível' then 'What I was looking for was not there'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'El catálogo está desactualizado' then 'Outdated catalog' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'O catálogo está desatualizado' then 'Outdated catalog'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'El catálogo no es atractivo' then 'Unattractive catalog' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'O catálogo não é atrativo' then 'Unattractive catalog'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'La serie no estaba completa' then 'Serie incompleta' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'A série estava incompleta' then 'Serie incompleta'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'Me quitaron la serie y no había terminado de verla' then 'Quitaron el contenido' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') = 'A série ficou indisponível e eu não tinha terminado de assisti-la.' then 'Quitaron el contenido' 
    ELSE 'Other' end as INSAT_VARIEDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_VARIEDAD') as DETALLE_INSAT_VARIEDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_TIPO_VARIEDAD') as INSAT_TIPO_VARIEDAD,
    --  Motivos de insatisfacción cuando están disconformes con publicidad (CSAT_PUBLICIDAD 1 a 3)
    case 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'La frecuencia de la publicidad es excesiva' then 'Too many ads' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'Há muitas propagandas' then 'Too many ads'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'La publicidad corta en momentos claves del contenido' then 'Interrupts at key moments' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'As propagandas interrompem o conteúdo em momentos importantes' then 'Interrupts at key moments'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'La publicidad es siempre la misma' then 'Same ads repeatdly' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'As propagandas são sempre as mesmas' then 'Same ads repeatdly'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'La publicidad no se alinea a mis intereses' then 'Not relevant to me' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'As propagandas não têm a ver com meus interesses' then 'Not relevant to me'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'La publicidad me agotó y no terminé de ver el contenido' then 'Made me stop watching' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'As propagandas me cansaram e não terminei de ver o conteúdo' then 'Made me stop watching'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'Desearía pagar una suscripción para evitar la publicidad' then 'I want a subscription' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') = 'Gostaria de pagar uma assinatura para evitar propagandas' then 'I want a subscription' 
    ELSE 'Other' end as INSAT_PUBLICIDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_PUBLICIDAD') as DETALLE_INSAT_PUBLICIDAD,
    --  Motivos de insatisfacción cuando están disconformes con facilidad de uso (CSAT_FACILIDAD 1 a 3)
    case 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'No puedo transmitir mi contenido a un Smart TV' then 'Difficulty casting on TV' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'Não consigo transmitir o conteúdo em uma Smart TV' then 'Difficulty casting on TV'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'No puedo hacer mi lista de favoritos/pendientes para ver' then 'Cannot create favorites list' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'Não consigo criar minha lista de favoritos/próximos conteúdos para assistir' then 'Cannot create favorites list'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'La búsqueda del contenido para ver no es práctica' then 'Search not practical' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'Não é fácil buscar o conteúdo que quero ver' then 'Search not practical'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'La navegación en la plataforma no es didáctica' then 'Navigation not intuitive' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'A navegação na plataforma é complicada' then 'Navigation not intuitive'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'La configuración de los subtítulos es limitada' then 'Limited subtitle settings' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'As configurações das legendas são limitadas' then 'Limited subtitle settings'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'No figura el idioma en que quiero ver el contenido' then 'Desired language not available' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD')  = 'O idioma no qual quero assistir o conteúdo não está disponível' then 'Desired language not available' 
    ELSE 'Other' end as INSAT_FACILIDAD,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FACILIDAD') as DETALLE_INSAT_FACILIDAD,
    --  Motivos de insatisfacción cuando están disconformes con funcionamiento (CSAT_FUNCIONAMIENTO 1 a 3)
    case 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'Demora de carga al momento de los comerciales' then 'Delay during commercial' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'O conteúdo demora para carregar quando as propagandas são transmitidas' then 'Delay during commercial'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'La plataforma se sale al intentar volver al menú principal o a Mercado Libre' then 'App exits unexpectedly' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'A plataforma sai do ar quando tento voltar ao menu principal ou para o Mercado Livre' then 'App exits unexpectedly'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'Tengo desfasaje de volumen mientras miro el contenido' then 'Audio/video out of sync' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'Há instabilidade no volume enquanto estou assistindo ao conteúdo' then 'Audio/video out of sync'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'No me permite ver el contenido a pantalla completa' then 'Cannot view full screen' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'Não consigo ver o conteúdo em tela cheia' then 'Cannot view full screen'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'Mala calidad de imagen del contenido' then 'Poor image quality' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO')  = 'A imagem é de baixa qualidade' then 'Poor image quality' 
    ELSE 'Other' end as INSAT_FUNCIONAMIENTO,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_FUNCIONAMIENTO') as DETALLE_INSAT_FUNCIONAMIENTO,
    -- Motivos de insatisfacción cuando están disconformes con recomendaciones (CSAT_RECOMENDACIONES 1 a 3)
    case 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Me notifican contenido que no es de mis preferencias' then 'Notifications not of interest' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Recebo notificações que não têm a ver com as minhas preferências' then 'Notifications not of interest'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Las notificaciones son demasiadas' then 'Too many notifications' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Recebo muitas notificações' then 'Too many notifications'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Me notifican contenido que luego debo pagar para verlo' then 'Paid-content notifications' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Recebo notificações de conteúdos pagos' then 'Paid-content notifications'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Sólo ingresé a ver la plataforma y ahora las notificaciones son demasiadas' then 'Just logged in - too many notifications' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Só acessei a plataforma para conhecê-la e agora recebo muitas notificações' then 'Just logged in - too many notifications'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Las notificaciones llegan en horario inadecuado' then 'Notifications at inappropriate times' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'As notificações chegam em horários inadequados' then 'Notifications at inappropriate times'
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Las notificaciones me llegan por canales que no deseo ¿cuáles?' then 'Notifications from undesired channels' 
      when JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES')  = 'Recebo notificações em canais que não escolhi. Por favor, indique quais são esses canais.' then 'Notifications from undesired channels' 
    ELSE 'Other' end as INSAT_RECOMENDACIONES,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_CSAT_INSAT_RECOMENDACIONES') as DETALLE_INSAT_RECOMENDACIONES,
    -- Preguntas adicionales competitivo
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_ADICIONALES') as RESPONDE_ADICIONALES,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_ADICIONALES_PLATAFORMAS') as PLATAFORMAS,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_ADICIONALES_PLATAFORMA_PRINCIPAL') as PLATAFORMA_PRINCIPAL,
    JSON_EXTRACT_SCALAR(NPS_REL_PARTICULAR_QUESTIONS,'$[0].NPS_REL_SEL_MPLAY_ADICIONALES_BENEFICIOS') as BENEFICIOS_OTRAS_PLATAFORMAS,
    LIFE_CYCLE_SEGMENT,
    TVM_CLASS,
    NBCU_FLAG,
    ORIGIN_NEGOCIO,
    CONTENT_TYPE,
    ORIGINAL_TITLE,
    GENRE,
    --COMPLETION_RATE,
    --MAX_TMV,
    --MAX_HOUR_VIEW,
    TYPE_USER,
    SEGMENTACION_BUYER,
    FLAG_DISNEY_LAST_STATE,
    FLAG_DISNEY_HISTORICO,
    TOTAL_TMV,
    NPS_REL_COMMENT AS COMMENT
  FROM `meli-bi-data.WHOWNER.BT_CX_NPS_REL_SURVEY_RESPONSES` RES
  LEFT JOIN SAMPLE
    ON RES.NPS_REL_CUS_CUST_ID = SAMPLE.CUS_CUST_ID
    AND RES.SIT_SITE_ID = SAMPLE.SIT_SITE_ID
    --AND (FORMAT_DATE("%Y%m", cast(RES.NPS_REL_RES_END_DT as date))) = CAST(SAMPLE.SAMPLE_DATE_MONTH AS STRING)
  WHERE NPS_REL_QUALTRICS_SURVEY_ID in ('SV_0GPkyb7XKs7N1gG', 'SV_40LmJyUwfoIZouW')
    and NPS_REL_RES_END_DT >= '2025-09-01'
)
select * from base_manual
union all
select * from base_auto