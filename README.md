# 📊 Análisis de Fidelización y Comportamiento de Usuarios en Mercado Play

Este repositorio contiene múltiples consultas SQL que analizan la fidelización, comportamiento y segmentación de usuarios en Mercado Play a partir de los datos de reproducciones, sesiones y plataformas.

---

## 📌 Índice

- [1. Fidelización Mensual](#1-fidelización-mensual)
  - [1.1. Clasificación M (Genérica)](#11-clasificación-m-genérica)
  - [1.2. Fidelización Mensual (Hacia Adelante)](#12-fidelización-mensual-hacia-adelante)
  - [1.3. Fidelización Mensual (Hacia Atrás)](#13-fidelización-mensual-hacia-atrás)
- [2. Clasificaciones de Usuarios](#2-clasificaciones-de-usuarios)
  - [2.1. Clasificación CAST](#21-clasificación-cast)
  - [2.2. Clasificación con AHA Moment](#22-clasificación-con-aha-moment)
- [3. Análisis de Sesiones y Vistas](#3-análisis-de-sesiones-y-vistas)
- [4. Plataforma Previa](#4-plataforma-previa)
- [5. AHA Moment (Nuevo Criterio)](#5-aha-moment-nuevo-criterio)
- [6. Usuarios SMART en TV por SO](#6-usuarios-smart-en-tv-por-so)
- [📂 Tablas Utilizadas](#tablas-utilizadas)

---

## 1. Fidelización Mensual

### 1.1. Clasificación M (Genérica)

Analiza la retención de usuarios mes a mes, agrupándolos según su comportamiento en períodos posteriores.

- Los usuarios se clasifican en grupos M1, M2, ..., según su actividad en meses futuros.
- Si un usuario tiene actividad más allá del límite definido (`M_LIMIT`), se lo clasifica en el último grupo disponible.
- ✅ Útil para seguimiento de retención.

**Tablas:**
- `DM_MKT_MPLAY_RAW_PLAYS`: Control de torre
- `BT_MKT_MPLAY_PLAYS`: Reproducciones

---

### 1.2. Fidelización Mensual (Hacia Adelante)

Similar a la anterior pero enfocada solo en la retención futura a partir de un mes base.

**Tablas:**
- `DM_MKT_MPLAY_RAW_PLAYS`
- `BT_MKT_MPLAY_PLAYS`

---

### 1.3. Fidelización Mensual (Hacia Atrás)

Clasifica usuarios que tuvieron actividad en el mes base según su comportamiento en los meses anteriores.

**Nota:** Se considera como base el mes actual y se analiza la recurrencia previa de los usuarios.

**Tablas:**
- `DM_MKT_MPLAY_RAW_PLAYS`
- `BT_MKT_MPLAY_PLAYS`

---

## 2. Clasificaciones de Usuarios

### 2.1. Clasificación CAST

Clasifica usuarios como `NEW`, `RETAINED` o `RECOVERED` según:

- Tiempo de reproducción
- Plataforma utilizada
- Si el usuario está logueado o no

**Objetivo:** Alimentar hojas `USERS TVM M` y `USERS TVM W` del Sheet **"Performance Mercado Play - Monthly & Weekly"**.  
Para análisis semanal (TVM W), modificar el `DATE_TRUNC` a `WEEK`.

**Tabla:**
- `BT_MKT_MPLAY_PLAYS`

---

### 2.2. Clasificación con AHA Moment

Extiende la lógica de CAST incluyendo el concepto de **AHA Moment**.  
Se genera una clasificación alternativa considerando la aparición de ese hito en la experiencia del usuario.

**Tabla:**
- `BT_MKT_MPLAY_PLAYS`

---

## 3. Análisis de Sesiones y Vistas

Consulta que agrupa por `sit_site_id`, mes y semana para calcular:

- Cantidad de sesiones
- Usuarios únicos
- Plataforma
- Tiempo de reproducción

Se analiza también el origen de la sesión.

**Objetivo:**

- Alimentar hojas `BASE`, `BASE Daily`, `CVR M` y `CVR W` en los Sheets:
  - "Seguimiento Weekly & Monthly por Touchpoint - Mercado Play"
  - "Performance Mercado Play - Monthly & Weekly"

**Tablas:**
- `BT_MKT_MPLAY_PLAYS`
- `BT_MKT_MPLAY_SESSION`
- `LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION`

---

## 4. Plataforma Previa

Analiza:

- Plataforma utilizada anteriormente
- Life cycle anterior del usuario
- Comportamiento actual

**Nota:** Solo se consideran usuarios **SMART**.

**Tabla:**
- `BT_MKT_MPLAY_PLAYS`

---

## 5. AHA Moment (Nuevo Criterio)

Permite determinar si un usuario alcanzó su **AHA Moment** dentro de los 30 días posteriores a su primera reproducción, en vez de considerar solo la primera fecha del mes como en versiones anteriores.

**Tabla:**
- `BT_MKT_MPLAY_PLAYS`

---

## 6. Usuarios SMART en TV por SO

Permite visualizar la cantidad de usuarios y minutos reproducidos por:

- `sit_site_id`
- Mes
- Plataforma

**Exclusivo:** Usuarios que vieron contenido en **TV**.  
Permite concatenar información sobre el tipo de **Sistema Operativo**.

**Tabla:**
- `BT_MKT_MPLAY_PLAYS`

---

## 📂 Tablas Utilizadas

| Tabla                                                         | Descripción                                 |
|---------------------------------------------------------------|---------------------------------------------|
| `DM_MKT_MPLAY_RAW_PLAYS`                                      | Control de torre                            |
| `BT_MKT_MPLAY_PLAYS`                                          | Reproducciones de Play                      |
| `BT_MKT_MPLAY_SESSION`                                        | Sesiones de usuario en Play                 |
| `LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION`                         | Origen de sesiones                          |

---

> 📝 **Nota:** Para modificar los análisis semanales o diarios, cambiar `DATE_TRUNC` o ajustar la fecha base en los CTEs según corresponda.