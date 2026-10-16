[README_9626.md](https://github.com/user-attachments/files/33522182/README_9626.md)
# Análisis 1 — Víctimas de violencia intrafamiliar en Cali

**Dataset:** Niños, niñas y adolescentes víctimas de delitos en Colombia
**Registro:** 37.849 filas × 8 columnas · UTF-8 · delimitador coma
**Tecnología:** Apache Spark 3.5.1 · Python 3.10.12 · Java 17

---

## Pregunta

> ¿Cuántos niños, niñas y adolescentes víctimas de **violencia intrafamiliar**
> fueron reportados en la ciudad de **Cali**?

## Respuesta

```
======================================================= 284 CASOS REPORTADOS
=======================================================
```

Criterio aplicado:

- `Municipio == 'Cali'`
- `Delito` contiene `VIOLENCIA INTRAFAMILIAR`

---

## Esquema del dataset

```
root
 |-- Fecha: timestamp
 |-- País: string
 |-- Municipio: string
 |-- Delito: string
 |-- Nacionalidad: string
 |-- Género: string
 |-- Clase de sitio: string
 |-- Zona: string
```

**No hay identificador de víctima.** Cada fila es un caso reportado. Eso tiene
consecuencias importantes (ver *Problemas encontrados*).

La columna `Delito` trae el artículo del Código Penal. Para violencia
intrafamiliar solo existe el **artículo 229** (9.584 registros a nivel
nacional), así que el filtro es inequívoco.

---

## Resultados

### Base de comparación

| Concepto | Valor |
|---|---:|
| Total del dataset | 37.849 |
| Violencia intrafamiliar en Colombia | 9.584 |
| Municipio Cali (cualquier delito) | 1.835 |
| **Cali + violencia intrafamiliar** | **284** |
| Cali sobre el total nacional | 2,96 % |
| Cali sobre el total del dataset | 0,75 % |

### Por género

| Género | Víctimas | % |
|---|---:|---:|
| Femenino | 165 | 58,1 |
| Masculino | 119 | 41,9 |

### Por zona de ocurrencia

| Zona | Víctimas | % |
|---|---:|---:|
| Vías públicas | 200 | 70,4 |
| Casas de habitación | 53 | 18,7 |
| Dentro de la vivienda | 21 | 7,4 |
| Apartamento | 2 | 0,7 |
| Establecimiento educativo | 1 | 0,4 |
| Establecimiento de salud | 1 | 0,4 |
| Clínicas y similares | 1 | 0,4 |
| Hospitales | 1 | 0,4 |

### Por mes de reporte

| Mes | Víctimas |
|---|---:|
| 2026-01 | 36 |
| 2026-02 | 56 |
| 2026-03 | 74 |
| 2026-04 | 51 |
| 2026-05 | 47 |
| 2026-06 | 20 |

### Ranking nacional

| # | Municipio | Víctimas |
|---|---|---:|
| 1 | Bogotá D.C. | 5.872 |
| 2 | Medellín | 465 |
| 3 | **Cali** | **284** |
| 4 | Soacha | 196 |
| 5 | Cartagena | 83 |

---

## Gráficos

| Archivo | Qué muestra |
|---|---|
| `g1_violencia_intrafamiliar_cali.png` | La respuesta central: Cali frente al resto del país |
| `g2_genero_cali.png` | Brecha de género |
| `g3_zona_cali.png` | Lugar de ocurrencia |
| `g4_top_municipios_vi.png` | Contexto nacional (Cali en rojo) |
| `g5_top_delitos_cali.png` | Posición dentro del panorama de delitos de Cali |
| `g6_evolucion_mensual_cali.png` | Tendencia del reporte |

---

## Cómo ejecutar

```bash
pip install -r ../requirements.txt
python3 victimas_cali.py
```

**Salida:** `~/Escritorio/BIGDATA 3/fase1_victimas/` con
`reporte_victimas.txt` y la carpeta `graficos/`.

Duración observada: 16,3 segundos.

---

## Problemas encontrados

### 1. `dropDuplicates()` destruyó el 30 % de los datos

La primera ejecución dio **219** en lugar de 284. La causa fue un
`dropDuplicates()` aplicado por costumbre: borró 11.436 filas.

**Por qué está mal aquí:** el dataset no tiene identificador de víctima. Dos
filas con la misma fecha, municipio, delito, género y zona son personas
**distintas**, no duplicados. Deduplicar fusionaba víctimas reales y subestimaba
el phenomenon en un 23 %.

**Solución:** se eliminó la deduplicación. Cada fila es un caso reportado.

```python
# NO HACER: d = d.dropDuplicates()
total = d.count()   # 37.849 filas = 37.849 víctimas reportadas
```

### 2. Normalización de texto

Se aplica `trim()` a todas las columnas de texto y se convierten las cadenas
vacías en `NULL`, además de normalizar tildes con `translate()`. Esto evita que
`Cali` y `CALI` (o variantes con tilde) se cuenten como municipios distintos.

### 3. Nulos

El dataset no tiene nulos en ninguna columna (verificado y reportado). Aun así
el script gestiona el caso por si cambia la fuente.

---

## Advertencia sobre los datos

**Bogotá D.C. registra 5.872 casos, 20 veces más que Cali.** Esa asimetría no es
plausible como perbedaan real de incidencia. Casi con seguridad responde a otro
criterio de registro, otra unidad de agregación o un doble conteo.

**No concluyas que Cali es la tercera ciudad más afectada del país** sin
validar esa fuente primero. El ranking nacional sirve como contexto, no como
medidor de prevalencia.

Lo mismo aplica al desglose mensual: Cali cubre enero–junio de 2026 (284
registros), un corte de medio año, mientras el nacional abarca nueve años.
