import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Datos (revisar y ajustar si hace falta)
index = [
    "Configuración de subtítulos limitada",
    "Dificultad para transmitir a TV",
    "La búsqueda no es práctica",
    "Navegación poco didáctica",
    "No está el idioma en que quiero ver",
    "No puedo hacer lista de favoritos",
    "Otros"
]

data = {
    "MLA": [1.51, 1.64, 1.89, 1.89, 0.38, 1.26, 3.77],
    "MLB": [0.72, 1.98, 3.06, 1.08, 3.60, 0.72, 4.50],
    "MLM": [0.85, 2.19, 1.34, 1.83, 0.37, 0.85, 1.71]
}

df = pd.DataFrame(data, index=index)

# Paleta de colores (ajustá hex si preferís otros tonos)
colors = {
    "Otros": "#CFCFD3",
    "Configuración de subtítulos limitada": "#A51C30",
    "Dificultad para transmitir a TV": "#E94B3C",
    "La búsqueda no es práctica": "#2F6B6F",
    "Navegación poco didáctica": "#F39C12",
    "No está el idioma en que quiero ver": "#4C9FBF",
    "No puedo hacer lista de favoritos": "#FFD400"
}

# Orden de apilado (desde abajo hacia arriba). Cambialo si querés otro orden visual.
stack_order = [
    "Otros",
    "Configuración de subtítulos limitada",
    "Dificultad para transmitir a TV",
    "La búsqueda no es práctica",
    "Navegación poco didáctica",
    "No está el idioma en que quiero ver",
    "No puedo hacer lista de favoritos"
]

# Preparar la figura
fig, ax = plt.subplots(figsize=(8,5))

bars = list(df.columns)
x = np.arange(len(bars))
bottom = np.zeros(len(bars))

# Apilar categorías
for cat in stack_order:
    vals = df.loc[cat].values
    ax.bar(x, vals, bottom=bottom, color=colors.get(cat, "#888888"), label=cat, width=0.5, edgecolor='none')
    # etiqueta interna si el segmento es suficientemente grande
    for xi, v, b in zip(x, vals, bottom):
        if v >= 0.8:
            ax.text(xi, b + v/2, f"{v:.1f}%", ha='center', va='center', color='white', fontsize=8, fontweight='bold')
    bottom += vals

# Totales encima de cada barra (enteros como en referencias)
totals = df.sum(axis=0).values
for xi, t in zip(x, totals):
    ax.text(xi, t + 0.6, f"{int(round(t))}", ha='center', va='bottom', fontsize=10, fontweight='bold')

# Estética
ax.set_xticks(x)
ax.set_xticklabels(bars, rotation=0)
ax.set_ylim(0, max(totals) + 6)
ax.set_ylabel("Porcentaje (%)")
ax.set_title("Facilidad - 2025-03 (porcentaje)")

# Leyenda debajo
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.14), ncol=3, frameon=False)

# Quitar marco derecho/superior
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig("grafico_facilidad.png", dpi=300, bbox_inches="tight")
plt.show()