"""Cobertura de líneas contra el mínimo del Documento de Pruebas (compuerta 1: >= 80%).

Uso: python3 .github/scripts/cobertura.py <formato> <minimo> "<patron>" [...]
Formatos: cobertura (coverlet, .NET), jacoco (Java), lcov (Flutter), texto (resumen "Lines : NN%").
"""
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

formato, minimo = sys.argv[1], float(sys.argv[2])
archivos = sorted({a for p in sys.argv[3:] for a in glob.glob(p, recursive=True)})
if not archivos:
    print(f"::error::No se encontró ningún reporte de cobertura ({' '.join(sys.argv[3:])}).")
    sys.exit(1)

cubiertas = totales = 0
if formato in ("cobertura", "lcov"):
    lineas = {}
    for a in archivos:
        if formato == "cobertura":
            for clase in ET.parse(a).iter("class"):
                f = clase.get("filename")
                for linea in clase.iter("line"):
                    k = (f, linea.get("number"))
                    lineas[k] = lineas.get(k, False) or int(linea.get("hits", "0")) > 0
        else:
            f = None
            for linea in open(a, encoding="utf-8"):
                if linea.startswith("SF:"):
                    f = linea[3:].strip()
                elif linea.startswith("DA:"):
                    n, hits = linea[3:].split(",")[:2]
                    lineas[(f, n)] = lineas.get((f, n), False) or int(hits) > 0
    totales, cubiertas = len(lineas), sum(lineas.values())
elif formato == "jacoco":
    for a in archivos:
        for c in ET.parse(a).getroot().findall("counter"):
            if c.get("type") == "LINE":
                cubiertas += int(c.get("covered"))
                totales += int(c.get("covered")) + int(c.get("missed"))
elif formato == "texto":
    m = None
    for a in archivos:
        m = re.search(r"Lines\s*:\s*([\d.]+)%", open(a, encoding="utf-8", errors="ignore").read()) or m
    if not m:
        print("::error::La salida de las pruebas no trae el resumen 'Lines : NN%'.")
        sys.exit(1)
    cubiertas, totales = float(m.group(1)), 100.0
else:
    print(f"::error::Formato de cobertura desconocido: {formato}")
    sys.exit(1)

porcentaje = round(100.0 * cubiertas / totales, 2) if totales else 0.0
if "GITHUB_STEP_SUMMARY" in os.environ:
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as r:
        r.write(f"### Cobertura de líneas: {porcentaje}% (mínimo {minimo:g}%)\n")
if porcentaje < minimo:
    print(f"::error::Cobertura de líneas {porcentaje}% por debajo del mínimo de {minimo:g}%.")
    sys.exit(1)
print(f"Cobertura de líneas: {porcentaje}% (mínimo {minimo:g}%)")
