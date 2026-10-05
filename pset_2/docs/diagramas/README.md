# Diagramas del PSet 2

| Diagrama | Imagen | Fuente (Graphviz) |
|---|---|---|
| Arquitectura implementada (infraestructura y flujo de datos) | [`arquitectura.png`](arquitectura.png) | [`arquitectura.dot`](arquitectura.dot) |
| Modelo dimensional de la capa GOLD (star schema) | [`star_schema.png`](star_schema.png) | [`star_schema.dot`](star_schema.dot) |

![Arquitectura](arquitectura.png)

![Star schema](star_schema.png)

## Editar y regenerar

Los `.dot` se dibujan con [Graphviz](https://graphviz.org/) y la fuente Liberation Sans. Desde esta carpeta,
sin instalar nada (solo Docker):

```bash
docker run --rm -v "$PWD":/data -w /data alpine:3.20 sh -c \
  'apk add -q graphviz font-liberation font-dejavu &&
   for d in arquitectura star_schema; do dot -Tpng -Gdpi=220 $d.dot -o $d.png; done'
```

Con Graphviz instalado basta `dot -Tpng -Gdpi=220 arquitectura.dot -o arquitectura.png`.

Son los mismos diagramas del memo técnico (Figuras 1 y 2); el memo usa una versión PDF vectorial.
