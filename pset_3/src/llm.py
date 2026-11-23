"""SOTA con prompting (Claude). Dueño: Tech Lead; solo él tiene ANTHROPIC_API_KEY.

Se evalúa sobre una submuestra de publicaciones del CV y del test: para cada publicación se le
pasan sus candidatos (título + abstract del work, título + descripción del award) y devuelve un
puntaje por par. Las respuestas se cachean en data/processed/llm/ y se registran como artefacto
en MLflow, así el resto del equipo puede reproducir las métricas sin la llave.
"""


def puntuar_pares(pares):
    raise NotImplementedError("Tech Lead")
