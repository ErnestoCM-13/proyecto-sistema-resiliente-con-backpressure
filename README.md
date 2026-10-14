# Sistema resiliente con backpressure — Hot Sale LATAM

Extiende el productor y el consumidor de E-Go03. El productor publica a un ritmo normal de 43 eventos/s con una ventana de 2 minutos a 430. El consumidor verifica cada `inventory_reserved` contra un servicio externo que se degrada bajo carga, protegido por un circuit breaker, y frena su propia lectura cuando el trabajo se acumula.

En diez corridas de 8 minutos, con y sin otro cliente saturando el servicio, **el consumidor agregó 65,131 eventos únicos de 65,131 esperados, verificó las 17,549 reservaciones y no dejó ningún mensaje sin confirmar.**

- [`decisions.md`](decisions.md) — las decisiones con sus cifras.

## Requisitos

| | |
|---|---|
| Go | 1.26.5 (anclado en `.tool-versions`) |
| Docker | con `docker compose` |
| Python 3 | para `inventory_service.py`; solo usa la biblioteca estándar (probado con 3.12.3) |
| `uv` y `numpy==2.4.4` | solo para generar el dataset |
| `curl`, `awk`, `sort` | los usa `scripts/spike.sh` |

## Estructura

```
cmd/
  producer/         publica el archivo al stream; -schedule da el ritmo normal y el spike
  consumer/         consume, deduplica, verifica inventario y agrega
  svcprobe/         mide el servicio de inventario como caja negra
  spikereport/      verifica y resume las corridas del spike
internal/
  breaker/          el circuit breaker
  backpressure/     el buffer de espera y su señal
  inventory/        el cliente del servicio, sus tipos de falla y su unión con el breaker
  pace/             calendarios de ritmo: cuándo le toca salir a cada evento
  stream/           publicación y lectura sobre Redis Streams      (de E-Go03, con un cambio)
  event/ dedup/ aggregate/ clock/                                   (de E-Go03, sin cambios)
scripts/
  spike.sh          corre el spike completo, con y sin carga externa
data/
  inventory_service.py   el servicio simulado que entrega el ejercicio
  spike/                 evidencia de las diez corridas
  spike-summary.json     su resumen
  svcprobe-*.json        mediciones del servicio
```

Para ver qué se extendió del código de E-Go03, el commit `e7a79bd` es la copia literal:

```bash
cd ~/Development/go_course/e-go04 && git diff --stat e7a79bd HEAD -- cmd/producer/main.go cmd/consumer/main.go internal/stream internal/event internal/dedup internal/aggregate internal/clock
```

## 1. El dataset

El archivo es el de E-Go03 y no está versionado. Sí lo están el generador, su salida y un checksum.

En mi máquina `data/eventos_hotsale.jsonl` es un enlace simbólico al archivo de E-Go03. **Si el tuyo también lo es, no corras el generador aquí: escribe a través del enlace.** En un clon limpio el archivo no existe y se genera así:

```bash
cd ~/Development/go_course/e-go04 && uv venv --python 3.12 && uv pip install numpy==2.4.4
```

```bash
cd ~/Development/go_course/e-go04/data && ../.venv/bin/python generate_events_go03.py > /tmp/generator-run.txt
```

```bash
diff /tmp/generator-run.txt ~/Development/go_course/e-go04/data/generator_output.txt && echo mismos-conteos
```

Tarda unos 48 segundos y produce 3,126,750 líneas. El `diff` no debe imprimir diferencias: los conteos salen de la semilla.

**El checksum no va a coincidir con un archivo regenerado.** El generador usa `uuid.uuid4()`, que no toma la semilla, así que cada generación tiene otros identificadores. `data/eventos_hotsale.jsonl.sha256` identifica mi copia. Lo que sí es igual en cualquier copia son los conteos, y `scripts/spike.sh` calcula los esperados a partir del archivo que encuentre.

## 2. Levantar Redis

```bash
cd ~/Development/go_course/e-go04 && cp .env.example .env && docker compose up -d --wait && docker compose ps
```

El contenedor `go04-redis` debe decir `healthy`. El puerto y la memoria máxima salen de `.env`.

## 3. Levantar el servicio de inventario

En su propia terminal, y se deja corriendo:

```bash
cd ~/Development/go_course/e-go04 && python3 data/inventory_service.py 2>/tmp/inventory-service.log
```

Escucha en `http://localhost:8090`. Los errores se mandan a un archivo porque el servicio imprime un `BrokenPipeError` por cada llamada que el consumidor abandona al vencer su plazo de 100 ms; sin la redirección inundan la terminal.

Para comprobar que responde:

```bash
curl -s http://localhost:8090/stats
```

## 4. Tests

Los de integración necesitan Redis levantado. Usan claves con nombre único que borran al terminar.

```bash
cd ~/Development/go_course/e-go04 && go test -race ./...
```

Solo los que no necesitan Redis:

```bash
cd ~/Development/go_course/e-go04 && go test -short ./...
```

Son 82 funciones de test con 240 casos. En mi máquina algunos de integración tardan a veces 5 segundos de más; es el reloj (ver "Cosas que conviene saber").

## 5. El spike completo

Con Redis y el servicio arriba. Corre los dos escenarios, 5 veces cada uno. **Tarda alrededor de hora y media** y no necesita atención.

```bash
cd ~/Development/go_course/e-go04 && MACHINE="describe aquí tu máquina" ./scripts/spike.sh
```

Después, la verificación y el resumen:

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/spikereport
```

Debe decir `5 of 5 runs passed every check` en los dos escenarios. Si alguna corrida falla una verificación, lo dice con su detalle y sale con error.

Para una prueba de 4 minutos con un calendario corto, sin tocar la evidencia versionada:

```bash
cd ~/Development/go_course/e-go04 && MACHINE="describe aquí tu máquina" RUNS=1 SCHEDULE="43:10s,430:30s,43:40s" LOAD_AT=20 LOAD_FOR=10s OUT_DIR=/tmp/spike-smoke ./scripts/spike.sh
```

En esa prueba corta `spikereport` marca una verificación como fallida, la de los dos minutos de spike, que es lo correcto.

Los dos escenarios:

| Escenario | Qué hace |
|---|---|
| `spike` | Solo el calendario. El breaker no abre |
| `spike-with-outside-load` | A media ventana de spike, otro cliente le manda al servicio 150 peticiones/s durante 20 s. El breaker abre y cierra |

Para ver el breaker en una corrida:

```bash
cd ~/Development/go_course/e-go04 && grep breaker bin/logs/spike-with-outside-load-run1-consumer.log
```

Variables de `scripts/spike.sh`:

| | |
|---|---|
| `MACHINE` | obligatoria; queda dentro de cada JSON |
| `RUNS` | corridas por escenario (default 5) |
| `SCENARIOS` | default `"spike spike-with-outside-load"` |
| `SCHEDULE` | default `43:1m,430:2m,43:5m`; segundos o minutos enteros |
| `LOAD_AT`, `LOAD_RATE`, `LOAD_FOR` | cuándo empieza la carga externa (default 110 s), a qué tasa (150/s) y cuánto dura (20s) |
| `DEDUP_MAX` | tamaño de la ventana de deduplicación (default 200000) |
| `OUT_DIR`, `LOG_DIR` | default `data/spike` y `bin/logs` |
| `STREAM`, `GROUP`, `REDIS_CONTAINER`, `REDIS_ADDR`, `INVENTORY_URL`, `FILE` | para apuntar a otro entorno |

## 6. Una corrida a mano

Un escenario de 40 segundos, con tres terminales: el servicio, el consumidor y el productor.

Los conteos esperados, sacados sin pasar por el sistema. Deben imprimir 9206 y 2520:

```bash
head -n 9460 ~/Development/go_course/e-go04/data/eventos_hotsale.jsonl | cut -d'"' -f4 | sort -u | wc -l
```

```bash
head -n 9460 ~/Development/go_course/e-go04/data/eventos_hotsale.jsonl | awk -F'"' '!seen[$4]++ && $8=="inventory_reserved"' | wc -l
```

Dejar todo en cero:

```bash
curl -s -X POST http://localhost:8090/reset
```

```bash
docker exec go04-redis redis-cli -e DEL hotsale:events
```

El consumidor, que se queda esperando el primer mensaje:

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/consumer -expect-unique 9206 -out ""
```

El productor, en otra terminal:

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/producer -schedule "43:10s,430:20s,43:10s" -out ""
```

El consumidor imprime cada vez que el backpressure se enciende o se apaga y termina solo, con una línea `count ... match`. Los terceros:

```bash
curl -s http://localhost:8090/stats
```

```bash
docker exec go04-redis redis-cli -e XPENDING hotsale:events aggregators
```

El servicio debe haber contado 2,520 peticiones y Redis no debe tener nada pendiente.

## 7. Reproducir las mediciones del servicio

Todas piden `-machine` y dejan su JSON en `data/`.

La escalera de tasas, unos 13 minutos:

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/svcprobe -machine "describe aquí tu máquina"
```

La recuperación después de una carga, alrededor de un minuto cada una:

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/svcprobe -mode recovery -overload-rate 116 -out data/svcprobe-recovery-116.json -machine "describe aquí tu máquina"
```

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/svcprobe -mode recovery -overload-rate 400 -out data/svcprobe-recovery-400.json -machine "describe aquí tu máquina"
```

El breaker con 1, 2, 4, 8 y 16 llamadores, unos 7 minutos:

```bash
cd ~/Development/go_course/e-go04 && go run ./cmd/svcprobe -mode breaker -machine "describe aquí tu máquina"
```

## Banderas

**`cmd/producer`**, además de las de E-Go03:

| | |
|---|---|
| `-schedule` | segmentos `tasa:duración`, como `43:1m,430:2m,43:5m`. Sustituye a `-rate` y `-limit` |
| `-out` | JSON con el reporte de pacing por segmento (default `data/producer-report.json`) |

**`cmd/consumer`**, además de las de E-Go03:

| | |
|---|---|
| `-inventory-url` | dirección del servicio (default `http://localhost:8090`) |
| `-check-timeout` | plazo de una verificación (default 100ms) |
| `-breaker-failures` | fallas consecutivas que abren el breaker (default 5) |
| `-breaker-cooldown` | cuánto permanece abierto (default 2s) |
| `-breaker-trials` | pruebas exitosas que lo cierran (default 3) |
| `-buffer-high` | reservaciones en espera a las que se deja de leer el stream (default 200) |
| `-buffer-low` | a las que se vuelve a leer (default 50) |
| `-workers` | verificaciones simultáneas (default 1) |
| `-expect-unique` | eventos únicos que la corrida debe agregar; si no coinciden, sale con error |
| `-read-count` | mensajes por lectura; su default bajó de 1000 a 100 |

**`cmd/svcprobe`**: `-mode` elige entre `ladder`, `recovery`, `breaker` y `load`. Este último manda carga sin tocar los contadores del servicio, y es el "otro cliente" del spike.

## Cosas que conviene saber antes de operarlo

**El consumidor usa un solo worker a propósito.** Con más, el servicio se degrada y el breaker pasa abierto entre el 67% y el 93% del tiempo. Las cifras están en la sección 4 de `decisions.md`.

**Durante el spike se atrasa todo, no solo las reservaciones.** Al encenderse el backpressure el consumidor deja de leer el stream. El evento típico esperó 74 segundos.

**El agregado no se persiste.** Un consumidor reiniciado arranca en cero y vuelve a procesar lo que quedó sin confirmar.

**Volver a correr el productor sobre un stream poblado no agrega nada.** Los IDs de entrada salen del número de línea. Para una corrida limpia hay que borrar el stream; `scripts/spike.sh` lo hace solo.

**En mi máquina el reloj de pared salta y Redis tarda de más en algunas lecturas vacías.** El consumidor lo anota (`redis left a stream read unanswered; reading again`) y sigue. Si ocurre tres veces seguidas se detiene con error.

**Las mediciones de latencia exigen productor y consumidor en la misma máquina**, porque usan `CLOCK_MONOTONIC`.

Las inconsistencias conocidas y las preguntas abiertas están en la sección 6 de [`decisions.md`](decisions.md).
