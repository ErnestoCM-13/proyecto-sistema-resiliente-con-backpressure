# Decisiones — E-Go04, sistema resiliente con backpressure

En este documento menciono las decisiones justificandolas con sus números.

## Entorno de medición

Todas las cifras de este documento se midieron en la misma máquina:

| | |
|---|---|
| CPU | AMD Ryzen 5 5600G, 6 núcleos / 12 hilos |
| RAM | 7.7 GiB |
| SO | Ubuntu 24.04 sobre WSL2 (Windows) |
| Go | 1.26.5 |
| Broker | Redis 7.4 en Docker, AOF `everysec`, `maxmemory 3gb`, política `noeviction` |
| Servicio externo | `data/inventory_service.py` con Python 3.12.3, en el mismo equipo |
| Dataset | `eventos_hotsale.jsonl` de E-Go03: 3,126,750 líneas, 662,973,067 bytes |

**Importante.** El documento del ejercicio pide reutilizar el archivo de E-Go03 sin regenerarlo. Así lo hice durante todo el desarrollo y en las diez corridas de evidencia. Pero después de producir esa evidencia, el archivo original se sobrescribió por un error con uno regenerado. El generador usa `uuid.uuid4()`, que no toma la semilla, así que el archivo nuevo tiene otros identificadores y otro SHA-256 (`a7fc03ed…40e43f` en lugar de `968cd747…53d2f9`). Todo lo demás sale de la semilla y es idéntico. Esto lo verifiqué sobre el archivo nuevo:

| Prefijo | Eventos únicos | Reservaciones | Duplicados | ¿Igual que con el archivo original? |
|---|---|---|---|---|
| 9,460 líneas | 9,206 | 2,520 | 254 | Sí |
| 15,050 líneas | 14,629 | 3,983 | 421 | Sí |
| 67,080 líneas | 65,131 | 17,549 | 1,949 | Sí |
| 100,000 líneas | 97,060 | 26,143 | 2,940 | Sí |

Para el prefijo de 67,080 líneas coinciden además los conteos por tipo, por país y los montos con el agregado que guardó el consumidor. **La evidencia de `data/spike/` se produjo con el archivo anterior**; sus cifras se reproducen con el actual pero sus bytes de entrada no. El `.sha256` versionado en este repo describe el archivo actual.

## 1. Escenario

El ejercicio pide usar mi propia cifra de E-Go03 como ritmo normal.

| | |
|---|---|
| Ritmo normal | **43 eventos/s**, la tasa real del Hot Sale medida en E-Go03 |
| Spike | **430 eventos/s durante 120 s** continuos, 10 veces el ritmo normal |
| Calendario | 60 s normales, 120 s de spike, 300 s normales (`43:1m,430:2m,43:5m`) |
| Líneas publicadas | 67,080 |
| Eventos únicos, contados fuera del sistema | 65,131 |
| Reservaciones de inventario únicas | 17,549, el 26.9% de los eventos |

A 43 eventos/s las 3,126,750 líneas tardarían 20.2 horas. El escenario usa un prefijo, y el total esperado no es "el mismo de Go03" sino el de ese prefijo, calculado aparte con `sort -u` sobre las mismas líneas.

Durante el spike llegan **115.8 reservaciones por segundo**. lo cual es el problema de este ejercicio.

### Qué aguanta el servicio

Lo medí como una caja negra, sin usar los umbrales del script: 8 tasas de llegada, 15 s cada una, 5 corridas (`data/svcprobe-ladder.json`). Mediana y rango:

| Llegadas/s | Fallas | p50 de los éxitos | Éxitos/s | Llamadas en vuelo |
|---|---|---|---|---|
| 12 | 0.0% | 22.2 ms | 12.0 | 1 |
| 30 | 0.0% | 21.8 ms | 30.0 | 1 |
| 60 | 8.2% [6.8–9.7] | 30.3 ms | 55.1 | 12 |
| 90 | 19.0% [18.5–19.9] | 298.5 ms | 72.9 | 28 |
| 116 | 21.3% [21.1–23.1] | 380.9 ms | 91.3 | 45 |
| 180 | 31.1% [30.0–31.3] | 581.8 ms | 124.1 | 106 |
| 250 | 54.7% [53.7–55.4] | 802.2 ms | 113.1 | 197 |
| 400 | 69.5% [69.2–70.4] | 1,504.3 ms | 121.9 | 215 |

Con tres resultados de esa tabla puedo respaldar las decisiones que tomé:

- **En estas pruebas, el servicio se mantuvo estable solo hasta 30/s.** A 60/s ya falla el 8.2% y el p95 sube a 205 ms.
- **A la tasa del spike (116/s) falla el 21.3% y cada respuesta tarda 381 ms.** El 503 tarda lo mismo que el éxito: 381.2 ms contra 380.9.
- **Entre el comportamiento estable y el degradado hay un intervalo.** La respuesta sana más lenta de 3,150 tardó 45.5 ms; la degradada más rápida, 204.3 ms.

## 2. Qué significa "no perder datos"

**Un evento está procesado cuando entró al agregado, y solo entonces se confirma en Redis con `XACK`.** Todo lo que todavía no cumple eso está en uno de dos lugares, y en los dos sigue existiendo en Redis:

| Dónde espera | Qué es | Límite | Qué pasa si se llena |
|---|---|---|---|
| En el stream, sin leer | Lo que el consumidor aún no pide. Durable (AOF) y sin reloj de reentrega | 3 GB de `maxmemory` | Redis rechaza el `XADD` (`noeviction`) y el productor termina con error. No se borra nada. La publicación es idempotente, así que repetirla es seguro |
| En el buffer de espera, en memoria | Reservaciones ya leídas que esperan su verificación. Siguen sin confirmar en Redis | 299, con capacidad para 300 | No puede ocurrir si el lector respeta la señal (§3). Si ocurriera, `Put` devuelve `ErrFull` y el consumidor se detiene con error |

**Con este escenario, el stream no debería llenarse.** Los 51,600 eventos del spike ocupan unos 13 MB, a los 260 B por evento medidos en E-Go03. Es el 0.4% de los 3 GB.

### Reglas

Cada regla tiene una prueba de integración que falla si se elimina (`cmd/consumer/consume_test.go`):

| Regla | Test |
|---|---|
| `XACK` solo después de agregar. Una reservación en el buffer sigue pendiente en Redis | `StopsOnAnAnswerItDoesNotUnderstand...`, `InterruptedLeavesUnfinishedWork...` |
| El consumidor no reclama trabajo abandonado (`XAUTOCLAIM`) mientras tenga trabajo propio pendiente | `DoesNotTakeItsOwnWaitingWorkForAbandoned` |
| El `XACK` de algo ya agregado se envía aunque el proceso se esté apagando | `StopsOnAnAnswerItDoesNotUnderstand...` |

Incluí la segunda regla porque, sin ella, el consumidor reclama sus propias reservaciones en espera, la ventana de deduplicación las toma por duplicados y las confirma sin haberlas procesado.

### Qué se atrasa y cuánto

Decidí mantener los eventos atrasados en el stream: al encenderse la señal, el consumidor deja de leer. **El costo es que se atrasan todos los tipos de evento**, no solo las reservaciones, porque comparten el stream. Lo medí en 5 corridas del spike:

| Latencia de punta a punta | Mediana [mín–máx] |
|---|---|
| p50 | 74.2 s [73.4–75.2] |
| p95 | 157.2 s [156.2–160.2] |
| Máxima | 168.6 s [167.6–171.6] |

El sistema se pone al día unos 80 s antes de que el productor termine, todavía recibiendo tráfico normal.

Con base en esta evidencia, descarté las alternativas: un buffer grande en memoria tendría miles de eventos leídos y sin confirmar durante minutos, expuestos al reloj de reentrega de 30 s; un desborde a disco duplicaría lo que Redis ya hace.

### Alcance de la garantía

- Al igual que en E-Go03, **el agregado no se persiste**. Un consumidor reiniciado arranca en cero y vuelve a procesar lo que quedó sin confirmar. La garantía es que no se pierdan eventos pendientes; un reinicio no conserva las cuentas del agregado.
- En teoría, **Redis con AOF `everysec` puede perder hasta un segundo de escrituras si Redis mismo se cae.** Pero no lo probé.
- **Lo que sí pude comprobar en las pruebas** fue que una interrupción a media corrida, seguida de un segundo consumidor que retoma, suma exactamente todos los eventos (`InterruptedLeavesUnfinishedWork...`).

## 3. Backpressure

**Elegí la profundidad del buffer de espera, con dos umbrales.** Se enciende al llegar a 200 reservaciones esperando y se apaga al bajar a 50. Encendida, el consumidor deja de leer del stream. lo implementé en `internal/backpressure`.

### Por qué esa y no las otras dos

| Candidata | Evidencia |
|---|---|
| Tasa de error del servicio | A 116/s solo falla el 21.3% y cada falla tarda 381 ms en llegar. Es ruidosa y lenta. Además es la entrada del breaker |
| Latencia del servicio | Separa sano de degradado (hueco de 46 a 204 ms), pero mide a la dependencia. Con el servicio sano en 22 ms el trabajo se acumula igual si llega más de lo que se le puede mandar, que es exactamente lo que pasa en el spike |
| **Profundidad del buffer** | Mide directamente trabajo que se acumula más rápido de lo que se procesa, sea cual sea la causa. |

El defecto de la profundidad es que reacciona después de que el trabajo ya se acumuló. Lo acepté porque lo acumulado está acotado y no se pierde.

### Por qué 200, 50 y lecturas de 100

El worker verifica 46.3 reservaciones/s (§4), así que un elemento en la posición N espera N/46.3 segundos en memoria. La profundidad máxima es el umbral alto más lo que traiga la última lectura.

| Alto / bajo / lectura | Profundidad máxima (típica / peor) | Espera máxima en memoria |
|---|---|---|
| 100 / 25 / 100 | 126 / 199 | 2.7 s / 4.3 s |
| **200 / 50 / 100** | **226 / 299** | **4.9 s / 6.5 s** |
| 400 / 100 / 100 | 426 / 499 | 9.2 s / 10.8 s |
| 200 / 50 / 1000 | 470 / 1,199 | 10.2 s / 25.9 s |

La restricción no es la memoria (mil eventos son menos de 1 MB) sino el reloj de reentrega de 30 s, ya que la espera en memoria debe quedar lejos de él. Con 200/50 queda en segundos. Bajar la lectura de 1,000 (el default de E-Go03) a 100 cambia ese default; el 1,000 era para la prueba de saturación.

### Resultados observados

En 5 corridas del spike (`data/spike-summary.json`):

| | Mediana [mín–máx] |
|---|---|
| Veces que se encendió | 99 [99–99] |
| Tiempo encendida | 334.4 s [333.4–337.4] |
| Profundidad máxima | 226 [226–231], contra un límite de 299 |
| Se enciende por primera vez | 3 s después de empezar el spike |

Cada transición queda en la salida del consumidor con su instante y su profundidad, y en el JSON de cada corrida.

## 4. Circuit breaker

Implementado en `internal/breaker` y unido al cliente HTTP en `internal/inventory` (`Guarded`).

| Parámetro | Valor | Evidencia |
|---|---|---|
| Timeout por llamada | **100 ms** | El hueco vacío entre 45.5 ms (lo sano más lento) y 204.3 ms (lo degradado más rápido) |
| Fallas consecutivas para abrir | **5** | Con ese timeout un servicio degradado falla todas las llamadas: abre a la quinta, en medio segundo. Sano, hubo 0 fallas en unas 3,800 respuestas |
| Cooldown | **2 s** | El servicio se recupera en 600 ms desde 116/s y en 900 ms desde 400/s, idéntico en 5 corridas de cada caso. 2 s es más del doble de lo peor medido |
| Llamadas de prueba en half-open | **3**, de una en una | Tras recuperarse, ninguna de 650 sondas falló, es decir, no hay estado "recuperado a medias" |

**Qué cuenta como falla:** un 503, un timeout o no poder conectar. Una respuesta que el contrato no nombra (un 404) no la cuenta el breaker, sino que detiene al consumidor. Un Ctrl+C tampoco cuenta.

**Una respuesta lenta es una falla.** Con el timeout dentro del hueco, el 100% de las llamadas degradadas fallan y el breaker abre de inmediato. Con un timeout por encima de todo (2.5 s) solo contaría el 503, el 21.3% de las llamadas; cinco seguidos ocurren el 0.04% de las veces y el breaker casi nunca abriría. El costo es que se descartan respuestas que habrían llegado bien, y el servicio hace ese trabajo de todos modos.

### Comportamiento aislado

Contra el servicio real, con N llamadores a máxima velocidad, 15 s por escalón, 5 corridas (`data/svcprobe-breaker.json`):

| Workers | Éxitos/s | Aperturas en 15 s | Tiempo abierto |
|---|---|---|---|
| **1** | **46.3** [45.5–46.7] | 0 | 0% |
| 2 | 20.7 [20.5–21.0] | 5 | 67% |
| 4 | 24.0 [24.0–24.0] | 6 | 78% |
| 8 | 28.0 [28.0–28.0] | 7 | 85% |
| 16 | 23.7 [23.0–24.2] | 7 | 93% |

**Por esos resultados, el consumidor usa un solo worker.** Una respuesta sana tarda 22 ms, así que un llamador no pasa de unas 45 llegadas/s y nunca degrada al servicio. Con más, la ventana del servicio se llena en menos de un segundo y cada vez terminan costando 2 s de cooldown.

En las 110 veces que el breaker pasó de abierto a half-open lo hizo entre **2,000.1 y 2,015.4 ms** después de abrir; las tres pruebas tardaron entre 44.5 y 87.1 ms; ninguna falló. Con 1, 2 y 4 workers el servicio contó exactamente las llamadas que el breaker dejó pasar.

### Comportamiento en el spike

**Con un worker, mi propia carga nunca abre el breaker**: 0 aperturas en las 5 corridas del spike. Para verlo actuar tuve que provocar la falla desde fuera: otro cliente (`svcprobe -mode load`) le manda al servicio 150 peticiones/s durante 20 s, a media ventana de spike. Fué una falla inducida.

| Con carga externa, 5 corridas | Valor |
|---|---|
| Aperturas | 10, en las cinco |
| Tiempo abierto | 20.0 s, en las cinco |
| Abre | 0.6 s después de que empieza la carga externa |
| Cierra | 1.6 s después de que termina |
| Llamadas fallidas | 14: las 5 que lo abren y 9 pruebas |
| Llamadas rechazadas sin tocar la red | 10 |

## 5. Evidencia de que el spike no perdió nada

Diez corridas de 8 minutos, cinco por escenario, con `scripts/spike.sh`. Los archivos están en `data/spike/` y el resumen en `data/spike-summary.json`.

**Las diez ejecuciones produjeron exactamente los mismos resultados:**

| | Esperado, contado fuera del sistema | Las 10 corridas |
|---|---|---|
| Líneas publicadas | 67,080 | 67,080 |
| Eventos únicos agregados | 65,131 (`sort -u`) | **65,131** |
| Duplicados descartados | 1,949 | **1,949** |
| Reservaciones verificadas | 17,549 (`awk`) | **17,549** |
| Mensajes sin confirmar en Redis | 0 (`XPENDING`) | **0** |
| Largo del stream | 67,080 (`XLEN`) | 67,080 |

El conteo por tipo también coincide con el calculado aparte: 25,708 `order_placed`, 21,874 `payment_confirmed` y 17,549 `inventory_reserved`.

**Tres fuentes independientes del consumidor respaldan el resultado:** el script cuenta los eventos únicos con herramientas de shell; Redis dice cuánto quedó pendiente; y el servicio de inventario cuenta sus propias peticiones. En el escenario con carga externa el servicio contó 20,563 en las cinco corridas, que son las 17,563 llamadas del consumidor más las 3,000 del otro cliente.

`cmd/spikereport` aplica 12 verificaciones a cada corrida del spike solo y 13 a cada una con carga externa. **Las diez ejecuciones cumplieron todas las verificaciones.**

### El spike se sostuvo

| Segmento | Tasa pedida | Tasa lograda (mediana de 5) |
|---|---|---|
| 1, 60 s | 43/s | 43.00 |
| 2, 120 s | 430/s | 430.00 |
| 3, 300 s | 43/s | 43.00 |

### Dos escenarios

Los escenarios difieren en una sola variable: la carga externa.

| Mediana [mín–máx] de 5 corridas | Spike solo | Spike con carga externa |
|---|---|---|
| Aperturas del breaker | 0 | 10 |
| Tiempo con el breaker abierto | 0 s | 20.0 s |
| Veces que se encendió el backpressure | 99 | 101 |
| Tiempo con backpressure | 334.4 s [333.4–337.4] | 364.2 s [363.4–367.1] |
| Profundidad máxima del buffer | 226 [226–231] | 226 [226–228] |
| Latencia p50 | 74.2 s | 96.0 s |
| Latencia p95 | 157.2 s | 180.0 s |
| Latencia máxima | 168.6 s | 191.5 s |
| Se pone al día antes del fin | ~80 s | ~50 s |
| Del primer al último evento | 480.0 s | 480.0 s |

Veinte segundos de saturación del servicio añadieron unos 22 s de latencia y 30 s de atraso. En las pruebas, no se perdió ningún evento.

## 6. Inconsistencias conocidas y preguntas abiertas

**El simulador premia sobrecargarlo.** Ofreciéndole 180 llegadas/s entrega 124 éxitos/s, más que las 115.8 que pide el spike. Se podría seguir el spike en tiempo real a costa de 31% de llamadas desperdiciadas, 582 ms por respuesta y 106 llamadas en vuelo. Pero elegí no hacerlo.

**El servidor de Python tiene una cola de 5 conexiones pendientes** (`request_queue_size` de `socketserver`), que no está entre los umbrales del script. El contador `ListenOverflows` del kernel se mueve a partir de 250 llegadas/s y con 8 o más llamadores simultáneos; no se movió a 116/s ni con un worker. Con 16 workers el breaker abre a los 0.1 s por llamadas que nunca llegaron (entre 44 y 52 por corrida).

**El simulador usa `random` sin semilla.** Dos corridas no dan las mismas fallas. Por eso decidí que todo se reportara con 5 corridas, mediana y rango.

**El reloj de pared de mi máquina salta, lo cual alarga las lecturas bloqueantes de Redis.** El 9 de octubre medí un adelanto de 15.25 s durante unos 70 ms cada 5 s. De 38 lecturas bloqueantes de 2 s, 33 se alargaron: la mayoría a 5.0 s, varias a unos 10 y 15 s, la peor a 15.76 s. go-redis espera el bloqueo más 10 s, así que una lectura de 2 s alargada a más de 12 devuelve `i/o timeout`. El consumidor lo reconoce como `stream.ErrBlockOverran`, vuelve a leer, lo cuenta en el reporte y se rinde a la tercera seguida. Ocurrió una vez en las diez corridas, pero esa corrida terminó bien. Según lo que investigué, **que Redis calcule el vencimiento con el reloj de pared es una inferencia a partir de los tiempos; pero no lo verifiqué en código.** Tampoco medí cada cuánto ocurrirían tres seguidas.

**Quedaron dos observaciones sin explicación.** En una corrida, un evento del tercer segmento salió 341 ms tarde; en las otras nueve el peor retraso de ese segmento fue de 2 a 3 ms. Y en una repetición de los tests, Redis tardó 1.5 s en contestar dos comandos a la vez.

**Código repetido en las herramientas.** `dueOffsets` de `svcprobe` repite lo que hace `internal/pace`, y `Spread` está copiada en `svcprobe` y en `spikereport`. Estas son herramientas de medición, y la evidencia ya estaba generada con ese código cuando me dí cuenta de esta repetición; no la unifiqué por falta de tiempo.

**Heredado de E-Go03 y sin cambios:** dos errores de Redis se clasifican por su texto (`BUSYGROUP` y el rechazo de un `XADD` repetido); la latencia exige productor y consumidor en la misma máquina, porque usa `CLOCK_MONOTONIC`; y los timestamps del evento se leen sin zona horaria. Nada de E-Go04 depende de ese timestamp: la deduplicación usa el instante de llegada y el pacing usa su propio calendario.

## 7. Si la escala cambia

**Si el servicio pudiera soportar una carga mayor**, el número de workers es una bandera, y la medición con la que se justifica se puede repetir con los comandos:

```bash
go run ./cmd/svcprobe -mode breaker -workers 1,2,4,8 -machine "..."
```

```bash
go run ./cmd/consumer -workers 4 -buffer-high 400 -buffer-low 100
```

**Si el spike durara más o tuviera una tasa mayor**, el atraso se calcula: crece a (llegadas × 0.269 − 46.3) reservaciones por segundo y ocupa 260 B por evento en Redis. Los 3 GB actuales guardan unos 11.5 millones de eventos; el límite sube con `REDIS_MAXMEMORY` en `.env`.

**Un segundo consumidor en el grupo no funciona sin otro cambio.** La ventana de deduplicación vive en el proceso, así que un evento y su duplicado podrían caer en consumidores distintos y contarse dos veces. Habría que llevar la ventana a Redis, por ejemplo con `SET dedup:<event_id> 1 NX EX 300` antes de procesar.
