# Guía paso a paso – Taller Event-Driven con RabbitMQ y Docker (Escenario 1: Centralizado)

## 0. Qué construiste

```
                         ┌──────────── Nodo único (tu PC) ─────────────┐
 PRODUCTORES             │                                              │         CONSUMIDORES
 producer-orders (Py)  ──┤                                              ├─► consumer-billing (Py)        orders.* , payments.*
 producer-payments(JS) ──┤     RabbitMQ  ──  exchange "eventos" (topic) ├─► consumer-stock (Go)          orders.created , inventory.*
 producer-inventory(Go)──┤                                              ├─► consumer-notifications (JS)  payments.* , shipping.*
 producer-shipping(JS) ──┤                                              ├─► consumer-audit (Py)          #  (todo)
                         └──────────────────────────────────────────────┘
```

| Evento (routing key) | Lo publica | Lo reciben (colas) |
|---|---|---|
| `orders.created` | producer-orders | billing, stock, audit |
| `payments.completed` | producer-payments | billing, notifications, audit |
| `inventory.updated` | producer-inventory | stock, audit |
| `shipping.dispatched` | producer-shipping | notifications, audit |

Lenguajes: Python, Node.js y Go (cumple el mínimo de 3). Exchange tipo **topic**, colas durables, `ack` manual.

---

## 1. Requisitos previos

1. Instala **Docker Desktop** (Windows/Mac) o Docker Engine + Compose plugin (Linux).
2. Verifica:
   ```bash
   docker --version
   docker compose version
   ```
3. Abre una terminal en la carpeta `taller-rabbitmq/` (donde está `docker-compose.yml`).

> Los puertos **5672** y **15672** deben estar libres.

---

## 2. Despliegue (Escenario 1)

### Paso 2.1 – Construir las imágenes
```bash
docker compose build
```
La primera vez tarda unos minutos (descarga dependencias de Python, Node y Go).

### Paso 2.2 – Levantar todo
```bash
docker compose up -d
```
RabbitMQ arranca primero (healthcheck); los demás servicios esperan a que esté sano.

### Paso 2.3 – Verificar que los 9 contenedores corren
```bash
docker compose ps
```
Deben aparecer: `rabbitmq` + 4 productores + 4 consumidores en estado `running` (rabbitmq en `healthy`).

### Paso 2.4 – Ver logs
```bash
docker compose logs -f                      # todos
docker compose logs -f producer-orders consumer-billing   # algunos
```
Salida esperada, por ejemplo:
```
producer-orders   | [producer-orders] -> orders.created: pedido #4821
consumer-billing  | [consumer-billing] Factura generada para pedido #4821 ...
consumer-stock    | [consumer-stock] Reservando 2 x Laptop para pedido #4821
consumer-audit    | [consumer-audit] #17 AUDITADO orders.created ...
```
Salir de los logs: `Ctrl + C` (los contenedores siguen corriendo).

---

## 3. Panel de administración de RabbitMQ

Abre **http://localhost:15672** → usuario `admin`, contraseña `admin123`.

- **Overview**: tasa de mensajes publicados/entregados.
- **Connections** (8 conexiones, una por servicio).
- **Exchanges** → `eventos` (tipo `topic`) → sección *Bindings* muestra qué colas están enlazadas.
- **Queues**: `billing.queue`, `stock.queue`, `notifications.queue`, `audit.queue`.
- Click en una cola → *Bindings* (routing keys) y *Get messages*.

---

## 4. Pruebas que debes realizar (y que sirven de evidencia)

1. **Flujo normal**: ver logs de productores y consumidores en paralelo.
2. **Enrutamiento selectivo**: comprobar que `consumer-notifications` NUNCA imprime `orders.created` y que `consumer-audit` imprime todo.
3. **Tolerancia al fallo de un consumidor**:
   ```bash
   docker compose stop consumer-billing
   ```
   Espera ~30 s: en la cola `billing.queue` (panel) verás los mensajes acumularse en *Ready*. Luego:
   ```bash
   docker compose start consumer-billing
   ```
   Los mensajes acumulados se consumen y la cola vuelve a 0. (Demuestra desacoplamiento y persistencia).
4. **Publicar un evento manual** (opcional): panel → Exchanges → `eventos` → *Publish message* → routing key `orders.created`, payload JSON de ejemplo.

---

## 5. Capturas de pantalla (lista exacta)

Guárdalas con nombre `captura-XX-descripcion.png`.

| # | Qué capturar | Cómo |
|---|---|---|
| 01 | Versiones de Docker | `docker --version` y `docker compose version` |
| 02 | Estructura del proyecto | `tree` o el explorador de archivos |
| 03 | Build exitoso | final de `docker compose build` |
| 04 | Contenedores corriendo | `docker compose ps` |
| 05 | Logs de productores | `docker compose logs --tail=20 producer-orders producer-payments producer-inventory producer-shipping` |
| 06 | Logs de consumidores | igual con los 4 consumidores |
| 07 | Panel – Overview | gráfica de mensajes |
| 08 | Panel – Connections | 8 conexiones |
| 09 | Panel – Exchanges | fila `eventos` tipo topic |
| 10 | Panel – detalle del exchange `eventos` | tabla de *Bindings* |
| 11 | Panel – Queues | 4 colas con sus tasas |
| 12 | Panel – detalle de una cola | bindings de `audit.queue` (`#`) |
| 13 | Prueba de fallo: cola acumulando | `billing.queue` con mensajes *Ready* > 0 |
| 14 | Prueba de fallo: recuperación | cola otra vez en 0 + logs de billing procesando |

---

## 6. Grabación del video (máx. 10 min)

Herramientas: OBS Studio, Win+G (Windows), QuickTime (Mac) o Zoom (grabando tu pantalla). Resolución 1080p, fuente grande en la terminal (Ctrl +).

**Guion sugerido (≈ 8 min)**

| Min | Contenido |
|---|---|
| 0:00–0:45 | Presentación: nombre, objetivo del taller, escenario 1 (centralizado) y diagrama de la arquitectura. |
| 0:45–2:00 | Recorrido del código: carpeta de cada servicio, mostrar que hay Python, Node y Go; abrir un productor y un consumidor y explicar exchange/routing key/binding. |
| 2:00–3:00 | Mostrar `docker-compose.yml`: servicio rabbitmq, healthcheck, redes y servicios. |
| 3:00–4:00 | `docker compose build` (puedes acelerar/cortar) y `docker compose up -d`. |
| 4:00–4:30 | `docker compose ps` → los 9 contenedores. |
| 4:30–5:45 | `docker compose logs -f`: mostrar productores publicando y consumidores reaccionando. Señalar el enrutamiento (qué consumidor recibe qué). |
| 5:45–7:15 | Panel web: Exchanges → bindings, Queues, Connections, gráfica de mensajes. |
| 7:15–8:30 | Prueba de fallo: `stop consumer-billing`, ver acumulación, `start`, ver recuperación. |
| 8:30–9:00 | Cierre: conclusiones breves. `docker compose down`. |

Tips: ensaya una vez, ten las ventanas abiertas de antemano, no muestres contraseñas personales, y verifica el audio antes de grabar todo.

> Nota: el enunciado pide el video en los **tres** escenarios. Como solo implementamos el centralizado, si luego te piden los otros dos, avísame y extendemos el proyecto.

---

## 7. Informe (máx. 4 páginas) – estructura recomendada

**Portada compacta** (no una hoja completa): título, nombres, curso, fecha.

### 7.1 Introducción (½ párrafo)
Objetivo del taller y descripción del caso (sistema de comercio con pedidos, pagos, inventario y envíos).

### 7.2 Diseño del sistema (≈ 1 página)
- **Diagrama arquitectural** del Escenario 1 (copia el de la sección 0 o rehazlo en draw.io/diagrams.net/Excalidraw). Debe mostrar: nodo único, red Docker, broker, 4 productores, 4 consumidores, lenguajes, exchange, colas y routing keys.
- **Tabla** de eventos → productor → consumidores (la de la sección 0).
- Decisiones: exchange `topic`, colas durables, mensajes persistentes, `ack` manual, reintentos de conexión, healthcheck.

### 7.3 Implementación (≈ ½ página)
Tecnologías (Python/pika, Node/amqplib, Go/amqp091-go), un Dockerfile por servicio y un `docker-compose.yml`. Explica brevemente cómo se despliega (los 3 comandos).

### 7.4 Evidencias de ejecución (≈ 1–1.5 páginas)
Inserta capturas **pequeñas pero legibles**, cada una con pie de figura ("Figura 3. Colas en el panel de RabbitMQ"): ps (04), logs (05–06), exchange/bindings (10), colas (11) y prueba de fallo (13–14). Comenta en 1–2 líneas qué demuestra cada una.

### 7.5 Análisis del escenario centralizado (≈ ½–1 página)
| | Detalle |
|---|---|
| Ventajas | Despliegue simple (un `compose up`), latencia mínima (red local de Docker), fácil de depurar, sin configuración de red entre máquinas. |
| Desventajas | Punto único de falla (si cae el nodo cae todo), recursos compartidos (CPU/RAM), no escala horizontalmente, no refleja un entorno real. |
| Retos técnicos | Orden de arranque (solucionado con healthcheck + reintentos), definir bindings correctos, persistencia de mensajes, manejo de ack, diferencias de librerías entre lenguajes. |

Añade una frase de comparación con los escenarios 2 y 3 *a nivel teórico* (mayor aislamiento del broker, más latencia de red, necesidad de usar IP/DNS en vez del nombre `rabbitmq`, mejor tolerancia a fallos parcial) y aclara que en este trabajo solo se implementó el escenario 1.

### 7.6 Conclusiones (3–4 líneas)
Desacoplamiento, interoperabilidad entre lenguajes mediante AMQP y limitaciones de la centralización.

### Checklist de formato
- [ ] ≤ 4 páginas (reduce imágenes si te pasas; márgenes 2 cm, fuente 11 pt).
- [ ] Todas las figuras numeradas y referenciadas en el texto.
- [ ] Enlace al video y al repositorio/zip.
- [ ] Exporta a PDF.

---

## 8. Entregables – checklist final

- [ ] Código fuente de los 4 productores y 4 consumidores.
- [ ] Un `Dockerfile` por servicio (8).
- [ ] `docker-compose.yml` (Escenario 1).
- [ ] Informe PDF (≤ 4 páginas) con diagrama, evidencias y comparación.
- [ ] Video ≤ 10 min.

---

## 9. Comandos útiles y solución de problemas

```bash
docker compose down            # detener y eliminar contenedores
docker compose down -v         # además borra el volumen (empieza desde cero)
docker compose up -d --build   # reconstruir tras cambiar código
docker compose logs rabbitmq   # ver logs del broker
docker compose restart consumer-billing
```

| Problema | Solución |
|---|---|
| `port is already allocated` | Otro RabbitMQ usa 5672/15672. Ciérralo o cambia el puerto izquierdo en el compose (`"5673:5672"`). |
| Un servicio dice "RabbitMQ no disponible" | Normal los primeros segundos; reintenta solo. Si persiste, mira `docker compose logs rabbitmq`. |
| Error al construir Go (`proxy.golang.org`) | Revisa tu conexión a Internet / VPN; el build descarga módulos. |
| No entra al panel | Espera ~30 s tras `up`, usa `http://localhost:15672` (usuario `admin`, clave `admin123`). |
| Las colas aparecen con otras propiedades y falla `PRECONDITION_FAILED` | Ejecuta `docker compose down -v` y vuelve a levantar. |
