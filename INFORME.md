## Flujo general

El Gateway le asigna un `client_id` a cada conexión y etiqueta con él cada mensaje de este.
Al terminar, manda un `EOF` con la cantidad total de registros que envió ese cliente.
Todos los controles guardan su estado separado por `client_id`, así que varios clientes
pueden procesarse en paralelo sin mezclarse.

## Coordinación entre instancias de Sum

Las instancias de Sum consumen de una misma cola, así que cada
registro lo procesa una sola instancia y cada una acumula, por cliente y por fruta, la
suma parcial de lo que leyó.

El problema es que el `EOF` de un cliente lo recibe una sola instancia, y puede llegar
mientras otras todavía tienen registros de ese cliente sin procesar. Para saber cuándo
terminó realmente, se usa un protocolo sobre un exchange de control:

1. La instancia que recibe el `EOF` les avisa a las demás con `CLOSE(cliente, total)`.
2. Cada instancia, al enterarse del cierre (por el `EOF` o por un `CLOSE`), les manda
   a las demás `COUNT(cliente, n)` con los registros que procesó hasta ese momento
   (que puede ser 0). Después, por cada registro nuevo de ese cliente que procese,
   manda `COUNT(cliente, 1)`.
3. Cada instancia suma su propio conteo y los que recibe. Cuando la suma llega al total
   del `EOF`, sabe que todos los registros del cliente fueron procesados y hace el flush.

No hay un nodo que centralice el conteo: cada Sum lleva su propia cuenta y decide de
forma independiente cuándo terminar. Cada Sum usa dos hilos, uno para datos y otro para
mensajes de control, y el estado compartido entre ambos se protege con un lock.

## Coordinación entre Sum y Aggregation

En el flush, cada Sum manda cada par (fruta, cantidad) de un cliente a un único
Aggregator, elegido con `crc32(client_id + fruta) % AGGREGATION_AMOUNT`. Como el hash es
determinístico, todas las instancias de Sum mandan la misma fruta de un mismo cliente
al mismo Aggregator. Esto es lo que hace correcto el cálculo por partes: si las
cantidades de una fruta quedaran repartidas entre varios Aggregators, cada uno vería
solo una fracción y podría dejarla fuera de su top parcial, aunque en total estuviera
entre las primeras.

En un principio se consideró particionar solo por fruta, pero eso limita el paralelismo
a la cantidad de frutas distintas: si hay pocas frutas y muchos Aggregators, algunos
nunca reciben datos. Al incluir el `client_id` en la clave, la misma fruta de distintos
clientes puede ir a distintos Aggregators, y la carga se reparte mejor.

Después de los datos, cada Sum manda un `EOF` del cliente a todos los Aggregators, aun
si no participo en el procesamiento de los registros de ese cliente: cada Aggregator espera exactamente
`SUM_AMOUNT` EOFs por cliente. Como cada Sum envía sus datos antes que su `EOF` por el
mismo canal, al recibir el último `EOF` el Aggregator ya tiene todos los datos. Calcula
su top parcial usando la comparación de `FruitItem` y lo manda al Join, incluso vacío
si no le tocó ninguna fruta de ese cliente.

El Join espera `AGGREGATION_AMOUNT` tops parciales por cliente. Como cada fruta de un
cliente está en un solo Aggregator, el top global está contenido en la unión de los
tops parciales: los ordena con la comparación de `FruitItem` y se queda con los primeros.

## Escalabilidad

- **Clientes:** el estado está separado por `client_id` en todos los controles, así que
  los clientes concurrentes no se bloquean entre sí.
- **Sums:** todas las instancias consumen de la misma cola de entrada, así que agregar
  Sums reparte los registros entre más réplicas. La memoria usada depende de la cantidad
  de frutas distintas, no de la cantidad de registros totales. Además, los
  `COUNT(cliente, 1)` están acotados: el Gateway publica el `EOF` después de todos los
  datos del cliente, y la cola los entrega en orden, con `prefetch_count=1`. Entonces,
  cuando una instancia recibe el `EOF`, cada una de las demás tiene a lo sumo un
  registro de ese cliente sin procesar, y el cierre cuesta del orden de dos `COUNT` por
  instancia, sin importar cuántos registros haya mandado el cliente.
- **Aggregators:** el reparto por hash de (cliente, fruta) distribuye el trabajo entre
  todas las réplicas, así que agregar Aggregators reparte la carga sin cambiar el diseño.
- **Volumen de datos:** los registros crudos solo llegan a los Sums. Después el volumen
  se reduce: a los Aggregators les llega un mensaje por cada (cliente, fruta) desde cada Sum y al Join como mucho
  `TOP_SIZE` frutas por Aggregator. Un cliente con muchos registros genera entre etapas
  el mismo tráfico que uno con pocos, si tiene las mismas frutas.