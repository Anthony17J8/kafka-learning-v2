# Модуль 0. Окружение

> **Вопрос**: сколько слушающих сокетов внутри контейнера и почему их больше, чем было в лабе 0.2?

**Ответ:** внутри контейнера слушающих сокетов больше.   
``INTERNAL://:19092`` для доступа внутри контейнера.
``EXTERNAL://:9092`` для доступа клиентов с хоста
``CONTROLLER://:9093`` сокет для контроллера

> **При поломке**:
> 1) ``KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://kafka:9092``
> 2)

1) Ошибка в логах:

```
[2026-09-07 12:11:08,905] WARN [AdminClient clientId=adminclient-1] Error connecting to node kafka:9092 (id: 1 rack: null isFenced: false) (org.apache.kafka.clients.NetworkClient)
java.net.UnknownHostException: kafka
	at java.base/java.net.InetAddress$CachedLookup.get(Unknown Source)
	at java.base/java.net.InetAddress.getAllByName0(Unknown Source)
	at java.base/java.net.InetAddress.getAllByName(Unknown Source)
	at org.apache.kafka.clients.DefaultHostResolver.resolve(DefaultHostResolver.java:27)
	at org.apache.kafka.clients.ClientUtils.resolve(ClientUtils.java:125)
	at org.apache.kafka.clients.ClusterConnectionStates$NodeConnectionState.resolveAddresses(ClusterConnectionStates.java:536)
	at org.apache.kafka.clients.ClusterConnectionStates$NodeConnectionState.currentAddress(ClusterConnectionStates.java:511)
	at org.apache.kafka.clients.ClusterConnectionStates.currentAddress(ClusterConnectionStates.java:173)
	at org.apache.kafka.clients.NetworkClient.initiateConnect(NetworkClient.java:1140)
	at org.apache.kafka.clients.NetworkClient.ready(NetworkClient.java:368)
	at org.apache.kafka.clients.admin.KafkaAdminClient$AdminClientRunnable.sendEligibleCalls(KafkaAdminClient.java:1268)
	at org.apache.kafka.clients.admin.KafkaAdminClient$AdminClientRunnable.processRequests(KafkaAdminClient.java:1529)
	at org.apache.kafka.clients.admin.KafkaAdminClient$AdminClientRunnable.run(KafkaAdminClient.java:1472)
	at java.base/java.lang.Thread.run(Unknown Source)
Error while executing topic command : Timed out waiting for a node assignment. Call: listTopics
[2026-09-07 12:11:09,628] ERROR org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment. Call: listTopics
 (org.apache.kafka.tools.TopicCommand)


```

2) Ошибка в логах:

```
[2026-09-07 12:16:26,325] WARN [AdminClient clientId=adminclient-1] Connection to node 1 (localhost/127.0.0.1:19092) could not be established. Node may not be available. (org.apache.kafka.clients.NetworkClient)
```

**Три исхода попытки подключиться**

Когда клиент пытается открыть TCP-соединение, он проходит две ступени: сначала превратить имя в IP, потом установить
сеанс. Сломаться может на любой, и симптомы разные:

| Что видно                                     | 	На какой ступени сломалось	 | Что это значит физически                          |
|-----------------------------------------------|------------------------------|---------------------------------------------------|
| UnknownHostException, Couldn't resolve server | 	DNS	                        | пакет вообще не покинул машину                    |
| Connection refused                            | TCP, ответ получен           | до хоста дошли, но на этом порту никто не слушает |
| тишина, потом таймаут                         | TCP, ответа нет              | SYN ушёл, ответа не пришло                        |

**Что происходит в каждой поломке:**
**Поломка 1** — клиент получил адрес kafka:9092. Хост пытается резолвить имя kafka. В /etc/hosts его нет, у DNS-сервера
тоже. Резолв проваливается, TCP-соединение даже не начинается. Клиент Kafka не сдаётся: он ретраит каждые
reconnect.backoff.ms, и так до истечения default.api.timeout.ms (по умолчанию 60 секунд), после чего бросает
TimeoutException.

Отсюда ощущение «повисло»: 60 секунд ничего не происходит, в логах повторяющиеся WARN про неразрешимое имя.

**Поломка 2** — клиент получил localhost:19092. Имя резолвится мгновенно в 127.0.0.1. Клиент шлёт TCP SYN на порт 19092.
Ядро видит, что этот порт никто не слушает, и немедленно отвечает пакетом RST. Это и есть Connection refused — быстрый и
однозначный ответ.

Дальше клиент так же ретраит и так же упрётся в общий таймаут, но каждая отдельная попытка проваливается за миллисекунды
с явным сообщением.

Почему «**отказано**» — это хорошая новость

``Connection refused`` означает, что вы дошли до нужной машины. Сетевой путь есть, фаервол пропустил, просто на порту
пусто.
Круг поиска сузился до одного: не тот порт или процесс не запущен.

Молчание с таймаутом — хуже. Вариантов много: фаервол с политикой DROP (он не отвечает RST, а просто выбрасывает пакет),
неправильный IP, машина недоступна, потери в сети. Здесь нужна дополнительная диагностика.

Именно поэтому фаерволы в защищённых периметрах настраивают на DROP, а не REJECT: сканеру портов не достаётся даже
подтверждения, что хост существует.