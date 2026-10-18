# Кэширование каталога товаров с Redis

Практическое задание по cache-aside, времени жизни кэша, инвалидации и работе при отказах. Вы получите готовый API каталога и реализуете слой кэширования. В дополнительных этапах добавите защиту от одновременных cache miss и stale-while-revalidate.

**Плановая трудоёмкость: 10–12 часов, включая дополнительные этапы и сдачу.** Базовая часть короче. Это оценка для студента, знакомого с Python, HTTP и `async/await`.

**Репозиторий:** [rbeketov/redis-cache-lab](https://github.com/rbeketov/redis-cache-lab). **Сдача:** Pull Request с кодом, отчётом и ссылкой на видео до четырёх минут.

## Контекст и результат

Каталог часто получает повторные запросы на одни и те же товары. Источник данных отвечает с задержкой 100 мс. Redis должен уменьшить число чтений источника, сохраняя оговорённую актуальность данных. Этот подход применяют в каталогах, справочниках и агрегирующих API.

После задания вы сможете объяснить путь cache hit / cache miss, настроить TTL, обновить данные без возврата прежнего значения после последовательного PATCH, обработать отказ Redis и проверить поведение при конкурентных запросах. Для SWR вы отдельно сформулируете допустимый период устаревания.

В задании один процесс API и один event loop. Источник данных хранится в памяти процесса, поэтому перезапуск API возвращает исходные товары. У Redis новый namespace после каждого запуска API.ы Распределённые блокировки между несколькими серверами в задание не входят.

## Состав проекта

| Файл | Назначение |
|---|---|
| `app/service.py` | Основное место вашей реализации |
| `app/source.py` | Готовый источник с задержкой и счётчиками |
| `app/main.py` | Готовый HTTP API и создание Redis-клиента |
| `app/models.py` | Товары, валидация PATCH, номер версии |
| `tests/` | Открытые проверки по уровням |
| [docs/ASSIGNMENT.md](docs/ASSIGNMENT.md) | Этапы и точный контракт |
| [docs/GRADING.md](docs/GRADING.md) | Предлагаемая шкала оценки |
| [docs/SUBMISSION.md](docs/SUBMISSION.md) | Куда загрузить код и видео, как открыть Pull Request |
| [REPORT.md](REPORT.md) | Шаблон вашего отчёта |

## Порядок выполнения

| Этап | Что сделать | Ориентир времени |
|---|---|---:|
| 0 | Создать fork, запустить окружение и smoke-тесты | 0,5 ч |
| 1 | Реализовать cache-aside, TTL и обработку повреждённых записей | 2 ч |
| 2 | Добавить инвалидацию при изменении товара | 1 ч |
| 3 | Обработать отказ Redis и восстановление без старой цены | 1,5 ч |
| 4 | Измерить обращения к источнику, заполнить отчёт | 1 ч |
| 5* | Объединить одновременные загрузки одного ключа, уровень П | 1,5 ч |
| 6* | Добавить stale-while-revalidate, уровень Э | 2 ч |
| 7 | Проверить результат, записать видео и открыть PR | 1 ч |

Всего до Э: 10,5 часа плюс резерв до 12. Обязательная часть со сдачей: около 7 часов. Подробные условия и ожидаемые результаты каждого этапа находятся в [ASSIGNMENT.md](docs/ASSIGNMENT.md), требования к оценкам — в [GRADING.md](docs/GRADING.md).

## Получение своей копии

1. Войдите в GitHub и [создайте fork](https://github.com/rbeketov/redis-cache-lab/fork) в своём аккаунте. Имя оставьте `redis-cache-lab`.
2. Откройте свою копию, нажмите **Code → HTTPS** и скопируйте адрес клонирования. Можно использовать SSH, если он уже настроен.
3. В bash/zsh/WSL выполните команды ниже, заменив `your-github-login` своим GitHub-логином:

```sh
GITHUB_USER=your-github-login
git clone "https://github.com/$GITHUB_USER/redis-cache-lab.git"
cd redis-cache-lab
git remote add upstream https://github.com/rbeketov/redis-cache-lab.git
git switch -c solution/redis-cache
```

`origin` указывает на вашу копию, `upstream` — на задание автора. Проверьте это командой `git remote -v`. Работа ведётся в ветке `solution/redis-cache`. Если вы назвали fork иначе, используйте адрес из **Code**.

Если GitHub не позволяет сделать fork из-за настроек доступа, сообщите автору через [Issues](https://github.com/rbeketov/redis-cache-lab/issues). Права записи в основной репозиторий для обычной сдачи не нужны.

## Необходимые знания и окружение

Нужны установленный Git 2.23+, аккаунт GitHub, базовый Python, функции и классы, JSON, HTTP GET/PATCH, исключения и `async/await`. Опыт Redis не предполагается. При необходимости до начала повторите `asyncio.Lock`, `asyncio.create_task` и `asyncio.gather`.

Рекомендуемое окружение: Linux, macOS или Windows с WSL2, работающий Docker Engine 24+ / Docker Desktop и Compose 2.20+. На компьютере желательно иметь 2 ядра, 4 ГБ доступной памяти и 3 ГБ свободного места. Первый запуск требует доступа к Docker Hub и PyPI. Это ориентиры для лабораторной, не результаты нагрузочного теста.

В контейнерах: Python 3.12.10, Redis 7.4.2, FastAPI 0.115.12, redis-py 5.2.1, pytest 8.3.5. Полные версии Python-зависимостей зафиксированы в `requirements.lock`. Python и Redis отдельно на хост устанавливать не нужно. Redis не публикует порт на хост, API доступен только через `127.0.0.1`.

## Быстрый запуск

Откройте терминал в корне выданного студентам репозитория, где лежит `compose.yaml`.

```sh
docker version
docker compose version
docker compose up --build -d api
docker compose ps
curl http://localhost:8000/health
```

Ожидается `{"status":"ok","mode":"cache_aside"}`. Документация API: [http://localhost:8000/docs](http://localhost:8000/docs).

```sh
curl http://localhost:8000/products/1
curl http://localhost:8000/products/1
curl http://localhost:8000/debug/stats
```

В исходной заготовке `source_reads` растёт при каждом GET. После реализации два последовательных запроса одного товара внутри TTL должны давать одно чтение источника. Считайте разницу счётчиков, если до этого уже обращались к API.

```sh
curl -X PATCH http://localhost:8000/products/1 \
  -H 'Content-Type: application/json' \
  -d '{"price_kopecks":55000,"stock":7}'
curl http://localhost:8000/products/1
```

Ожидаются `price_kopecks: 55000`, `stock: 7` и `revision: 2`. Цена задана целым числом копеек. Товаров два: ID 1 и 2. Неизвестный ID даёт 404, некорректный PATCH даёт 422.

Изменения кода подключены через bind mount. После редактирования перезапустите API:

```sh
docker compose restart api
```

## Проверки по этапам

```sh
docker compose run --rm tests python -m pytest -q -m smoke
docker compose run --rm tests python -m pytest -q -m basic
docker compose run --rm tests python -m pytest -q -m advanced
docker compose run --rm tests python -m pytest -q -m expert
docker compose run --rm tests
```

На старте smoke-тесты должны проходить. Часть остальных проверок намеренно падает, потому что `service.py` пока обращается напрямую к источнику. Тесты используют настоящий Redis и собственные уникальные префиксы, удаляют только свои ключи и не изменяют товары работающего API.

Некоторые отдельные проверки могут проходить уже в заготовке. Уровень считается достигнутым, когда проходят все его проверки и выполнены требования к объяснению результата. Проверки не надо отключать, переписывать или подменять постоянными ответами.

## Настройки

| Переменная | По умолчанию | Смысл |
|---|---:|---|
| `CACHE_MODE` | `cache_aside` | `cache_aside` либо `swr` для этапа Э |
| `FRESH_TTL` | 5 | Секунды свежести |
| `STALE_TTL` | 10 | Дополнительное окно устаревания, только SWR |
| `SOURCE_DELAY` | 0.1 | Секунды задержки источника |
| `API_PORT` | 8000 | Порт API на хосте |

Для режима SWR после его реализации:

```sh
CACHE_MODE=swr docker compose up -d --force-recreate api
```

Команды с присваиванием переменных рассчитаны на bash/zsh/WSL. В PowerShell можно задать `$env:CACHE_MODE="swr"` перед командой. При смене переменных нужен `up --force-recreate`, одного `restart` недостаточно.

## Сбой и восстановление Redis

```sh
docker compose stop redis
curl http://localhost:8000/products/1
curl -X PATCH http://localhost:8000/products/1 \
  -H 'Content-Type: application/json' -d '{"price_kopecks":61000}'
docker compose start redis
curl http://localhost:8000/products/1
```

После реализации запросы продолжают работать. Последний GET возвращает цену 61000. Redis-клиент уже имеет таймауты подключения и команд по 0.2 секунды, автоматические повторы отключены. Производительность при сбое может снизиться: все запросы обращаются к источнику. Измерять время ответа до миллисекунды для оценки не нужно.

## Остановка и типовые проблемы

```sh
docker compose down
```

Команда останавливает и удаляет контейнеры этого учебного проекта. Постоянных томов здесь нет, демонстрационные изменения товаров исчезнут.

Если порт 8000 занят: `API_PORT=8010 docker compose up -d api`, затем используйте порт 8010. Если Docker не отвечает, запустите Docker Desktop. Если API не поднялся, выполните `docker compose logs api`. Если изменились зависимости, пересоберите контейнеры. Если тесты не находят Redis, запускайте их через Compose из этой инструкции, а не из случайного Python-окружения.

## Что сдавать

**Код обязателен:** загрузите реализацию, собственные тесты и заполненный `REPORT.md` в ветку своего fork. Видео до четырёх минут разместите на Яндекс.Диске, Google Drive или видеохостинге с доступом по ссылке. Сам видеофайл в Git не добавляйте.

Откройте **Pull Request из своего fork в `rbeketov/redis-cache-lab`, ветку `main`**. Шаблон [pull_request_template.md](.github/pull_request_template.md) подставится автоматически: добавьте ссылку на ролик, его длительность, заявленный уровень, результат тестов и команды проверки. Ссылка на открытый PR является ссылкой на вашу сдачу.

Все шаги с командами Git, настройками доступа к ролику и действиями после замечаний: [docs/SUBMISSION.md](docs/SUBMISSION.md). Ошибки заготовки и вопросы по постановке можно сообщить через [Issues](https://github.com/rbeketov/redis-cache-lab/issues/new).

Точную дату сдачи сообщает автор после согласования с преподавателем. Плановый срок выполнения по записям занятия: около 14–16 календарных дней; это не оценка трудоёмкости в часах.

## Материалы

- [Redis GET](https://redis.io/docs/latest/commands/get/), [SET и TTL](https://redis.io/docs/latest/commands/set/), [DEL](https://redis.io/docs/latest/commands/del/).
- [redis-py asyncio](https://redis.readthedocs.io/en/stable/examples/asyncio_examples.html).
- [Python asyncio locks](https://docs.python.org/3.12/library/asyncio-sync.html#lock).
- [Python asyncio tasks](https://docs.python.org/3.12/library/asyncio-task.html).
- [Cache-Aside pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/cache-aside).
