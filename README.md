# Введение


 - REST API для CRUD операций с курсами валют 
 - WebSocket для real-time обновлений 
 - Фоновая задача с периодическим обновлением данных 
 - Event-driven публикация через NATS 
 - Swagger документация 
 - Docker Compose для запуска

# Клонирование и запуск
```bash
git clone https://github.com/karabike/CourseValute.git
cd CourseValute
docker-compose up --build
```
# Доступные сервисы

*API Documentation: http://localhost:8000/docs*

*WebSocket: ws://localhost:8000/ws/currency*

*NATS Monitoring: http://localhost:8222*


# API Endpoints

```GET /api/v1/currency/rates — список всех курсов```

```GET /api/v1/currency/rates/{id} — получить курс по ID```

```POST /api/v1/currency/rates — создать новый курс```

```PATCH /api/v1/currency/rates/{id} — обновить курс```

```DELETE /api/v1/currency/rates/{id} — удалить курс```

```POST /api/v1/tasks/run — запустить фоновую задачу вручную```

```GET /api/v1/tasks/status — статус фоновых задач```

```GET /api/v1/currency/task-logs — логи выполнения задач```

```ws://localhost:8000/ws/currency — WebSocket```
