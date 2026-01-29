1. Вынесите папки airlow и backend из папки Airflow в корень репозитория
2. Замените docker-compose.yaml и ReportPage.tsx на файлы из папки airlow
3. В корне папки docker compose up -d
4. Airflow (ETL)
  4.1 UI: http://localhost:8081
  4.2 Найдите DAG crm_telemetry_reporting, включите и запустите.
  4.3 Убедитесь, что все задачи завершились успешно.
5. Проверить, что витрина заполнена в ClickHouse
  5.1 clickhouse-client --host localhost --query "SELECT count(*) FROM reports.report_mart"
  Ожидается число > 0.
6. Проверить API /reports
  6.1 Получите токен из Keycloak (realm reports-realm, client reports-api).
  6.2 Запрос:
    curl -H "Authorization: Bearer <TOKEN>" \
         "http://localhost:8000/reports?start=2024-01-01&end=2024-12-31"
    Ожидается JSON с user_id, start, end, report.

  6.3 Проверки доступа:
    Без токена: должен быть 401.
    С токеном другого пользователя: данные будут только по sub токена (фильтрация по user_id).

7. Проверить UI
  7.1 Откройте http://localhost:3000
  7.2 Войдите через Keycloak
  7.3 Выберите даты и нажмите Download Report — таблица должна заполниться.
