1. Вынесите папки airlow и backend из папки Airflow в корень репозитория
2. Замените docker-compose.yaml и ReportPage.tsx на файлы из папки airlow
3. В корне папки docker compose up -d
4. Airflow (ETL)
  4.1 UI: http://localhost:8081
  4.2 Посмотрите лог контейнера Airflow и найдите строку с паролем (обычно вида Password for user 'admin'): docker compose logs airflow
    4.2.1 Если в логе ничего нет, введите случайный логин-пароль 3 раза
  4.3 Найдите DAG crm_telemetry_reporting, включите и запустите.
  4.4 Убедитесь, что все задачи завершились успешно.
5. Проверить UI
  5.1 Откройте http://localhost:3000
  5.2 Войдите через Keycloak
  5.3 Выберите даты и нажмите Download Report — таблица должна заполниться.
