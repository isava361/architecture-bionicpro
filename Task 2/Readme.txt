1. Замените основной код на код из zip файла
2. В корне папки docker compose up -d
3. Airflow (ETL)
  3.1 UI: http://localhost:8081
  3.2 Посмотрите лог контейнера Airflow и найдите строку с паролем (обычно вида Password for user 'admin'): docker compose logs airflow
    3.2.1 Если в логе ничего нет, введите случайный логин-пароль 3 раза
  3.3 Найдите DAG crm_telemetry_reporting, включите и запустите.
  3.4 Убедитесь, что все задачи завершились успешно.
4. Проверить UI
  4.1 Откройте http://localhost:3000
  4.2 Войдите через Keycloak с помощью например user1 - password123
  4.3 Выберите даты и нажмите Download Report — отчет должен загрузиться.
