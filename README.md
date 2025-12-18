# Airflow Homework

Этот репозиторий содержит DAG для загрузки данных и аналитики клиентов.  

## Содержание

- `dags/` — DAG файлы и SQL скрипты
- `docker-compose.yaml` — конфигурация Docker для Airflow и Postgres
- `data/` — CSV файлы с исходными и выгруженными данными
- `README.md` — этот файл

## Скриншоты работы DAG

![alt text](<screenshots/DAG in UI.png>)

*DAG загружен и виден в Airflow UI.*

![alt text](<screenshots/executed succesfully.png>)

*Все задачи выполнены успешно*

![alt text](<screenshots/table created.png>) 

*Таблицы созданы успешно.*

![alt text](<screenshots/data loaded.png>)

*Данные успешно загружены*

![alt text](<screenshots/check succeded.png>)

*Проверка, что запрос возвращает больше 0 строк прошла успешно*

![alt text](<screenshots/export succeded.png>)

*Выгрузка прошла успешно*

![alt text](<screenshots/exported top clients.png>)

*Выгруженные данные клиентов*


![alt text](<screenshots/exported wealth segment.png>)

*Выгруженные данные клиентов по wealth segment*



## Запуск

1. Запустить `docker-compose up` в папке проекта.
2. Открыть Airflow UI: `http://localhost:8080`
3. DAG `Kruchkova_Yulia_N_hw_06` будет доступен.
