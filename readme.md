## Приветствую!

### Небольшое описание
Решил написать с помощью Airflow. Непосредственно написанный DAG находится в директории **src/dags**. Помимо него здесь лежат Dockerfile для Airflow и Docker compose с одной нодой кликхауса и постгрес, Airflow.

### Немного про задачу
Не совсем понял из задания нужно ли чтобы данные были нормализованы или нет.
Оставил нормализованный вариант (_create_normalized_in_ch в даге), так как тип JSON
является экспериментальным в кликхаус. Наверное, если бы писал через постгрес, положил бы в этот тип,
раз мы придерживаемся ELT, а не ETL. 

### Про кликхаус и опять про реализацию
В целом, выбрал кликхаус, так как ранее не писал Даги с ним, плюс, он хорошо подходит для чтения,
которое потом будет на Transform этапе.
Функционал конкретного дага можно расширить, например, проверяя в начале есть ли файл с сегодняшней датой,
также можно удалять временные файлы за собой. Я предпочитаю оставлять, чтобы потом легко выяснить в чем было дело
при неудачном запуске.
На этом все. Надеюсь реализация понравилась и я не утомил сильно :)