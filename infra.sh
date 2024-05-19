
#!/bin/bash

# Прекращение выполнения скрипта при любой ошибке
set -e

# Поднять сервисы с помощью Docker Compose
echo "Поднимаем сервисы Docker Compose..."
docker-compose up -d

# Запуск файла migration.py
echo "Запуск файла migration.py..."
~/anaconda3/bin/python migration.py

echo "Все шаги выполнены успешно!"
