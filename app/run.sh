#!/bin/bash


printf "\n\n\n\n\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\033[1;32m| \033[1;97mНачинается установка зависимостей... \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;30mpip install -r requirements.txt \033[0m\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\n\n\n\n"

sleep 3s

pip install -r requirements.txt --upgrade pip





printf "\n\n\n\n\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\033[1;32m| 1 из 3 - Установлены зависимости \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;97mПереход к трансформации датасета... \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;30mpython ingest.py \033[0m\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\n\n\n\n"

python ingest.py




printf "\n\n\n\n\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\033[1;32m| 2 из 3 - Данные сформированы \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;97mПереход к построению прогноза... \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;30mpython forecast.py \033[0m\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\n\n\n\n"

python forecast.py




printf "\n\n\n\n\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\033[1;32m| 3 из 3 - Построен прогноз \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;97mМожно проверять результат выполнения в папке step3__forecast \033[0m\n"
printf "\033[1;32m| \033[0m\n"
printf "\033[1;32m| \033[1;97m:) \033[0m\n"
printf "\033[1;32m|----------------------- \033[0m\n"
printf "\n\n\n\n"


chmod 777 -R *