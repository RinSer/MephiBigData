#!/bin/bash

#####################################
# Установочный скрипт Elasticsearch #
# с Kibana и logstash с помощью     #
# пакета Debian для Ubuntu 20.04    #
#####################################

# Устанавливаем PGP ключи
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -

# Устанавливаем пакет apt-transport-https
sudo apt-get install apt-transport-https -y

# Сохраняем определение репозитория в файл /etc/apt/sources.list.d/elastic-7.x.list
echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-7.x.list

# Устанавливаем пакеты Elasticsearch, Kibana и logstash
sudo apt-get update && sudo apt-get install elasticsearch kibana logstash -y