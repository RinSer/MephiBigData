#!/bin/bash

#######################################
# Скрипт для запуска Elasticsearch    #
# с Kibana и logstash на Ubuntu 20.04 #
#######################################

# Запускаем Elasticsearch
sudo service elasticsearch start

# Запускаем Kibana с конфигурационным файлом из текущей директории
sudo /usr/share/kibana/bin/kibana -c kibana.yml --allow-root &

# Запускаем logstash с конфигурационным файлом из текущей директории
sudo /usr/share/logstash/bin/logstash -f elasticsearch_process_stats.conf