Проектная работа "Анализ химического состава лекарственных средств"

Алгоритм обработки данных в Google Cloud Shell

1. Переходим в Cloud-Shell и прописываем команды:
gcloud projects list
gcloud config set project chemical-311015
export REGION=europe-west3
export ZONE=europe-west3-a
export PROJECT=$(gcloud info --format='value(config.project)')
export BUCKET_NAME=${PROJECT}-chems
gsutil mb -l ${REGION} gs://${BUCKET_NAME}

2. Загружаем справочник ingredients в GCP Bucket.

3. Создание инстанса:
gcloud sql instances create hive-metastore-mysql-chems \
    --database-version="MYSQL_5_7" \
    --activation-policy=ALWAYS \
    --zone $ZONE

4. Создание кластера dataproc
gcloud config set compute/zone $ZONE
gcloud dataproc clusters create hive-cluster-2 \
	--region=$REGION \
    --scopes cloud-platform \
    --image-version 1.3 \
    --bucket=$BUCKET_NAME \
	--master-machine-type=n1-standard-1 \
	--num-workers=2 \
	--worker-machine-type=n1-standard-1 \
	--optional-components=PRESTO \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
    --properties hive:hive.metastore.chems.dir=gs://${PROJECT}-chems/datasets \
    --metadata "hive-metastore-instance=${PROJECT}:${REGION}:hive-metastore-mysql-chems"

5. Создадим таблицу справочника и сохраним содержимое в parquet:
gsutil ls -lr gs://${BUCKET_NAME}/datasets/
gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --execute "
      CREATE EXTERNAL TABLE ingredients
      (RXCUI STRING, INGREDIENT STRING, ING_RXCUI STRING)
      STORED AS PARQUET
      LOCATION 'gs://${PROJECT}-chems/datasets/ingredients';" \
      --region $REGION

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        CREATE EXTERNAL TABLE ingredients_csv(
          RXCUI STRING, 
          INGREDIENT STRING, 
          ING_RXCUI STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        location 'gs://${BUCKET_NAME}/datasets/ingr';"

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        INSERT OVERWRITE TABLE ingredients
        SELECT * FROM  ingredients_csv;"

Проверка:
gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "SELECT COUNT(*) FROM ingredients;"


5.1 Все лекарства.

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --execute "
      CREATE EXTERNAL TABLE ids_pq
      (RXCUI STRING)
      STORED AS PARQUET
      LOCATION 'gs://${PROJECT}-chems/datasets/all/parquet';" \
      --region $REGION


gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        CREATE EXTERNAL TABLE ids_csv(
          RXCUI STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        location 'gs://${BUCKET_NAME}/datasets/all';"

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        INSERT OVERWRITE TABLE ids_pq
        SELECT * FROM  ids_csv;"
Проверка:
gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "SELECT COUNT(*) FROM ids_pq;"

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "SELECT * FROM ids_pq limit 10;"	

6. Выгрузим актуальные данные из Bigquery за последний месяц и сохраним в parquet.

bq --location=us extract \
--destination_format PARQUET \
--print_header=false \
bigquery-public-data:nlm_rxnorm.rxncuichanges_current \
gs://${BUCKET_NAME}/rxncuichanges_current/parquet/currents.parquet


gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        CREATE EXTERNAL TABLE rxncuichanges_pq(
          rxaui   STRING,
          code  STRING,
          sab  STRING,
          tty  STRING,
          str  STRING,
          old_rxcui  STRING,
          new_rxcui  STRING)
        STORED AS PARQUET
        location 'gs://${BUCKET_NAME}/rxncuichanges_current/parquet/';"


Проверка:
gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "SELECT COUNT(*) FROM rxncuichanges_pq;"

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "SELECT rxaui FROM rxncuichanges_pq limit 10;"


7. Преобразование,соединение и запросы.

Создаем итоговую таблицу для итогов.

gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        CREATE EXTERNAL TABLE final_pq(
          RXCUI   STRING,
          INGREDIENT  STRING)
        STORED AS PARQUET
        location 'gs://${BUCKET_NAME}/final/parquet/';"


gcloud dataproc jobs submit hive \
    --cluster hive-cluster-2 \
    --region=${REGION} \
    --execute "
        INSERT OVERWRITE TABLE final_pq(
        SELECT ids_pq.rxcui as rxcui,INGREDIENT FROM ids_pq 
        left join (SELECT rxcui,ingredient FROM ingredients)ings on
        ings.rxcui=ids_pq.rxcui)"



gcloud compute ssh hive-cluster-2-m
presto --catalog hive --schema default

Проверочные запросы:
select distinct * from final_pq limit 10;
select count(rxcui) as count_med, ingredient from final_pq group by ingredient order by count_med desc limit 10;
select distinct str from rxncuichanges_pq limit 10;

8. Выгружаем результаты в BigQuery.

9. Создаем Goоgle-Sheet с доп. информацией.
Перевод наименований медикаментов, их применение, негативное воздействие на организм.

10. Совмещаем результаты между собой и дополнительной информацией в отчете Tableau.
Визуализируем:
- данные по самым популярным ингредиентам и их расшифровке,
- данные по применению ингредиенов в выпуске новых медикаментов.

Анализируем, строим гипотезы, делвем выводы, принимаем решения.

Ссылка на отчет: https://tb.pik.ru/#/site/pik-project/workbooks/2772/views
Ссылка на схему: https://app.diagrams.net/#G1-URwCUKZiGSbwf6CA30s7wL2GRv2pqgQ
