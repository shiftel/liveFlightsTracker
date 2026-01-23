# Ustawienia zmiennych dla czytelności
$CONTAINER_NAME = "spark-master"
$SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
$MASTER_URL = "spark://spark-master:7077"
$PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2,org.postgresql:postgresql:42.7.2"
$APP_PATH = "/opt/spark/apps/spark_apps/transform_flights.py"

# Poprawka na błąd /nonexistent/.ivy2
$IVY_FIX = "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2"
$IVY_FIX_EXEC = "spark.executor.extraJavaOptions=-Divy.home=/tmp/.ivy2"

Write-Host "--- Inicjalizacja Spark Streaming Job ---" -ForegroundColor Cyan
Write-Host "--- Ustalono lokalizację Ivy na /tmp/.ivy2 ---" -ForegroundColor Yellow

docker exec -it $CONTAINER_NAME $SPARK_SUBMIT `
  --conf "$IVY_FIX" `
  --conf "$IVY_FIX_EXEC" `
  --master $MASTER_URL `
  --packages $PACKAGES `
  $APP_PATH