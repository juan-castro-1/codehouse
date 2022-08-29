Proyecto Final CoderHouse
Alumno: Juan Castro

En el siguiente trabajo se presenta un script con un ETL que carga data de un archivo csv, realiza varias transformaciones y luego sube diferentes tablas a un bucket de AWS en formato parquet y se crean tablas en una postgresql. La data proviene de un archivo csv que contiene datos de jugadores del FIFA 2022. Se realizan dos transformaciones, primero se filtran columnas y valores nulos. Luego se crean varias metricas nuevas con valores para poder ser analizados. Por último se crean cuatro tablas con todas las metricas agregadas a nivel país, club, edad y posición. Finalizando se cargan las diferentes tablas a un Bucket S3 en AWS en formato parquet y en una base de datos postgresql local.
