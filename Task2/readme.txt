Как компилировать файл:
Открыть файл mm.java из каталога prog/src/main/java в среде Intelliji, запустить в меню Maven процедуры clean и package. 
Как запустить программу:
Оставаясь в среде после компиляции открыть снизу вкладку терминал, прописать и запустить следующие команды:
export HADOOP_HOME=~/hadoop-2.7.1
export JAVA_HOME=~/jdk1.7.0_80
$HADOOP_HOME/bin/hadoop jar target/mm.jar mm -conf config.xml <Директория матрицы A> <Директория матрицы B> <Директория матрицы C>
