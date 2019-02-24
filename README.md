# Apache

Introducción a herramientas y tecnologías de Big Data


## Instalación y configuración de Hadoop en modo local (*standalone*)

Esta guía describe la instalación y configuración  de Apache Hadoop en modo standalone. 

### Paso 1

Descargue Java SE Development Kit (JDE) 8u181 (jdk-8u181-linux-x64.tar.gz).

http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

### Paso 2

Descargue el archivo `jdk-8u181-linux-x64.tar.gz`

### Paso 3

Ubiquelo en el directorio `~/home/USER` y descomprima el archivo 

    mv jdk1.8.0_181 ~/jdk1.8.0_181
    tar -xvf jdk-8u181-linux-x64.tar

### Paso 4

Descargue Apache Hadoop 3.1.1 de https://hadoop.apache.org/releases.html con `wget`.

### Paso 5

Descomprima el archivo desde Terminal (ubicado en el folder `Downloads`):

    tar -xvf hadoop-3.1.1.tar
    
    
### Paso 6

Mueva el archivo a su directorio `home` y renombrelo.

    mv hadoop-3.1.1 ~/hadoop


### Paso 7

Edite su archivo `.bashrc` usando un editor de textos. Puede hacer esto desde
la línea de comandos con:

    code ~/.bashrc
    
Agregue los siguientes comandos (reemplace `USER` por su nombre de usuario):

    export JAVA_HOME="/home/USER/jdk1.8.0_181"
    export PATH="$JAVA_HOME/bin:$PATH"

    export HADOOP_HOME="/home/user_name/hadoop"
    export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"
    export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
    export HADOOP_ROOT_LOGGER="ERROR,console"
    export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
    export PATH="$HADOOP_HOME:$HADOOP_HOME/bin:$PATH"


### Paso 8

Verifique la instalación. En Terminal digite 

    source ~/.bashrc

para hacer efectivos los cambios. Luego digite `hadoop` en la línea de comandos. 
Como resultado, debe imprimirse la ayuda de hadoop en la pantalla.


**Nota.--** En este paso, Apache Hadoop se puede ejecutar en modo *standalone*.

**Nota.--** Hadoop usará el sistema local de archivos como sistema HDFS.
