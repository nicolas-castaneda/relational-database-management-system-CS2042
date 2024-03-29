# RDBMS-BD2
Relational database managment system, made in c++ for Database 2

## Tabla de Contenidos

- [RDBMS-BD2](#rdbms-bd2)
  - [Tabla de Contenidos](#tabla-de-contenidos)
  - [Objetivos](#objetivos)
    - [Principal](#principal)
    - [Secundarios](#secundarios)
  - [Organización de archivos](#organización-de-archivos)
  - [Funciones implementadas](#funciones-implementadas)
  - [Dataset](#dataset)
    - [Descripción de las Columnas](#descripción-de-las-columnas)
  - [SQLParser](#sqlparser)
    - [Queries](#queries)
    - [Non terminals](#non-terminals)
  - [Experimentación](#experimentación)
  - [Conclusiones](#conclusiones)
  - [Autores](#autores)
  - [Bibliografía](#bibliografía)

## Instalación

### Ubuntu

* CMake (3.27)
* YACC
* Bison

#### Instalar g++13

```
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt update
sudo apt install gcc-13 g++-13 -y
```

#### Instalar rapidjson

```
git clone https://github.com/Tencent/rapidjson.git
cmake .
sudo make install
```

#### Instalar spdlog

```
git clone https://github.com/gabime/spdlog.git
cmake .
sudo make install
```

#### Instalar Crow

```
git clone https://github.com/CrowCpp/Crow.git
cmake .
sudo make install
```

#### Instalar proyecto

```
./run.sh g++-13
cmake --build build
```

## Objetivos

### Principal

- Desarrollar un sistema de gestión de bases de datos relacionales que emplea índices basados en estructuras de datos reconocidas, almacenados en archivos, con el objetivo de mejorar el rendimiento de las consultas y comprender su comportamiento en función de diversas cantidades de datos.

### Secundarios

- Armar un parser para el lenguaje SQL que permita realizar operaciones de carga de archivos, creación de tablas , creación de índices y consulta de datos.

- Comparación de estructuras de datos: Realizar un análisis comparativo sobre las estructuras de datos utilizadas (AVL, ISAM, Sequential File) en términos de eficiencia, consumo de memoria y escalabilidad.
  
- Manejo de Memoria Secundaria: Explorar estrategias avanzadas para el manejo eficiente de la memoria secundaria, como la implementación de técnicas de paginación o buffering para minimizar los accesos a disco.

## Organización de archivos

Estrategias utilizadas en el presente proyecto:

* [Sequential Index](https://github.com/nicolas-castaneda/sequential-index)
* [AVL Index](https://github.com/AaronCS25/avl-index-zzz)
* [ISAM Index]()

## Funciones implementadas

Las estructuras de datos implementadas deben poder realizar las siguientes operaciones:

* Response add (T key);
* Response erase (T key);
* Response search(T key);
* Response searchByRange(T key);

**Nota:** Por defecto, todas las operaciones sobre la tabla se ejecutan sobre el índice Sequential, que se crea automáticamente para todos los índices. Esto significa que las operaciones sobre la tabla utilizarán el índice Sequential a menos que se especifique lo contrario.

### Indexed Sequential Access Method

Los métodos de insert, remove y search del ISAM muestran a continuación: 

#### Insert
<div style="text-align:center"><img src="./imgs/isam_insert.png" /></div>

#### Remove
<div style="text-align:center"><img src="./imgs/isam_remove.png" /></div>

#### Search
<div style="text-align:center"><img src="./imgs/isam_search.png" /></div>

### Ilustración general de la estructura del ISAM
<div style="text-align:center"><img src="./imgs/Isam4.drawio.png" /></div>

### Ilustración del *Split* en el *Insert*
<div style="text-align:center"><img src="./imgs/InserISAM.drawio.png" /></div>

### Ilustración del *Merge* en el *Remove*
<div style="text-align:center"><img src="./imgs/deleteISAM.drawio.png" /></div>


## Dataset

El conjunto de datos utilizado en este proyecto proviene de la plataforma Kaggle y consiste en pistas de Spotify que abarcan diferentes géneros musicales. Este dataset es una **buena** elección por las siguientes razónes:

- **Variedad de Datos:** El dataset contiene una amplia gama de tipos de datos relacionados con la música, como detalles de las pistas, información de los artistas y atributos musicales. Esto proporciona una riqueza de información que permite probar la DBMS y su manejo de tablas con tipos de datos variados (*varchar, int, double, text, bool, etc*).

- **Calidad de los Datos:** Los datos de Spotify son de alta calidad y reflejan una representación real de la información que se encuentra en la plataforma de streaming de música. Esto hace que el dataset sea adecuado para simular una evaluación del rendimiento de las operaciones sobre datos reales en un contexto real.

- **Gran Volumen de Datos:** Con más de 114,000 observaciones, este dataset es lo suficientemente grande como para probar y evaluar el rendimiento de las operaciones de bases de datos con grandes cantidades de datos. Esto es fundamental para comprender cómo las estructuras de datos utilizadas en el proyecto se comportan en escenarios de poca, media y alta carga.

### Descripción de las Columnas

| Columna          | Descripción |
|------------------|---------------------------------------------------------------------------------------------------|
| track_id         | ID de Spotify para la pista.|
| artists          | Nombres de los artistas que interpretaron la pista (separados por ; si hay más de uno).|
| album_name       | Nombre del álbum en el que aparece la pista.|
| track_name       | Nombre de la pista.|
| popularity       | Popularidad de la pista (valor entre 0 y 100, calculado en función de reproducciones y precencia). |
| duration_ms      | Duración de la pista en milisegundos.|
| explicit         | Indica si la pista tiene letras explícitas (true = sí tiene; false = no tiene O desconocido).|
| danceability     | Adecuación de la pista para bailar (0.0 menos bailable, 1.0 muy bailable).|
| energy           | Medida de intensidad y actividad de la pista (0.0 a 1.0).|
| key              | Tonalidad de la pista (números enteros mapeados a notas musicales).|
| loudness         | Volumen general de la pista en decibeles (dB).|
| mode             | Modalidad de la pista (1 para mayor, 0 para menor).|
| speechiness      | Detecta presencia de palabras habladas (0.0 a 1.0).|
| acousticness     | Medida de confianza de si la pista es acústica (0.0 a 1.0).|
| instrumentalness | Predice si la pista no contiene vocales (0.0 a 1.0).|
| liveness         | Detecta presencia de audiencia (0.0 a 1.0, mayor valor indica interpretación en vivo).|
| valence          | Positividad musical transmitida por la pista (0.0 a 1.0).|
| tempo            | Tempo estimado en BPM (ritmo de la pista).|
| time_signature   | Firma de tiempo estimada (3 a 7 indica compás de 3/4 a 7/4).|
| track_genre      | Género al que pertenece la pista.|


## SQLParser

El parser fue construido usando las herramientas de GNU, bison y flex. Flex usando para scannear el input y tokenizarlo, por otro lado, se construyó el GIC en bison, este funciona como el parser. Con estas herramientas se logró desarrollar un lenguaje simplificado de SQL, a la vez haciendo posible su escalabilidad y manejo a futuro.

### Queries

```CREATE TABLE {tablename} ({col1} {type} primary key, {col2} {type}, ...);```

{type}: int | double | bool | char | char(n)

```INSERT INTO {tablename} VALUES({value1}, {value2}, ...);```

```INSERT INTO {tablename} FROM {namefile};```

```SELECT { * | columnames } FROM tablename [WHERE {constraint1} (AND | OR) {constraint2} ... ];```

{constraint} : {value1} [ = | < | > | <= | >= ] {value2}

```CREATE INDEX [SEQ | AVL | ISAM] ON {tablename}({colname});```

```DROP TABLE {tablename};```

```DELETE FROM {tablename} WHERE {pk_col} = {value};```

### SELECT

El select acepta una lista de condiciones (ej. id = 4 AND col1 = 3 OR col2 = 'heider'). El parser separá en listas de listas estas condiciones, en donde el primer nivel de lista es por la separación de OR, y el segundo nivel de AND. Esto ayudara a la logica para realizar los search de mejor manera. Construira un lambda para cada lista más profunda, para cada atributo que no tiene un indice, caso contrario guardar ese condicional para realizar el search sobre ese, y al resultado se le aplicara el lambda. En este punto tendrá un resultado para cada lista inferior (AND), ahora, como lo hemos hecho, solo se concatenan los resultados para devolver el select.

## Experimentación

Para probar el funcionamiento del proyecto se realizaron pruebas de inserciones de múltiples registros, inserciones de un registro, búsqueda, búsqueda por rango y eliminación sobre 1K, 10K, 100K y 1M de datos.

### Bulk Insert

|            | 1k      | 10k   | 100k   | 1M      |
|------------|---------|-------|--------|---------|
| ISAM       | 16000µs | 185ms | 1807ms | 25713ms |
| AVL        | 351774µs| 4531ms| 53645ms| 684193ms|
| Sequencial | 10829µs |  94ms | 925ms  |  8968ms |

<div style="text-align:center"><img src="./imgs/Figure_1.png" /></div>
### Insert

|            | 1k   | 10k  | 100k | 1M   |
|------------|------|------|------|------|
| ISAM       | 34µs | 38µs | 47µs | 64µs |
| AVL        | 436µs| 475µs| 687µs| 777µs|
| Sequencial | 140µs| 176µs| 186µs| 248µs|

<div style="text-align:center"><img src="./imgs/Figure_2.png" /></div>

### Search

|            | 1k  | 10k  | 100k | 1M   |
|------------|-----|------|------|------|
| ISAM       | 8µs | 10µs | 15µs | 21µs |
| AVL        | 43µs| 49µs | 65µs | 80µs |
| Sequencial |131µs|156µs | 188µs| 223µs|

<div style="text-align:center"><img src="./imgs/Figure_3.png" /></div>

### Range Search

|            | 1k    | 10k    | 100k   | 1M      |
|------------|-------|--------|--------|---------|
| ISAM       | 228µs | 1203µs | 5402µs | 13213µs |
| AVL        |16058µs|159193µs| 1721ms | 17882ms |
| Sequencial |12152µs| 96530µs| 1118ms |  9550ms |

<div style="text-align:center"><img src="./imgs/Figure_4.png" /></div>

### Remove

|            | 1k    | 10k  | 100k | 1M   |
|------------|-------|------|------|------|
| ISAM       | 43 µs | 48µs | 52µs | 74µs |
| AVL        | 608µs | 831µs| 989µs|1163µs|
| Sequencial | 161 µs| 255µs| 293µs| 357µs|

<div style="text-align:center"><img src="./imgs/Figure_remove.png" /></div>


## Conclusiones

* El Indexed Secuential Access Method (ISAM) es una estructura muy eficiente para realizar búsquedas y búsquedas por rango. La principal desventaja es que al ser un índice estático, si tenemos muy pocos registros (<1k), estaríamos desperdiciando mucha memoria y si tenemos muchos registros (>10M), tiende a ser una búsqueda lineal.
* La inserción de varios registros en el Sequential Index es una query que toma mucho tiempo en completar debido a la reconstrucción de archivos. En base a esto se encontró una manera de aumentar la eficiencia de inserción para varios registros bajo tener un costo mayor de memoria.  
* El Avl Index consta de operaciones de inserción y eliminación que toman más tiempo que el sequential en realizarse, esto puede ser debido a que el avl realiza operaciones de balanceo tras estas operaciones. Por otro lado, sus búsquedas son más eficiente que las del Sequential, pues este solo hace búsquedas sobre el mismo archivo, mientras que el Sequential primero hace búsqueda binaria y luego linear en el archivo auxiliar.     
* El ISAM presenta una desventaja añadida al realizar un bulk insert debido a métodos personalizados propias de la inserción, sin embargo las operaciones de search son óptimas.

## Autores

| **Aaron Camacho** | **Nicolas Castañeda** | **Juaquín Remon** | **Enrique Flores** | **Renato Cernades** |
|:------------:|:------------:|:------------:|:------------:|:------------:|
| ![AaronCS25](https://avatars.githubusercontent.com/u/102536323?s=400&v=4) | ![nicolas-castaneda](https://avatars.githubusercontent.com/u/102196795?v=4) | ![jauquin456](https://avatars.githubusercontent.com/u/83974317?v=4) | ![Enriquefft](https://avatars.githubusercontent.com/u/60308719?v=4) | ![Avatar del Autor 5](https://avatars.githubusercontent.com/u/83974266?s=400&u=f9e6664839a841ae781d3932ca156316ab35ea03&v=4) |
| [github.com/AaronCS25](https://github.com/AaronCS25) | [github.com/NickCQCCS](https://github.com/nicolas-castaneda) | [github.com/juaquin456](https://github.com/juaquin456) | [github.com/Enriquefft](https://github.com/Usuario_Autor_4) | [github.com/Usuario_Autor_5](https://github.com/RenatoCernades0107) |

## Bibliografía

- [1] ["simple_wc_example"](https://github.com/jonathan-beard/simple_wc_example). Disponible en GitHub. [Accedido: 23 Sep, 2023]
- [2] ["Spotify Tracks Dataset"](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset). Disponible en Kaggle. [Accedido: 23 Sep, 2023]
- [3] ["Introduction to AVL Tree"](https://www.geeksforgeeks.org/introduction-to-avl-tree/). Disponible en GeeksforGeeks. [Accedido: 23 Sep, 2023]
- [4] ["File Organization in DBMS"](https://www.geeksforgeeks.org/file-organization-in-dbms-set-1/). Disponible en GeeksforGeeks. [Accedido: 23 Sep, 2023]
- [5] ["Why use a .tpp file when implementing templated functions and classes defined in a .h file?"](https://stackoverflow.com/questions/44774036/why-use-a-tpp-file-when-implementing-templated-functions-and-classes-defined-i). Disponible en StackOverflow. [Accedido: 23 Sep, 2023]
