# ExSQL Migrator

ExSQL Migrator es una aplicación de escritorio hecha con Electron para convertir uno o varios archivos Excel en tablas SQL listas para migrarse a MySQL, PostgreSQL o SQL Server.

La experiencia está pensada como un flujo guiado de producto: seleccionas archivos, revisas las tablas detectadas, validas la conexión y ejecutas la migración con seguimiento en tiempo real.

## Qué resuelve

ExSQL acelera la carga de información desde hojas de cálculo hacia una base de datos relacional cuando necesitas:

- convertir múltiples hojas o múltiples archivos en tablas SQL
- detectar columnas y tipos automáticamente
- previsualizar el SQL antes de tocar la base destino
- decidir qué hacer si una tabla ya existe
- revisar resultados y errores sin salir de la aplicación

## Funciones principales

- Importación de archivos .xlsx, .xls y .xlsm
- Soporte para seleccionar varios archivos en una sola corrida
- Detección automática de múltiples tablas dentro de una misma hoja
- Inferencia de tipos SQL como INTEGER, BIGINT, DECIMAL, VARCHAR, DATE, DATETIME, BOOLEAN y JSON
- Procesamiento en streaming para archivos grandes
- Preview de DDL antes de ejecutar la migración
- Modos de destino: eliminar y recrear, vaciar e insertar, agregar datos, sincronizar y actualizar, u omitir tabla
- Progreso en tiempo real con métricas, advertencias y errores
- Explorador SQL integrado con consultas de solo lectura y gráficos básicos

## Motores soportados

- MySQL
- PostgreSQL
- SQL Server

## Flujo de uso

### 1. Selecciona tus archivos Excel

Haz clic en Seleccionar archivos o usa el botón Abrir Excels del ribbon. La aplicación acepta uno o varios archivos al mismo tiempo.

Durante el análisis, ExSQL detecta automáticamente tablas por hoja y también identifica tablas separadas por filas o columnas vacías.

### 2. Revisa las tablas detectadas

Antes de migrar, la interfaz muestra:

- nombre sugerido de tabla
- archivo de origen
- hoja de origen
- número de filas
- columnas detectadas y tipo inferido

Esto permite validar la estructura antes de conectarte a la base destino.

### 3. Configura la conexión SQL

Selecciona el motor de base de datos y completa los datos de conexión:

- host o servidor
- puerto
- base de datos
- usuario
- contraseña
- instancia, si usas SQL Server

Puedes usar el botón Probar conexión antes de migrar.

### 4. Ajusta las opciones de migración

Las opciones disponibles en la interfaz son:

- Tamaño de batch: controla cuántas filas se envían por bloque
- Si la tabla existe: decide cómo se tratará la tabla destino
- Prefijo de tablas: agrega un prefijo a los nombres creados
- Crear índices automáticos: mejora búsqueda y sincronización cuando aplica
- Usar transacciones: ejecuta la inserción de manera más segura
- Crear tabla de log: registra el resultado de la corrida en la base destino

### 5. Previsualiza el SQL

Con Preview SQL puedes inspeccionar el DDL generado antes de ejecutar la migración. Esto ayuda a validar nombres, tipos de datos y estructura objetivo.

### 6. Ejecuta la migración

Al iniciar la migración, ExSQL muestra:

- fase actual
- porcentaje de avance
- tabla en proceso
- filas procesadas
- tiempo transcurrido
- tiempo estimado restante
- advertencias y errores de la corrida

Cuando termina, la aplicación presenta un resumen por tabla con filas insertadas, actualizadas, omitidas, duplicadas y eliminadas según el modo seleccionado.

### 7. Consulta la base y genera gráficos

Después de migrar, puedes abrir el módulo Consultas SQL y Gráficos para:

- listar tablas del esquema conectado
- preparar consultas base con un clic
- ejecutar consultas de solo lectura
- ver resultados tabulares
- crear gráficos de barras o líneas a partir de los resultados

## Modos cuando la tabla ya existe

### Eliminar y recrear

Borra la tabla destino y la vuelve a crear desde cero.

### Vaciar e insertar

Mantiene la estructura actual, vacía sus datos y vuelve a cargar la información.

### Agregar datos

Conserva la tabla y agrega nuevas filas al contenido existente.

### Sincronizar y actualizar

Intenta detectar claves confiables para insertar filas nuevas, actualizar existentes y eliminar ausentes cuando la estructura lo permite.

### Omitir tabla

No procesa una tabla si ya existe en la base destino.

## Privacidad y tratamiento de datos

- ExSQL lee los archivos Excel desde su ubicación original.
- ExSQL no copia ni exporta automáticamente los archivos Excel al proyecto.
- El código fuente no crea una base de datos local propia para almacenar tus datos.
- Electron sí genera caché de ejecución en AppData, como cualquier app de escritorio, pero ahí no se guardan tus archivos Excel.
- Si activas Crear tabla de log, ExSQL crea la tabla _exsql_migration_log en la base destino y registra el nombre del archivo, la tabla migrada, filas procesadas, errores, fechas y el dialecto usado.

## Instalación

### Requisitos

- Node.js
- npm
- acceso a una base MySQL, PostgreSQL o SQL Server

### Instalar dependencias

```bash
npm install
```

## Desarrollo local

### Ejecutar en modo desarrollo

```bash
npm run dev
```

### Compilar la aplicación

```bash
npm run build
```

### Generar instaladores

```bash
npm run dist
```

Los paquetes generados se escriben en la carpeta release.

## Estructura del repositorio

```text
excel-to-sql-migrator/
  assets/
  src/
    main/
    renderer/
    shared/
  package.json
  package-lock.json
  tsconfig.main.json
  tsconfig.renderer.json
```

## Tecnologías

- Electron
- TypeScript en proceso principal y tipos compartidos
- JavaScript en renderer
- Knex para conectividad SQL multi-dialecto
- ExcelJS y SheetJS para lectura de Excel

## Publicación en GitHub

El repositorio está preparado para subir el código fuente completo de la aplicación, sin dependencias instaladas ni archivos de datos locales accidentales. Revisa .gitignore antes de hacer tu primer commit si más adelante decides versionar archivos de ejemplo.
