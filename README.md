# SeaDuck PHP

[Apache Iceberg](https://iceberg.apache.org/) for PHP, powered by libduckdb

[![Build Status](https://github.com/ankane/seaduck-php/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/seaduck-php/actions)

## Installation

Run:

```sh
composer require ankane/seaduck
```

Add scripts to `composer.json` to download the shared library:

```json
    "scripts": {
        "post-install-cmd": "SeaDuck\\Library::check",
        "post-update-cmd": "SeaDuck\\Library::check"
    }
```

And run:

```sh
composer install
```

## Getting Started

Create a client for an Iceberg catalog

```php
use SeaDuck\S3TablesCatalog;

$catalog = new S3TablesCatalog(arn: 'arn:aws:s3tables:...');
```

Note: SeaDuck requires a default namespace, which is `main` by default. This namespace is created if it does not exist. Pass `defaultNamespace` to use a different one.

Create a table

```php
$catalog->sql('CREATE TABLE events (id bigint, name text)');
```

Load data from a file

```php
$catalog->sql("COPY events FROM 'data.csv'");
```

You can also load data directly from other [data sources](https://duckdb.org/docs/stable/data/data_sources)

```php
$catalog->attach('blog', 'postgres://localhost:5432/blog');
$catalog->sql('INSERT INTO events SELECT * FROM blog.events');
```

Query the data

```php
$catalog->sql('SELECT COUNT(*) FROM events')->toArray();
```

## Namespaces

List namespaces

```php
$catalog->listNamespaces();
```

Create a namespace

```php
$catalog->createNamespace('main');
```

Check if a namespace exists

```php
$catalog->namespaceExists('main');
```

Drop a namespace

```php
$catalog->dropNamespace('main');
```

## Tables

List tables

```php
$catalog->listTables();
```

Check if a table exists

```php
$catalog->tableExists('events');
```

Drop a table

```php
$catalog->dropTable('events');
```

## Snapshots

Get snapshots for a table

```php
$catalog->snapshots('events');
```

Query the data at a specific snapshot version or time

```php
$catalog->sql('SELECT * FROM events AT (VERSION => ?)', [3]);
// or
$catalog->sql('SELECT * FROM events AT (TIMESTAMP => ?)', [new DateTime()]);
```

## SQL Safety

Use parameterized queries when possible

```php
$catalog->sql('SELECT * FROM events WHERE id = ?', [1]);
```

For places that do not support parameters, use `quote` or `quoteIdentifier`

```php
$quotedTable = $catalog->quoteIdentifier('events');
$quotedFile = $catalog->quote('path/to/data.csv');
$catalog->sql("COPY $quotedTable FROM $quotedFile");
```

## History

View the [changelog](https://github.com/ankane/seaduck-php/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/seaduck-php/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/seaduck-php/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/seaduck-php.git
cd seaduck
composer install

# REST catalog
docker compose up
CATALOG=rest composer test

# S3 Tables catalog
CATALOG=s3tables composer test

# Glue catalog
CATALOG=glue composer test
```
