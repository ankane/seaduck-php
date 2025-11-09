<?php

namespace Tests;

use PHPUnit\Framework\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    protected $catalog;

    protected function setUp(): void
    {
        $this->catalog = $this->catalog();

        foreach ($this->catalog->listTables('seaduck_php_test') as $t) {
            $this->catalog->dropTable($t);
        }
    }

    protected function catalog()
    {
        $catalog = getenv('CATALOG') ?: 'rest';
        if ($catalog == 'glue') {
            return new \SeaDuck\GlueCatalog(
                ...$this->catalogOptions(),
                warehouse: getenv('GLUE_WAREHOUSE')
            );
        } elseif ($catalog == 'rest') {
            return new \SeaDuck\RestCatalog(
                ...$this->catalogOptions(),
                uri: 'http://localhost:8181',
                _secretOptions: [
                    'type' => 's3',
                    'key_id' => 'admin',
                    'secret' => 'password',
                    'endpoint' => '127.0.0.1:9000',
                    'url_style' => 'path',
                    'use_ssl' => 0
                ]
            );
        } elseif ($catalog == 's3tables') {
            return new \SeaDuck\S3TablesCatalog(
                ...$this->catalogOptions(),
                arn: getenv('S3_TABLES_ARN')
            );
        } else {
            throw new \Exception('Unsupported catalog');
        }
    }

    protected function catalogOptions()
    {
        return ['defaultNamespace' => 'seaduck_php_test'];
    }

    protected function createEvents()
    {
        $this->catalog->sql('CREATE TABLE events (a bigint, b text)');
        $this->loadEvents();
    }

    protected function loadEvents()
    {
        $this->catalog->sql("COPY events FROM 'tests/support/data.csv'");
    }
}
