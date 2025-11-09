<?php

namespace SeaDuck;

class Catalog
{
    private $catalog;
    private $db;
    private $defaultNamespace;

    public function __construct(
        $url,
        $defaultNamespace,
        $attachOptions,
        $secretOptions = null,
        $extensions = []
    ) {
        $this->catalog = 'iceberg';
        $this->defaultNamespace = $defaultNamespace;

        $this->db = \Saturio\DuckDB\DuckDB::create();
        $this->installExtension('iceberg');
        foreach ($extensions as $extension) {
            $this->installExtension($extension);
        }
        if ($secretOptions) {
            $this->createSecret($secretOptions);
        }
        $attachOptions = array_merge(['type' => 'iceberg'], $attachOptions);
        $this->attachWithOptions($this->catalog, (string) $url, $attachOptions);

        try {
            $this->useNamespace($defaultNamespace);
        } catch (\Saturio\DuckDB\Exception\PreparedStatementExecuteException $e) {
            $this->createNamespace($defaultNamespace, ifNotExists: true);
            $this->useNamespace($defaultNamespace);
        }
        $this->detach('memory');
    }

    public function listNamespaces()
    {
        return $this->execute('SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?', [$this->catalog])->rows();
    }

    public function createNamespace($namespace, $ifNotExists = null)
    {
        $this->execute('CREATE SCHEMA ' . ($ifNotExists ? 'IF NOT EXISTS ' : '') . $this->quoteNamespace($namespace));
    }

    public function namespaceExists($namespace)
    {
        return count($this->execute('SELECT 1 FROM information_schema.schemata WHERE catalog_name = ? AND schema_name = ?', [$this->catalog, $namespace])->rows()) > 0;
    }

    // CASCADE not implemented for Iceberg yet
    public function dropNamespace($namespace, $ifExists = null)
    {
        $this->execute('DROP SCHEMA ' . ($ifExists ? 'IF EXISTS ' : '') . $this->quoteNamespace($namespace));
    }

    public function listTables($namespace = null)
    {
        $sql = 'SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ?';
        $params = [$this->catalog];

        if ($namespace) {
            $sql .= ' AND table_schema = ?';
            array_push($params, $namespace);
        }

        return $this->execute($sql, $params)->rows();
    }

    public function tableExists($tableName)
    {
        [$namespace, $tableName] = $this->splitTable($tableName);
        return count($this->execute('SELECT 1 FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = ?', [$this->catalog, $namespace, $tableName])->rows()) > 0;
    }

    public function dropTable($table, $ifExists = null)
    {
        $this->execute('DROP TABLE ' . ($ifExists ? 'IF EXISTS ' : '') . $this->quoteTable($table));
    }

    public function snapshots($tableName)
    {
        return $this->execute('SELECT * FROM iceberg_snapshots(' . $this->quoteTable($tableName) . ')')->toArray();
    }

    public function sql($sql, $params = [])
    {
        return $this->execute($sql, $params);
    }

    public function attach($alias, $url)
    {
        $type = null;
        $extension = null;

        if (str_starts_with($url, 'postgres://') || str_starts_with($url, 'postgresql://')) {
            $type = 'postgres';
            $extension = 'postgres';
        } else {
            throw new \InvalidArgumentException('Unsupported data source type');
        }

        if ($extension) {
            $this->installExtension($extension);
        }

        $options = [
            'type' => $type,
            'read_only' => true
        ];
        $this->attachWithOptions($alias, $url, $options);
    }

    public function detach($alias)
    {
        $this->execute('DETACH ' . $this->quoteIdentifier($alias));
    }

    // experimental
    public function extensionVersion()
    {
        return $this->execute('SELECT extension_version FROM duckdb_extensions() WHERE extension_name = ?', ['iceberg'])->toArray()[0]['extension_version'];
    }

    // experimental
    public function duckdbVersion()
    {
        return $this->execute('SELECT VERSION() AS version')->toArray()[0]['version'];
    }

    // libduckdb does not provide function
    // https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers.html
    public function quoteIdentifier($value)
    {
        return '"' . str_replace('"', '""', $value) . '"';
    }

    // libduckdb does not provide function
    // TODO support more types
    public function quote($value)
    {
        if (is_null($value)) {
            return 'NULL';
        } elseif ($value === true) {
            return 'true';
        } elseif ($value === false) {
            return 'false';
        } elseif (is_int($value) || is_float($value)) {
            return (string) $value;
        } else {
            if ($value instanceof \DateTime) {
                $value = $value->format('Y-m-d\TH:i:s.up');
            }

            if (is_string($value)) {
                return "'" . str_replace("'", "''", $value) . "'";
            } else {
                throw new \TypeError("can't quote");
            }
        }
    }

    private function execute($sql, $params = [])
    {
        $stmt = $this->db->preparedStatement($sql);
        for ($i = 0; $i < count($params); $i++) {
            $param = $params[$i];
            if ($param instanceof \DateTime) {
                $param = \Saturio\DuckDB\Type\Timestamp::fromDatetime($param);
            }
            $stmt->bindParam($i + 1, $param);
        }
        $result = $stmt->execute();
        return new Result([...$result->columnNames()], [...$result->rows()]);
    }

    private function installExtension($extension)
    {
        $this->execute('INSTALL ' . $this->quoteIdentifier($extension));
    }

    private function createSecret($options)
    {
        $this->execute('CREATE SECRET (' . $this->optionsArgs($options) . ')');
    }

    private function attachWithOptions($alias, $url, $options)
    {
        $this->execute('ATTACH ' . $this->quote($url) . ' AS ' . $this->quoteIdentifier($alias) . ' (' . $this->optionsArgs($options) . ')');
    }

    private function optionsArgs($options)
    {
        return join(', ', array_map(fn ($k, $v) => $this->optionName($k) . ' ' . $this->quote($v), array_keys($options), $options));
    }

    private function optionName($k)
    {
        $name = strtoupper($k);
        // should never contain user input, but just to be safe
        if (!preg_match('/^[A-Z_]+$/', $name)) {
            throw new \InvalidArgumentException('Invalid option name');
        }
        return $name;
    }

    private function useNamespace($namespace)
    {
        $this->execute('USE ' . $this->quoteNamespace($namespace));
    }

    private function quoteNamespace($value)
    {
        return $this->quoteIdentifier($this->catalog) . '.' . $this->quoteIdentifier($value);
    }

    private function splitTable($value)
    {
        if (is_array($value)) {
            if (count($value) == 2) {
                return $value;
            } else {
                throw new \InvalidArgumentException('Invalid table identifier');
            }
        } else {
            return [$this->defaultNamespace, $value];
        }
    }

    private function quoteTable($value)
    {
        [$namespace, $tableName] = $this->splitTable($value);
        return $this->quoteNamespace($namespace) . '.' . $this->quoteIdentifier($tableName);
    }
}
