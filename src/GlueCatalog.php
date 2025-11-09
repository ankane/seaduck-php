<?php

namespace SeaDuck;

class GlueCatalog extends Catalog
{
    // https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_sagemaker_lakehouse
    public function __construct($warehouse, $defaultNamespace = 'main')
    {
        $attachOptions = [
            'endpoint_type' => 'glue'
        ];
        $secretOptions = [
            'type' => 's3',
            'provider' => 'credential_chain'
        ];
        parent::__construct(
            $warehouse,
            $defaultNamespace,
            $attachOptions,
            secretOptions: $secretOptions
        );
    }
}
