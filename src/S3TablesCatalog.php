<?php

namespace SeaDuck;

class S3TablesCatalog extends Catalog
{
    // https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_s3_tables
    public function __construct($arn, $defaultNamespace = 'main')
    {
        $attachOptions = [
            'endpoint_type' => 's3_tables'
        ];
        $secretOptions = [
            'type' => 's3',
            'provider' => 'credential_chain'
        ];
        parent::__construct(
            $arn,
            $defaultNamespace,
            $attachOptions,
            secretOptions: $secretOptions,
            extensions: ['aws', 'httpfs']
        );
    }
}
