<?php

namespace SeaDuck;

class RestCatalog extends Catalog
{
    public function __construct(
        $uri,
        $warehouse = null,
        $defaultNamespace = 'main',
        $_secretOptions = null
    ) {
        $attachOptions = [
            'endpoint' => $uri,
            'authorization_type' => 'none'
        ];
        parent::__construct(
            $warehouse,
            $defaultNamespace,
            $attachOptions,
            secretOptions: $_secretOptions
        );
    }
}
