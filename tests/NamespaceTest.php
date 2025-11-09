<?php

use Tests\TestCase;

final class NamespaceTest extends TestCase
{
    public function testListNamespaces()
    {
        $this->assertContains(['seaduck_php_test'], $this->catalog->listNamespaces());
    }

    public function testCreateNamespace()
    {
        $this->catalog->dropNamespace('seaduck_php_test2', ifExists: true);

        $this->catalog->createNamespace('seaduck_php_test2');
        $this->assertTrue($this->catalog->namespaceExists('seaduck_php_test2'));
    }

    public function testCreateNamespaceIfNotExists()
    {
        $this->expectNotToPerformAssertions();

        $this->catalog->dropNamespace('seaduck_php_test2', ifExists: true);

        $this->catalog->createNamespace('seaduck_php_test2');
        $this->catalog->createNamespace('seaduck_php_test2', ifNotExists: true);
    }

    public function testCreateNamespaceAlreadyExists()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('already exists');

        $this->catalog->createNamespace('seaduck_php_test');
    }

    public function testNamespaceExists()
    {
        $this->assertTrue($this->catalog->namespaceExists('seaduck_php_test'));
        $this->assertFalse($this->catalog->namespaceExists('missing'));
    }

    public function testDropNamespace()
    {
        $this->catalog->dropNamespace('seaduck_php_test2', ifExists: true);

        $this->catalog->createNamespace('seaduck_php_test2');
        $this->assertTrue($this->catalog->namespaceExists('seaduck_php_test2'));

        $this->catalog->dropNamespace('seaduck_php_test2');
        $this->assertFalse($this->catalog->namespaceExists('seaduck_php_test2'));
    }

    public function testDropNamespaceIfExists()
    {
        $this->expectNotToPerformAssertions();

        $this->catalog->dropNamespace('missing', ifExists: true);
    }

    public function testDropNamespaceMissing()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('does not exist');

        $this->catalog->dropNamespace('missing');
    }

    public function testDropNamespaceNotEmpty()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('is not empty');

        $this->catalog->sql('CREATE TABLE events (id bigint)');
        $this->catalog->dropNamespace('seaduck_php_test');
    }
}
