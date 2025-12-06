<?php

use Tests\TestCase;

final class CatalogTest extends TestCase
{
    public function testSnapshots()
    {
        $this->createEvents();

        $snapshots = $this->catalog->snapshots('events');
        $this->assertCount(1, $snapshots);
        $this->assertEquals([1], array_map(fn ($v) => $v['sequence_number'], $snapshots));

        $this->loadEvents();

        $snapshots = $this->catalog->snapshots('events');
        $this->assertCount(2, $snapshots);
        $this->assertEquals([2, 1], array_map(fn ($v) => $v['sequence_number'], $snapshots));
    }

    public function testSchemaEvolution()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('Not implemented Error');

        $this->createEvents();
        $this->catalog->sql("ALTER TABLE events ADD COLUMN c VARCHAR DEFAULT 'hello'");
    }

    public function testTimeTravel()
    {
        // TODO remove
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);

        $this->createEvents();
        $snapshots = $this->catalog->snapshots('events');
        $snapshotVersion = end($snapshots)['snapshot_id'];
        $this->loadEvents();

        $this->assertCount(6, $this->catalog->sql('SELECT * FROM events')->toArray());
        $this->assertCount(3, $this->catalog->sql('SELECT * FROM events AT (VERSION => ?)', [$snapshotVersion])->toArray());
    }

    public function testListTables()
    {
        $this->createEvents();
        $this->assertContains(['seaduck_php_test', 'events'], $this->catalog->listTables());
    }

    public function testListTablesNamespace()
    {
        $this->createEvents();
        $this->assertContains(['seaduck_php_test', 'events'], $this->catalog->listTables('seaduck_php_test'));
    }

    public function testListTablesNamespaceMissing()
    {
        $this->assertEmpty($this->catalog->listTables('missing'));
    }

    public function testTableExists()
    {
        $this->assertFalse($this->catalog->tableExists('events'));
        $this->createEvents();
        $this->assertTrue($this->catalog->tableExists('events'));
    }

    public function testDropTable()
    {
        $this->expectNotToPerformAssertions();

        $this->createEvents();
        $this->catalog->dropTable('events');
    }

    public function testDropTableMissing()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('Table with name events does not exist!');

        $this->catalog->dropTable('events');
    }

    public function testDropTableIfExists()
    {
        $this->expectNotToPerformAssertions();

        $this->createEvents();
        $this->catalog->dropTable('events', ifExists: true);
        $this->catalog->dropTable('events', ifExists: true);
    }

    public function testAttachPostgres()
    {
        $pg = pg_connect('dbname=seaduck_php_test');
        pg_query($pg, 'DROP TABLE IF EXISTS postgres_events');
        pg_query($pg, 'CREATE TABLE postgres_events (id bigint, name text)');
        pg_query_params($pg, 'INSERT INTO postgres_events VALUES ($1, $2)', [1, 'Test']);

        $this->catalog->attach('pg', 'postgres://localhost/seaduck_php_test');

        $this->catalog->sql('CREATE TABLE events (id bigint, name text)');
        $this->catalog->sql('INSERT INTO events SELECT * FROM pg.postgres_events');

        $expected = [['id' => 1, 'name' => 'Test']];
        $this->assertEquals($expected, $this->catalog->sql('SELECT * FROM events')->toArray());

        $this->catalog->detach('pg');

        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('Table with name postgres_events does not exist!');
        $this->catalog->sql('INSERT INTO events SELECT * FROM pg.postgres_events');
    }

    public function testAttachPostgresReadOnly()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('attached in read-only mode!');

        $pg = pg_connect('dbname=seaduck_php_test');
        pg_query($pg, 'DROP TABLE IF EXISTS postgres_events');
        pg_query($pg, 'CREATE TABLE postgres_events (id bigint, name text)');

        $this->catalog->attach('pg', 'postgres://localhost/seaduck_php_test');

        $this->catalog->sql('INSERT INTO pg.postgres_events VALUES (?, ?)', [2, 'Test 2']);
    }

    public function testAttachUnsupportedType()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported data source type');

        $this->catalog->attach('hello', 'pg://');
    }

    public function testExtensionVersion()
    {
        $this->assertEquals('db7c01e9', $this->catalog->extensionVersion());
    }

    public function testDuckdbVersion()
    {
        $this->assertEquals('v1.4.2', $this->catalog->duckdbVersion());
    }
}
