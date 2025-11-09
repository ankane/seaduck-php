<?php

use Tests\TestCase;

final class SqlTest extends TestCase
{
    public function testResult()
    {
        $this->createEvents();
        $result = $this->catalog->sql('SELECT * FROM events');
        $this->assertEquals(['a', 'b'], $result->columns());
        $this->assertEquals([[1, 'one'], [2, 'two'], [3, 'three']], $result->rows());
        $this->assertEquals([['a' => 1, 'b' => 'one'], ['a' => 2, 'b' => 'two'], ['a' => 3, 'b' => 'three']], $result->toArray());
    }

    public function testTypes()
    {
        $this->assertIsInt($this->catalog->sql('SELECT 1')->rows()[0][0]);
        $this->assertIsFloat($this->catalog->sql('SELECT 1.0')->rows()[0][0]);
        $this->assertInstanceOf(Saturio\DuckDB\Type\Date::class, $this->catalog->sql('SELECT current_date')->rows()[0][0]);
        $this->assertInstanceOf(Saturio\DuckDB\Type\Time::class, $this->catalog->sql('SELECT current_time')->rows()[0][0]);
        $this->assertTrue($this->catalog->sql('SELECT true')->rows()[0][0]);
        $this->assertFalse($this->catalog->sql('SELECT false')->rows()[0][0]);
        $this->assertNull($this->catalog->sql('SELECT NULL')->rows()[0][0]);
    }

    public function testParams()
    {
        $this->assertIsInt($this->catalog->sql('SELECT ?', [1])->rows()[0][0]);
        $this->assertIsFloat($this->catalog->sql('SELECT ?', [1.0])->rows()[0][0]);
        $this->assertTrue($this->catalog->sql('SELECT ?', [true])->rows()[0][0]);
        $this->assertFalse($this->catalog->sql('SELECT ?', [false])->rows()[0][0]);
        $this->assertNull($this->catalog->sql('SELECT ?', [null])->rows()[0][0]);
        $this->assertInstanceOf(Saturio\DuckDB\Type\Timestamp::class, $this->catalog->sql('SELECT ?', [new DateTime()])->rows()[0][0]);
    }

    public function testExtraParams()
    {
        $this->expectException(Saturio\DuckDB\Exception\BindValueException::class);
        $this->expectExceptionMessage("Couldn't bind parameter '2' to prepared statement");

        $this->catalog->sql('SELECT ?', [1, 2]);
    }

    public function testUpdate()
    {
        $this->expectException(Saturio\DuckDB\Exception\BindValueException::class);
        $this->expectExceptionMessage('Not implemented Error');

        $this->createEvents();
        $this->catalog->sql('UPDATE events SET b = ? WHERE a = ?', ['two!', 2]);
    }

    public function testDelete()
    {
        $this->expectException(Saturio\DuckDB\Exception\BindValueException::class);
        $this->expectExceptionMessage('Not implemented Error');

        $this->createEvents();
        $this->catalog->sql('DELETE FROM events WHERE a = ?', [2]);
    }

    public function testView()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('Not implemented Error');

        $this->createEvents();
        $this->catalog->sql('CREATE VIEW events_view AS SELECT a AS c, b AS d FROM events');
    }

    public function testMultipleStatements()
    {
        // TODO fix
        $this->expectException(TypeError::class);

        $this->catalog->sql('SELECT 1; SELECT 2');
    }

    public function testQuoteIdentifier()
    {
        $this->assertEquals('"events"', $this->catalog->quoteIdentifier('events'));
        $this->assertEquals('"""events"""', $this->catalog->quoteIdentifier('"events"'));
    }

    public function testQuote()
    {
        $this->assertEquals('NULL', $this->catalog->quote(null));
        $this->assertEquals('true', $this->catalog->quote(true));
        $this->assertEquals('false', $this->catalog->quote(false));
        $this->assertEquals('1', $this->catalog->quote(1));
        $this->assertEquals('0.5', $this->catalog->quote(0.5));
        $this->assertEquals("'2025-01-02T03:04:05.123456Z'", $this->catalog->quote(new DateTime('2025-01-02 03:04:05.123456')));
        $this->assertEquals("'hello'", $this->catalog->quote('hello'));
    }

    public function testQuoteUnsupportedType()
    {
        $this->expectException(TypeError::class);
        $this->expectExceptionMessage("can't quote");

        $this->catalog->quote((object) 1);
    }

    public function testQuoteStatement()
    {
        $value = join(array_map(fn ($i) => array_rand(array_flip(['a', "'", '"', '\\'])), range(1, 19)));
        $this->assertEquals($value, $this->catalog->sql('SELECT ' . $this->catalog->quote($value) . ' AS value')->rows()[0][0]);
    }
}
