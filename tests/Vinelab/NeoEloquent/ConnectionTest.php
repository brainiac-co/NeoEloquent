<?php namespace Vinelab\NeoEloquent\Tests;

use DB, Mockery as M;

class ConnectionTest extends TestCase {

    public function setUp()
    {
        parent::setUp();

        $this->user = array(
            'name'     => 'Mulkave',
            'email'    => 'me@mulkave.io',
            'username' => 'mulkave'
        );
    }

    public function testConnection()
    {
        $c = DB::connection('neo4j');

        $this->assertInstanceOf('Vinelab\NeoEloquent\Connection', $c);

        $c1 = DB::connection('neo4j');
		$c2 = DB::connection('neo4j');

		$this->assertEquals(spl_object_hash($c1), spl_object_hash($c2));
    }

    public function testConnectionClientInstance()
    {
        $c = DB::connection('neo4j');

        $client = $c->getClient();

        $this->assertInstanceOf('Everyman\Neo4j\Client', $client);
    }

    public function testGettingConfigParam()
    {
        $c = DB::connection('neo4j');

        $this->assertEquals($c->getConfig('port'), 7474);
        $this->assertEquals($c->getConfig('host'), 'localhost');
    }

    public function testDriverName()
    {
        $c = DB::connection('neo4j');

        $this->assertEquals('neo4j', $c->getDriverName());
    }

    public function testGettingClient()
    {
        $c = DB::connection('neo4j');

        $this->assertInstanceOf('Everyman\Neo4j\Client', $c->getClient());
    }

    public function testGettingDefaultHost()
    {
        $c = DB::connection('default');

        $this->assertEquals('localhost', $c->getHost());
    }

    public function testGettingDefaultPort()
    {
        $c = DB::connection('default');

        $port=  $c->getPort();

        $this->assertEquals(7474, $port);
        $this->assertInternalType('int', $port);
    }

    public function testGettingQueryCypherGrammar()
    {
        $c = DB::connection('default');

        $grammar = $c->getQueryGrammar();

        $this->assertInstanceOf('Vinelab\NeoEloquent\Query\Grammars\CypherGrammar', $grammar);
    }

    public function testPrepareBindings()
    {
        $date = M::mock('DateTime');
        $date->shouldReceive('format')->once()->with('foo')->andReturn('bar');

        $bindings = array('test' => $date);

        $conn = $this->getMockConnection();
        $grammar = m::mock('Vinelab\NeoEloquent\Query\Grammars\CypherGrammar');
        $grammar->shouldReceive('getDateFormat')->once()->andReturn('foo');
        $conn->setQueryGrammar($grammar);
        $result = $conn->prepareBindings($bindings);

        $this->assertEquals(array('test' => 'bar'), $result);
    }

    public function testLogQueryFiresEventsIfSet()
    {
        $connection = $this->getMockConnection();
        $connection->logQuery('foo', array(), time());
        $connection->setEventDispatcher($events = m::mock('Illuminate\Events\Dispatcher'));
        $events->shouldReceive('fire')->once()->with('illuminate.query', array('foo', array(), null, null));
        $connection->logQuery('foo', array(), null);
    }

    public function testPretendOnlyLogsQueries()
    {
        $connection = $this->getMockConnection();
        $queries = $connection->pretend(function($connection)
        {
            $connection->select('foo bar', array('baz'));
        });
        $this->assertEquals('foo bar', $queries[0]['query']);
        $this->assertEquals(array('baz'), $queries[0]['bindings']);
    }

    public function testResolvingPaginatorThroughClosure()
    {
        $connection = $this->getMockConnection();
        $paginator  = m::mock('Illuminate\Pagination\Factory');
        $connection->setPaginator(function() use ($paginator)
        {
            return $paginator;
        });
        $this->assertEquals($paginator, $connection->getPaginator());
    }

    public function testResolvingCacheThroughClosure()
    {
        $connection = $this->getMockConnection();
        $cache  = m::mock('Illuminate\Cache\CacheManager');
        $connection->setCacheManager(function() use ($cache)
        {
            return $cache;
        });
        $this->assertEquals($cache, $connection->getCacheManager());
    }

    public function testPreparingSimpleBindings()
    {
        $bindings = array(
            'username' => 'jd',
            'name' => 'John Doe'
        );

        $c = DB::connection('default');

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($bindings, $prepared);
    }

    public function testPreparingWheresBindings()
    {
        $bindings = array(
            array('username' => 'jd'),
            array('email'    => 'marie@curie.sci')
        );

        $c = DB::connection('default');

        $expected = array(
            'username' => 'jd',
            'email'    => 'marie@curie.sci'
        );

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($expected, $prepared);
    }

    public function testPreparingFindByIdBindings()
    {
        $bindings = array(
            array('id' => 6)
        );

        $c = DB::connection('default');

        $expected = array('_nodeId' => 6);

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($expected, $prepared);
    }

    public function testPreparingWhereInBindings()
    {
        $bindings = array('mc', 'ae', '2pac', 'mulkave');

        $c = DB::connection('default');

        $expected = array(
            'mc'      => 'mc',
            'ae'      => 'ae',
            '2pac'    => '2pac',
            'mulkave' => 'mulkave'
        );

        $prepared = $c->prepareBindings($bindings);

        $this->assertEquals($expected, $prepared);
    }

    public function testGettingCypherGrammar()
    {
        $c = DB::connection('default');

        $query = $c->getCypherQuery('MATCH (u:`User`) RETURN * LIMIT 10', array());

        $this->assertInstanceOf('Everyman\Neo4j\Cypher\Query', $query);
    }

    public function testCheckingIfBindingIsABinding()
    {
        $c = DB::connection('default');

        $empty = array();
        $valid = array('key' => 'value');
        $invalid = array(array('key' => 'value'));
        $bastard = array(array('key' => 'value'), 'another' => 'value');

        $this->assertFalse($c->isBinding($empty));
        $this->assertFalse($c->isBinding($invalid));
        $this->assertFalse($c->isBinding($bastard));
        $this->assertTrue($c->isBinding($valid));
    }

    public function testCreatingConnection()
    {
        $c = DB::connection('default');

        $connection = $c->createConnection();

        $this->assertInstanceOf('Everyman\Neo4j\Client', $connection);
    }

    public function testSelectWithBindings()
    {
        $created = $this->createUser();

        $query = 'MATCH (n:`User`) WHERE n.username = {username} RETURN * LIMIT 1';

        $bindings = array(array('username' => $this->user['username']));

        $c = DB::connection('default');

        $results = $c->select($query, $bindings);

        $log = $c->getQueryLog();
        $log = reset($log);

        $this->assertEquals($log['query'], $query);
        $this->assertEquals($log['bindings'], $bindings);
        $this->assertInstanceOf('Everyman\Neo4j\Query\ResultSet', $results);

        // This is how we get the first row of the result (first [0])
        // and then we get the Node instance (the 2nd [0])
        // and then ask it to return its properties
        $selected = $results[0][0]->getProperties();

        $this->assertEquals($this->user, $selected, 'The fetched User must be the same as the one we just created');
    }

    /**
     * @depends testSelectWithBindings
     */
    public function testSelectWithBindingsById()
    {

        // Create the User record
        $created = $this->createUser();

        $c = DB::connection('default');

        $query = 'MATCH (n:`User`) WHERE n.username = {username} RETURN * LIMIT 1';

        // Get the ID of the created record
        $results = $c->select($query, array(array('username' => $this->user['username'])));

        $node = $results[0][0];
        $id = $node->getId();

        $bindings = array(
            array('id' => $id)
        );

        // Select the Node containing the User record by its id
        $query = 'MATCH (n:`User`) WHERE id(n) = {_nodeId} RETURN * LIMIT 1';

        $results = $c->select($query, $bindings);

        $log = $c->getQueryLog();

        $this->assertEquals($log[1]['query'], $query);
        $this->assertEquals($log[1]['bindings'], $bindings);
        $this->assertInstanceOf('Everyman\Neo4j\Query\ResultSet', $results);

        $selected = $results[0][0]->getProperties();

        $this->assertEquals($this->user, $selected);
    }

    public function testAffectingStatement()
    {
        $c = DB::connection('default');

        $created = $this->createUser();

        $type = 'dev';

        // Now we update the type and set it to $type
        $query = 'MATCH (n:`User`) WHERE n.username = {username} ' .
                 'SET n.type = {type}, n.updated_at = {updated_at} ' .
                 'RETURN count(n)';

        $bindings = array(
            array('type'       => $type),
            array('updated_at' => '2014-05-11 13:37:15'),
            array('username'   => $this->user['username'])
        );

        $results = $c->affectingStatement($query, $bindings);

        $this->assertInstanceOf('Everyman\Neo4j\Query\ResultSet', $results);

        foreach($results as $result)
        {
            $count = $result[0];
            $this->assertEquals(1, $count);
        }

        // Try to find the updated one and make sure it was updated successfully
        $query = 'MATCH (n:User) WHERE n.username = {username} RETURN n';
        $cypher = $c->getCypherQuery($query, array(array('username' => $this->user['username'])));

        $results = $cypher->getResultSet();

        $this->assertInstanceOf('Everyman\Neo4j\Query\ResultSet', $results);

        $user = null;

        foreach ($results as $result)
        {
            $node = $result[0];
            $user = $node->getProperties();

        }

        $this->assertEquals($type, $user['type']);
    }

    public function testAffectingStatementOnNonExistingRecord()
    {
        $c = DB::connection('default');

        $type = 'dev';

        // Now we update the type and set it to $type
        $query = 'MATCH (n:`User`) WHERE n.username = {username} ' .
                 'SET n.type = {type}, n.updated_at = {updated_at} ' .
                 'RETURN count(n)';

        $bindings = array(
            array('type'       => $type),
            array('updated_at' => '2014-05-11 13:37:15'),
            array('username'    => $this->user['username'])
        );

        $results = $c->affectingStatement($query, $bindings);

        $this->assertInstanceOf('Everyman\Neo4j\Query\ResultSet', $results);

        foreach($results as $result)
        {
            $count = $result[0];
            $this->assertEquals(0, $count);
        }
    }

    public function tearDown()
    {
        $query = 'MATCH (n:User) WHERE n.username = {username} DELETE n RETURN count(n)';

        $c = DB::connection('default');

        $cypher = $c->getCypherQuery($query, array(array('username' => $this->user['username'])));
        $cypher->getResultSet();

        parent::tearDown();
    }

    public function testSettingDefaultCallsGetDefaultGrammar()
    {
        $connection = $this->getMockConnection();
        $mock = M::mock('StdClass');
        $connection->expects($this->once())->method('getDefaultQueryGrammar')->will($this->returnValue($mock));
        $connection->useDefaultQueryGrammar();
        $this->assertEquals($mock, $connection->getQueryGrammar());
    }

    public function testSettingDefaultCallsGetDefaultPostProcessor()
    {
        $connection = $this->getMockConnection();
        $mock = M::mock('StdClass');
        $connection->expects($this->once())->method('getDefaultPostProcessor')->will($this->returnValue($mock));
        $connection->useDefaultPostProcessor();
        $this->assertEquals($mock, $connection->getPostProcessor());
    }

    public function testSelectOneCallsSelectAndReturnsSingleResult()
    {
        $connection = $this->getMockConnection(array('select'));
        $connection->expects($this->once())->method('select')->with('foo', array('bar' => 'baz'))->will($this->returnValue(array('foo')));
        $this->assertEquals('foo', $connection->selectOne('foo', array('bar' => 'baz')));
    }

    public function testInsertCallsTheStatementMethod()
    {
        $connection = $this->getMockConnection(array('statement'));
        $connection->expects($this->once())->method('statement')
            ->with($this->equalTo('foo'), $this->equalTo(array('bar')))
            ->will($this->returnValue('baz'));
        $results = $connection->insert('foo', array('bar'));
        $this->assertEquals('baz', $results);
    }

    public function testUpdateCallsTheAffectingStatementMethod()
    {
        $connection = $this->getMockConnection(array('affectingStatement'));
        $connection->expects($this->once())->method('affectingStatement')->with($this->equalTo('foo'), $this->equalTo(array('bar')))->will($this->returnValue('baz'));
        $results = $connection->update('foo', array('bar'));
        $this->assertEquals('baz', $results);
    }

    public function testDeleteCallsTheAffectingStatementMethod()
    {
        $connection = $this->getMockConnection(array('affectingStatement'));
        $connection->expects($this->once())->method('affectingStatement')->with($this->equalTo('foo'), $this->equalTo(array('bar')))->will($this->returnValue('baz'));
        $results = $connection->delete('foo', array('bar'));
        $this->assertEquals('baz', $results);
    }

    public function testBeganTransactionFiresEventsIfSet()
    {
        $connection = $this->getMockConnection(array('getName'));
        $connection->expects($this->once())->method('getName')->will($this->returnValue('name'));
        $connection->setEventDispatcher($events = m::mock('Illuminate\Events\Dispatcher'));
        $events->shouldReceive('fire')->once()->with('connection.name.beganTransaction', $connection);
        $connection->beginTransaction();
    }

    public function testCommitedFiresEventsIfSet()
    {
        $connection = $this->getMockConnection(array('getName'));
        $connection->expects($this->once())->method('getName')->will($this->returnValue('name'));
        $connection->setEventDispatcher($events = m::mock('Illuminate\Events\Dispatcher'));
        $events->shouldReceive('fire')->once()->with('connection.name.committed', $connection);
        $connection->commit();
    }

    public function testRollBackedFiresEventsIfSet()
    {
        $connection = $this->getMockConnection(array('getName'));
        $connection->expects($this->once())->method('getName')->will($this->returnValue('name'));
        $connection->setEventDispatcher($events = m::mock('Illuminate\Events\Dispatcher'));
        $events->shouldReceive('fire')->once()->with('connection.name.rollingBack', $connection);
        $connection->rollBack();
    }

    public function testTransactionMethodRunsSuccessfully()
    {
        $client = M::mock('Everyman\Neo4j\Client');
        $client->shouldReceive('beginTransaction')->once()
            ->andReturn(M::mock('Everyman\Neo4j\Transaction')->makePartial());

        $connection  = $this->getMockConnection();
        $connection->setClient($client);

        $result = $connection->transaction(function($db) { return $db; });
        $this->assertEquals($connection, $result);
    }

    public function testTransactionMethodRollsbackAndThrows()
    {
        $neo = M::mock('Everyman\Neo4j\Client');
        $neo->shouldReceive('beginTransaction')->once()
            ->andReturn(M::mock('Everyman\Neo4j\Transaction')->makePartial());

        $connection = $this->getMockConnection();
        $connection->setClient($neo);

        try
        {
            $connection->transaction(function() { throw new \Exception('foo'); });
        }
        catch (\Exception $e)
        {
            $this->assertEquals('foo', $e->getMessage());
        }
    }

    public function testFromCreatesNewQueryBuilder()
    {
        $conn = $this->getMockConnection();
        $conn->setQueryGrammar(M::mock('Vinelab\NeoEloquent\Query\Grammars\CypherGrammar'));
        $builder = $conn->table('users');
        $this->assertInstanceOf('Vinelab\NeoEloquent\Query\Builder', $builder);
        $this->assertEquals('users', $builder->from);
    }

    /*
     * Utility methods below this line
     */

    public function createUser()
    {
        $c = DB::connection('default');

        // First we create the record that we need to update
        $create = 'CREATE (u:User {name: {name}, email: {email}, username: {username}})';
        // The bindings structure is a little weird, I know
        // but this is how they are collected internally
        // so bare with it =)
        $createCypher = $c->getCypherQuery($create, array(
            array('name'     => $this->user['name']),
            array('email'    => $this->user['email']),
            array('username' => $this->user['username'])
        ));

        return $createCypher->getResultSet();
    }

    protected function getMockConnection($methods = array())
    {
        $defaults = array('getDefaultQueryGrammar', 'getDefaultPostProcessor', 'getDefaultSchemaGrammar');
        return $this->getMock('Vinelab\NeoEloquent\Connection', array_merge($defaults, $methods), array());
    }

}

class DatabaseConnectionTestMockNeo { }
