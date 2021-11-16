<?php

namespace Vinelab\NeoEloquent;

use Closure;
use DateTime;
use Exception;
use Laudis\Neo4j\Authentication\Authenticate;
use Laudis\Neo4j\ClientBuilder;
use Laudis\Neo4j\Contracts\AuthenticateInterface;
use Laudis\Neo4j\Contracts\ClientInterface;
use Laudis\Neo4j\Contracts\TransactionInterface;
use Laudis\Neo4j\Databags\ResultSummary;
use Laudis\Neo4j\Databags\SummarizedResult;
use Laudis\Neo4j\Formatter\OGMFormatter;
use Laudis\Neo4j\Formatter\SummarizedResultFormatter;
use Laudis\Neo4j\Types\CypherList;
use LogicException;
use Neoxygen\NeoClient\Client;
use Throwable;
use Vinelab\NeoEloquent\Exceptions\InvalidCypherException;
use Vinelab\NeoEloquent\Exceptions\QueryException;
use Vinelab\NeoEloquent\Query\Builder as QueryBuilder;
use Vinelab\NeoEloquent\Query\Expression;
use Vinelab\NeoEloquent\Query\Grammars\CypherGrammar;
use Vinelab\NeoEloquent\Schema\Builder;
use Vinelab\NeoEloquent\Schema\Grammars\CypherGrammar as SchemaGrammar;
use Vinelab\NeoEloquent\Query\Grammars\Grammar;
use Vinelab\NeoEloquent\Query\Processors\Processor;
use Illuminate\Database\Connection as IlluminateConnection;
use Illuminate\Database\Schema\Grammars\Grammar as IlluminateSchemaGrammar;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Arr;
use function sprintf;

class Connection extends IlluminateConnection
{
    public const TYPE_HA = 'ha';
    public const TYPE_MULTI = 'multi';
    public const TYPE_SINGLE = 'single';

    /**
     * The Neo4j active client connection.
     *
     * @var ClientInterface
     */
    protected $neo;

    /**
     * The Neo4j database transaction.
     *
     * @var TransactionInterface
     */
    protected $transaction;

    /**
     * Default connection configuration parameters.
     *
     * @var array
     */
    protected $defaults = array(
        'scheme' => 'bolt',
        'host' => 'localhost',
        'port' => 7687,
        'username' => null,
        'password' => null,
    );

    /**
     * The neo4j driver name.
     *
     * @var string
     */
    protected $driverName = 'neo4j';

    /**
     * Create a new database connection instance.
     *
     * @param array $config The database connection configuration
     */
    public function __construct(array $config = [])
    {
        $this->config = $config;
        // activate and set the database client connection
        $this->neo = $this->createConnection();

        // We need to initialize a query grammar and the query post processors
        // which are both very important parts of the database abstractions
        // so we initialize these to their default values while starting.
        $this->useDefaultQueryGrammar();

        $this->useDefaultPostProcessor();
    }


    public function createConnection()
    {
        return $this->getClient();
    }

    /**
     * Create a new Neo4j client.
     *
     * @return ClientInterface
     */
    public function createSingleConnectionClient()
    {
        return $this->initBuilder()
            ->withDriver('default', $this->buildUriFromConfig($this->getConfig()), $this->getAuth())
            ->build();
    }

    private function initBuilder(): ClientBuilder
    {
        $formatter = new SummarizedResultFormatter(OGMFormatter::create());
        return ClientBuilder::create()->withFormatter($formatter);
    }


    public function createMultipleConnectionsClient()
    {
        $builder = $this->initBuilder();

        $default = $this->getConfigOption('default');

        foreach ($this->getConfigOption('connections') as $connection => $config) {
            if ($default === $connection) {
                $builder = $builder->withDefaultDriver($connection);
            }

            $builder = $builder->withDriver($connection, $this->buildUriFromConfig($config), $this->getAuth());
        }

        return $builder->build();
    }

    /**
     * Get the currenty active database client.
     *
     * @return ClientInterface
     */
    public function getClient()
    {
        if (!$this->neo) {
            $this->setClient($this->createSingleConnectionClient());
        }

        return $this->neo;
    }

    /**
     * Set the client responsible for the
     * database communication.
     *
     * @param ClientInterface $client
     */
    public function setClient(ClientInterface $client)
    {
        $this->neo = $client;
    }

    public function getScheme()
    {
        return $this->getConfig('scheme');
    }

    /**
     * Get the connection host.
     *
     * @return string
     */
    public function getHost()
    {
        return $this->getConfig('host');
    }

    /**
     * Get the connection port.
     *
     * @return int|string
     */
    public function getPort()
    {
        return $this->getConfig('port');
    }

    /**
     * Get the connection username.
     *
     * @return int|string
     */
    public function getUsername()
    {
        return $this->getConfig('username');
    }

    /**
     * Get the connection password.
     *
     * @return int|string
     */
    public function getPassword()
    {
        return $this->getConfig('password');
    }

    /**
     * Returns whether or not the connection should be secured.
     *
     * @return bool
     */
    public function isSecured(array $config)
    {
        return $this->getUsername() !== null && $this->getPassword() !== null;
    }

    /**
     * Get an option from the configuration options.
     *
     * @param string|null $option
     *
     * @return mixed
     */
    public function getConfig($option = null)
    {
        return Arr::get($this->config, $option);
    }

    /**
     * Get an option from the configuration options.
     *
     * @param string $option
     * @param mixed  $default
     *
     * @return mixed
     */
    public function getConfigOption($option, $default = null)
    {
        return Arr::get($this->getConfig(), $option, $default);
    }

    /**
     * Get the database connection name.
     *
     * @return string|null
     */
    public function getName()
    {
        return $this->getConfigOption('name');
    }

    /**
     * Get the Neo4j driver name.
     *
     * @return string
     */
    public function getDriverName()
    {
        return $this->driverName;
    }

    /**
     * Run a select statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return CypherList
     */
    public function select($query, $bindings = array(), $useReadPdo = false)
    {
        return $this->run($query, $bindings, function (self $me, $query, array $bindings) {
            if ($me->pretending()) {
                return array();
            }

            // For select statements, we'll simply execute the query and return an array
            // of the database result set. Each element in the array will be a single
            // node from the database, and will either be an array or objects.
            $query = $me->getCypherQuery($query, $bindings);

            /** @var SummarizedResult $results */
            $summary = $this->getClient()->run($query['statement'], $query['parameters']);
            /** @var CypherList $results */
            $results = $summary->getResult();

            return $results;
        });
    }

    /**
     * Run an insert statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return mixed
     */
    public function insert($query, $bindings = array())
    {
        return $this->statement($query, $bindings, true);
    }

    /**
     * Run a Cypher statement and get the number of nodes affected.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return SummarizedResult
     */
    public function affectingStatement($query, $bindings = array())
    {
        return $this->run($query, $bindings, function (self $me, $query, array $bindings) {
            if ($me->pretending()) {
                return 0;
            }

            // For update or delete statements, we want to get the number of rows affected
            // by the statement and return that back to the developer. We'll first need
            // to execute the statement and then we'll use CypherQuery to fetch the affected.
            $query = $me->getCypherQuery($query, $bindings);

            /** @var SummarizedResult $summarizedResult */
            return $this->getClient()->writeTransaction(static function (TransactionInterface $tsx) use ($query) {
                return $tsx->run($query['statement'], $query['parameters']);
            });
        });
    }

    /**
     * Execute a Cypher statement and return the boolean result.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return CypherList|bool
     */
    public function statement($query, $bindings = array(), $rawResults = false)
    {
        return $this->run($query, $bindings, function (self $me, $query, array $bindings) use ($rawResults) {
            if ($me->pretending()) {
                return true;
            }

            $query = $me->getCypherQuery($query, $bindings);

            /** @var SummarizedResult $run */
            $run = $this->getClient()->run($query['statement'], $query['parameters']);
            $results = $run->getResult();

            return ($rawResults === true) ? collect($results) : true;
        });
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param string $query
     *
     * @return bool
     */
    public function unprepared($query)
    {
        return $this->run($query, [], function ($me, $query) {
            if ($me->pretending()) {
                return true;
            }

            $this->getClient()->run($query);

            return true;
        });
    }

    /**
     * Make a query out of a Cypher statement
     * and the bindings values.
     *
     * @param string $query
     * @param array  $bindings
     */
    public function getCypherQuery($query, array $bindings)
    {
        return ['statement' => $query, 'parameters' => $this->prepareBindings($bindings)];
    }

    /**
     * Prepare the query bindings for execution.
     *
     * @param array $bindings
     *
     * @return array
     */
    public function prepareBindings(array $bindings)
    {
        $grammar = $this->getQueryGrammar();

        $prepared = array();

        foreach ($bindings as $key => $binding) {
            // The bindings are collected in a little bit different way than
            // Eloquent, we will need the key name in order to know where to replace
            // the value using the Neo4j client.
            $value = $binding;

            // We need to get the array value of the binding
            // if it were mapped
            if (is_array($value)) {
                // There are different ways to handle multiple
                // bindings vs. single bindings as values.
                $value = array_values($value);
            }

            // We need to transform all instances of the DateTime class into an actual
            // date string. Each query grammar maintains its own date string format
            // so we'll just ask the grammar for the format to get from the date.

            if ($value instanceof DateTime) {
                $binding = $value->format($grammar->getDateFormat());
            }

            // We will set the binding key and value, then
            // we replace the binding property of the id (if found)
            // with a _nodeId instead since the client
            // will not accept replacing "id(n)" with a value
            // which have been previously processed by the grammar
            // to be _nodeId instead.
            if (!is_array($binding)) {
                $binding = [$binding];
            }

            foreach ($binding as $property => $real) {
                // We should not pass any numeric key-value items since the Neo4j client expects
                // a JSON dictionary.
                if (is_numeric($property)) {
                    $property = (!is_numeric($key)) ? $key : 'id';
                }

                if ($property == 'id') {
                    $property = $grammar->getIdReplacement($property);
                }

                // when the value is an array means we have
                // a property as an array so we'll
                // keep adding to it.
                if (is_array($value)) {
                    $prepared[$property][] = $real;
                } else {
                    $prepared[$property] = $real;
                }
            }
        }

        return $prepared;
    }

    /**
     * Get the query grammar used by the connection.
     *
     * @return CypherGrammar
     */
    public function getQueryGrammar()
    {
        if (!$this->queryGrammar) {
            $this->useDefaultQueryGrammar();
        }

        return $this->queryGrammar;
    }

    /**
     * Get the default query grammar instance.
     *
     * @return CypherGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return new Query\Grammars\CypherGrammar();
    }

    /**
     * A binding should always be in an associative
     * form of a key=>value, otherwise we will not be able to
     * consider it a valid binding and replace its values in the query.
     * This function validates whether the binding is valid to be used.
     *
     * @param array $binding
     *
     * @return bool
     */
    public function isBinding(array $binding)
    {
        if (!empty($binding)) {
            // A binding is valid only when the key is not a number
            $keys = array_keys($binding);

            return !is_numeric(reset($keys));
        }

        return false;
    }

    /**
     * Execute a Closure within a transaction.
     *
     * @param Closure $callback
     *
     * @return mixed
     *
     * @throws Throwable
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        $this->beginTransaction();

        // We'll simply execute the given callback within a try / catch block
        // and if we catch any exception we can rollback the transaction
        // so that none of the changes are persisted to the database.
        try {
            $result = $callback($this);

            $this->commit();
        }

        // If we catch an exception, we will roll back so nothing gets messed
        // up in the database. Then we'll re-throw the exception so it can
        // be handled how the developer sees fit for their applications.
        catch (Exception $e) {
            $this->rollBack();

            throw $e;
        } catch (Throwable $e) {
            $this->rollBack();

            throw $e;
        }

        return $result;
    }

    /**
     * Start a new database transaction.
     */
    public function beginTransaction()
    {
        $this->transactions++;

        if ($this->transactions == 1) {
            $this->transaction = $this->neo->beginTransaction();
        }

        $this->fireConnectionEvent('beganTransaction');
    }

    /**
     * Commit the active database transaction.
     */
    public function commit()
    {
        if ($this->transactions == 1) {
            $this->transaction->commit();
        }

        $this->transactions--;

        $this->fireConnectionEvent('committed');
    }

    public function rollBack($toLevel = null)
    {
        if ($this->transactions == 1) {
            $this->transactions = 0;

            $this->transaction->rollBack();
        } else {
            $this->transactions--;
        }

        $this->fireConnectionEvent('rollingBack');
    }

    /**
     * Begin a fluent query against a database table.
     * In neo4j's terminologies this is a node.
     *
     * @param string      $table
     * @param string|null $as
     *
     * @return \Vinelab\NeoEloquent\Query\Builder
     */
    public function table($table, $as = null)
    {
        $query = new \Vinelab\NeoEloquent\Query\Builder($this, $this->getQueryGrammar(), $this->getPostProcessor());

        return $query->from($table, $as);
    }

    /**
     * Run a Cypher statement and log its execution context.
     *
     * @param string  $query
     * @param array   $bindings
     * @param Closure $callback
     *
     * @return mixed
     *
     * @throws QueryException
     */
    protected function run($query, $bindings, Closure $callback)
    {
        $start = microtime(true);

        // To execute the statement, we'll simply call the callback, which will actually
        // run the Cypher against the Neo4j connection. Then we can calculate the time it
        // took to execute and log the query Cypher, bindings and time in our memory.
        try {
            $result = $callback($this, $query, $bindings);
        }

        // If an exception occurs when attempting to run a query, we'll format the error
        // message to include the bindings with Cypher, which will make this exception a
        // lot more helpful to the developer instead of just the database's errors.
        catch (Exception $e) {
            $this->handleExceptions($query, $bindings, $e);
        }

        // Once we have run the query we will calculate the time that it took to run and
        // then log the query, bindings, and execution time so we will report them on
        // the event that the developer needs them. We'll log time in milliseconds.
        $time = $this->getElapsedTime($start);

        $this->logQuery($query, $bindings, $time);

        return $result;
    }

    /**
     * Run a Cypher statement.
     *
     * @param string   $query
     * @param array    $bindings
     * @param Closure $callback
     *
     * @return mixed
     *
     * @throws InvalidCypherException
     */
    protected function runQueryCallback($query, $bindings, Closure $callback)
    {
        // To execute the statement, we'll simply call the callback, which will actually
        // run the SQL against the PDO connection. Then we can calculate the time it
        // took to execute and log the query SQL, bindings and time in our memory.
        try {
            $result = $callback($this, $query, $bindings);
        }

        // If an exception occurs when attempting to run a query, we'll format the error
        // message to include the bindings with SQL, which will make this exception a
        // lot more helpful to the developer instead of just the database's errors.
        catch (Exception $e) {
            throw new QueryException(
                $query,
                $this->prepareBindings($bindings),
                $e
            );
        }

        return $result;
    }

    /**
     * Disconnect from the underlying PDO connection.
     */
    public function disconnect()
    {
        $this->neo = null;
    }

    /**
     * Reconnect to the database.
     *
     *
     * @throws LogicException
     */
    public function reconnect()
    {
        if (is_callable($this->reconnector)) {
            return call_user_func($this->reconnector, $this);
        }

        throw new LogicException('Lost connection and no reconnector available.');
    }

    /**
     * Reconnect to the database if a PDO connection is missing.
     */
    protected function reconnectIfMissingConnection()
    {
        if (is_null($this->getClient())) {
            $this->reconnect();
        }
    }

    /**
     * Set the reconnect instance on the connection.
     *
     * @param callable $reconnector
     *
     * @return $this
     */
    public function setReconnector(callable $reconnector)
    {
        $this->reconnector = $reconnector;

        return $this;
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return Builder
     */
    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new Schema\Builder($this);
    }

    /**
     * Handle exceptions thrown in $this::run()
     *
     * @throws mixed
     */
    protected function handleExceptions($query, $bindings, $e)
    {
        throw new QueryException($query, $bindings, $e);
    }

    /**
     * @return string
     */
    private function buildUriFromConfig(array $config): string
    {
        $uri = '';
        $scheme = $this->getScheme($config);
        if ($scheme) {
            $uri .= $scheme . '://';
        }

        $host = $this->getHost($config);
        if ($host) {
            $uri .= '@' . $host;
        }

        $port = $this->getPort($config);
        if ($port) {
            $uri .= ':' . $port;
        }

        return $uri;
    }

    /**
     * @return AuthenticateInterface
     */
    private function getAuth(): AuthenticateInterface
    {
        $username = $this->getUsername($this->getConfig());
        $password = $this->getPassword($this->getConfig());
        if ($username && $password) {
            return Authenticate::basic($username, $password);
        }

        return Authenticate::disabled();
    }
}
