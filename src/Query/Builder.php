<?php

namespace Vinelab\NeoEloquent\Query;

use BadMethodCallException;
use Carbon\Carbon;
use Closure;
use DateTime;
use GraphAware\Common\Result\AbstractRecordCursor as Result;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder as IlluminateQueryBuilder;
use Illuminate\Database\Query\Processors\Processor as IlluminateProcessor;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Str;
use Laudis\Neo4j\Types\CypherList;
use Laudis\Neo4j\Types\Node;
use Vinelab\NeoEloquent\Connection;
use Vinelab\NeoEloquent\Query\Grammars\Grammar;
use Vinelab\NeoEloquent\Traits\ResultTrait;

class Builder extends IlluminateQueryBuilder
{
    use ResultTrait;

    /**
     * The database active client handler.
     *
     * @var Neoxygen\NeoClient\Client
     */
    protected $client;

    /**
     * The matches constraints for the query.
     *
     * @var array
     */
    public $matches = [];

    /**
     * The WITH parts of the query.
     *
     * @var array
     */
    public $with = [];

    /**
     * The current query value bindings.
     *
     * @var array
     */
    public $bindings = [
        'matches' => [],
        'select' => [],
        'join' => [],
        'where' => [],
        'having' => [],
        'order' => [],
    ];

    /**
     * All of the available clause operators.
     *
     * @var array
     */
    public $operators = [
        '+', '-', '*', '/', '%', '^',    // Mathematical
        '=', '<>', '<', '>', '<=', '>=', // Comparison
        'is null', 'is not null',
        'and', 'or', 'xor', 'not',       // Boolean
        'in', '[x]', '[x .. y]',         // Collection
        '=~',                             // Regular Expression
    ];

    /**
     * An aggregate function and column to be run.
     *
     * @var array
     */
    public $aggregate;

    /**
     * The columns that should be returned.
     *
     * @var array
     */
    public $columns;

    /**
     * Indicates if the query returns distinct results.
     *
     * @var bool
     */
    public $distinct = false;

    /**
     * The table which the query is targeting.
     *
     * @var string
     */
    public $from;

    /**
     * The where constraints for the query.
     *
     * @var array
     */
    public $wheres;

    /**
     * The groupings for the query.
     *
     * @var array
     */
    public $groups;

    /**
     * The having constraints for the query.
     *
     * @var array
     */
    public $havings;

    /**
     * The orderings for the query.
     *
     * @var array
     */
    public $orders;

    /**
     * The maximum number of records to return.
     *
     * @var int
     */
    public $limit;

    /**
     * The number of records to skip.
     *
     * @var int
     */
    public $offset;

    /**
     * The query union statements.
     *
     * @var array
     */
    public $unions;

    /**
     * The maximum number of union records to return.
     *
     * @var int
     */
    public $unionLimit;

    /**
     * The number of union records to skip.
     *
     * @var int
     */
    public $unionOffset;

    /**
     * The orderings for the union query.
     *
     * @var array
     */
    public $unionOrders;

    /**
     * Indicates whether row locking is being used.
     *
     * @var string|bool
     */
    public $lock;

    /**
     * The field backups currently in use.
     *
     * @var array
     */
    protected $backups = [];

    /**
     * The binding backups currently in use.
     *
     * @var array
     */
    protected $bindingBackups = [];

    /**
     * Create a new query builder instance.
     *
     * @param ConnectionInterface                  $connection
     * @param \Illuminate\Database\Query\Grammars\Grammar     $grammar
     * @param \Illuminate\Database\Query\Processors\Processor $processor
     *
     * @return void
     */
    public function __construct(ConnectionInterface $connection, Grammar $grammar, IlluminateProcessor $processor)
    {
        $this->grammar = $grammar;
        $this->grammar->setQuery($this);
        $this->processor = $processor;

        $this->connection = $connection;

        $this->client = $connection->getClient();
    }

    /**
     * Set the node's label which the query is targeting.
     *
     * @param string $label
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function from($label, $as = null)
    {
        $this->from = $label;

        // $as is used only for implementation purposes
        // by the original Builder contract.

        return $this;
    }

    /**
     * Insert a new record and get the value of the primary key.
     *
     * @param array  $values
     * @param string $sequence
     *
     * @return int
     */
    public function insertGetId(array $values, $sequence = null)
    {
        $cypher = $this->grammar->compileCreate($this, $values);

        $bindings = $this->getBindingsMergedWithValues($values);

        /** @var CypherList $results */
        $results = $this->connection->insert($cypher, $bindings);

        /** @var Node $node */
        $node = $results->first()->first()->getValue();

        return $node->getId();
    }

    /**
     * Update a record in the database.
     *
     * @param array $values
     *
     * @return int
     */
    public function update(array $values)
    {
        $cypher = $this->grammar->compileUpdate($this, $values);

        $bindings = $this->getBindingsMergedWithValues($values, true);

        $updated = collect($this->connection->update($cypher, $bindings)->getResult());

        return ($updated) ? count(current($this->getRecordsByPlaceholders($updated))) : 0;
    }

    /**
     *  Bindings should have the keys postfixed with _update as used
     *  in the CypherGrammar so that we differentiate them from
     *  query bindings avoiding clashing values.
     *
     * @param array $values
     *
     * @return array
     */
    protected function getBindingsMergedWithValues(array $values, $updating = false)
    {
        $bindings = [];

        $values = $this->getGrammar()->postfixValues($values, $updating);

        foreach ($values as $key => $value) {
            $bindings[$key] = $value;
        }

        return array_merge($this->getBindings(), $bindings);
    }

    /**
     * Get the current query value bindings in a flattened array
     * of $key => $value.
     *
     * @return array
     */
    public function getBindings()
    {
        $bindings = [];

        // We will run through all the bindings and pluck out
        // the component (select, where, etc.)
        foreach ($this->bindings as $component => $binding) {
            if (! empty($binding)) {
                // For every binding there could be multiple
                // values set so we need to add all of them as
                // flat $key => $value item in our $bindings.
                foreach ($binding as $key => $value) {
                    $bindings[$key] = $value;
                }
            }
        }

        return $bindings;
    }

    /**
     * Add a basic where clause to the query.
     *
     * @param string $column
     * @param string $operator
     * @param mixed  $value
     * @param string $boolean
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     *
     * @throws \InvalidArgumentException
     */
    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        // First we check whether the operator is 'IN' so that we call whereIn() on it
        // as a helping hand and centralization strategy, whereIn knows what to do with the IN operator.
        if (mb_strtolower($operator) == 'in') {
            return $this->whereIn($column, $value, $boolean);
        }

        // If the column is an array, we will assume it is an array of key-value pairs
        // and can add them each as a where clause. We will maintain the boolean we
        // received when the method was called and pass it into the nested where.
        if (is_array($column)) {
            return $this->whereNested(function (self $query) use ($column) {
                foreach ($column as $key => $value) {
                    $query->where($key, '=', $value);
                }
            }, $boolean);
        }

        if (func_num_args() == 2) {
            list($value, $operator) = [$operator, '='];
        } elseif ($this->invalidOperatorAndValue($operator, $value)) {
            throw new \InvalidArgumentException('Value must be provided.');
        }

        // If the columns is actually a Closure instance, we will assume the developer
        // wants to begin a nested where statement which is wrapped in parenthesis.
        // We'll add that Closure to the query then return back out immediately.
        if ($column instanceof Closure) {
            return $this->whereNested($column, $boolean);
        }

        // If the given operator is not found in the list of valid operators we will
        // assume that the developer is just short-cutting the '=' operators and
        // we will set the operators to '=' and set the values appropriately.
        if (! in_array(mb_strtolower($operator), $this->operators, true)) {
            list($value, $operator) = [$operator, '='];
        }

        // If the value is a Closure, it means the developer is performing an entire
        // sub-select within the query and we will need to compile the sub-select
        // within the where clause to get the appropriate query record results.
        if ($value instanceof Closure) {
            return $this->whereSub($column, $operator, $value, $boolean);
        }

        // If the value is "null", we will just assume the developer wants to add a
        // where null clause to the query. So, we will allow a short-cut here to
        // that method for convenience so the developer doesn't have to check.
        if (null === $value) {
            return $this->whereNull($column, $boolean, $operator != '=');
        }

        // Now that we are working with just a simple query we can put the elements
        // in our array and add the query binding to our array of bindings that
        // will be bound to each SQL statements when it is finally executed.
        $type = 'Basic';

        $property = $column;

        // When the column is an id we need to treat it as a graph db id and transform it
        // into the form of id(n) and the typecast the value into int.
        if ($column == 'id') {
            $column = 'id('.$this->modelAsNode().')';
            $value = intval($value);
        }
        // When it's been already passed in the form of NodeLabel.id we'll have to
        // re-format it into id(NodeLabel)
        elseif (preg_match('/^.*\.id$/', $column)) {
            $parts = explode('.', $column);
            $column = sprintf('%s(%s)', $parts[1], $parts[0]);
            $value = intval($value);
        }
        // Also if the $column is already a form of id(n) we'd have to type-cast the value into int.
        elseif (preg_match('/^id\(.*\)$/', $column)) {
            $value = intval($value);
        }

        $binding = $this->prepareBindingColumn($column);

        $this->wheres[] = compact('type', 'binding', 'column', 'operator', 'value', 'boolean');

        $property = $this->wrap($binding);

        if (! $value instanceof Expression) {
            $this->addBinding([$property => $value], 'where');
        }

        return $this;
    }

    /**
     * Increment the value of an existing column on a where clause.
     * Used to allow querying on the same attribute with different values.
     *
     * @param string $column
     *
     * @return string
     */
    protected function prepareBindingColumn($column)
    {
        $count = $this->columnCountForWhereClause($column);

        $binding = ($count > 0) ? $column.'_'.($count + 1) : $column;

        $prefix = $this->from;
        if (is_array($prefix)) {
            $prefix = implode('_', $prefix);
        }

        // we prefix when we do have a prefix ($this->from) and when the column isn't an id (id(abc..)).
        $prefix = (! preg_match('/id([a-zA-Z0-9]?)/', $column) && ! empty($this->from)) ? mb_strtolower($prefix) : '';

        return $prefix.$binding;
    }

//    /**
//     * Execute the query as a "select" statement.
//     *
//     * @param array $columns
//     *
//     * @return array|static[]
//     */
//    public function get($columns = ['*'])
//    {
//        return $this->getFresh($columns);
//    }

    /**
     * Get the count of the total records for the paginator.
     *
     * @param array $columns
     *
     * @return int
     */
    public function getCountForPagination($columns = ['*'])
    {
        $this->backupFieldsForCount();

        $this->aggregate = ['function' => 'count', 'columns' => $columns];

        $results = $this->get();

        $this->aggregate = null;

        $this->restoreFieldsForCount();

        if (isset($this->groups)) {
            return count($results);
        }

        return isset($results[0]) ? (int) array_change_key_case((array) $results[0])['aggregate'] : 0;
    }

    /**
     * Backup some fields for the pagination count.
     */
    protected function backupFieldsForCount()
    {
        foreach (['orders', 'limit', 'offset', 'columns'] as $field) {
            $this->backups[$field] = $this->{$field};

            $this->{$field} = null;
        }

        foreach (['order', 'select'] as $key) {
            $this->bindingBackups[$key] = $this->bindings[$key];

            $this->bindings[$key] = [];
        }
    }

    /**
     * Restore some fields after the pagination count.
     */
    protected function restoreFieldsForCount()
    {
        foreach (['orders', 'limit', 'offset', 'columns'] as $field) {
            $this->{$field} = $this->backups[$field];
        }

        foreach (['order', 'select'] as $key) {
            $this->bindings[$key] = $this->bindingBackups[$key];
        }

        $this->backups = [];
        $this->bindingBackups = [];
    }

    /**
     * Delete a record from the database.
     *
     * @param mixed $id
     *
     * @return int
     */
    public function delete($id = null)
    {
        // If an ID is passed to the method, we will set the where clause to check
        // the ID to allow developers to simply and quickly remove a single row
        // from their database without manually specifying the where clauses.
        if (null !== $id) {
            $this->where('id', '=', $id);
        }

        $cypher = $this->grammar->compileDelete($this);

        $result = $this->connection->delete($cypher, $this->getBindings());

        if ($result instanceof Result) {
            $result = true;
        }

        return $result;
    }

    /**
     * Get the number of occurrences of a column in where clauses.
     *
     * @param string $column
     *
     * @return int
     */
    protected function columnCountForWhereClause($column)
    {
        if (is_array($this->wheres)) {
            return count(array_filter($this->wheres, function ($where) use ($column) {
                return $where['column'] == $column;
            }));
        }
    }

    /**
     * Add a "where in" clause to the query.
     *
     * @param string $column
     * @param mixed  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function whereIn($column, $values, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotIn' : 'In';

        // If the value of the where in clause is actually a Closure, we will assume that
        // the developer is using a full sub-select for this "in" statement, and will
        // execute those Closures, then we can re-construct the entire sub-selects.
        if ($values instanceof Closure) {
            return $this->whereInSub($column, $values, $boolean, $not);
        }

        if ($values instanceof Arrayable) {
            $values = $values->toArray();
        }

        $property = $column;

        if ($column == 'id') {
            $column = 'id('.$this->modelAsNode().')';
        }

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        $property = $this->wrap($property);

        $this->addBinding([$property => $values], 'where');

        return $this;
    }

    /**
     * Add a where between statement to the query.
     *
     * @param string $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function whereBetween($column, array $values, $boolean = 'and', $not = false)
    {
        $type = 'between';

        $property = $column;

        if ($column == 'id') {
            $column = 'id('.$this->modelAsNode().')';
        }

        $this->wheres[] = compact('column', 'type', 'boolean', 'not');

        $this->addBinding([$property => $values], 'where');

        return $this;
    }

    /**
     * Add a "where null" clause to the query.
     *
     * @param string $column
     * @param string $boolean
     * @param bool   $not
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function whereNull($column, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotNull' : 'Null';

        if ($column == 'id') {
            $column = 'id('.$this->modelAsNode().')';
        }

        $binding = $this->prepareBindingColumn($column);

        $this->wheres[] = compact('type', 'column', 'boolean', 'binding');

        return $this;
    }

    /**
     * Add a WHERE statement with carried identifier to the query.
     *
     * @param string $column
     * @param string $operator
     * @param string $value
     * @param string $boolean
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function whereCarried($column, $operator = null, $value = null, $boolean = 'and')
    {
        $type = 'Carried';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        return $this;
    }

    /**
     * Add a WITH clause to the query.
     *
     * @param array $parts
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function with(array $parts)
    {
        if ($this->isAssocArray($parts)) {
            foreach ($parts as $key => $part) {
                if (! in_array($part, $this->with)) {
                    $this->with[$key] = $part;
                }
            }
        } else {
            foreach ($parts as $part) {
                if (! in_array($part, $this->with)) {
                    $this->with[] = $part;
                }
            }
        }

        return $this;
    }

    /**
     * Insert a new record into the database.
     *
     * @param array $values
     *
     * @return bool
     */
    public function insert(array $values)
    {
        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient for building these
        // inserts statements by verifying the elements are actually an array.
        if (! is_array(reset($values))) {
            $values = [$values];
        }

        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient for building these
        // inserts statements by verifying the elements are actually an array.
        else {
            foreach ($values as $key => $value) {
                $value = $this->formatValue($value);
                ksort($value);
                $values[$key] = $value;
            }
        }

        // We'll treat every insert like a batch insert so we can easily insert each
        // of the records into the database consistently. This will make it much
        // easier on the grammars to just handle one type of record insertion.
        $bindings = [];

        foreach ($values as $record) {
            $bindings[] = $record;
        }

        $cypher = $this->grammar->compileInsert($this, $values);

        // Once we have compiled the insert statement's Cypher we can execute it on the
        // connection and return a result as a boolean success indicator as that
        // is the same type of result returned by the raw connection instance.
        $bindings = $this->cleanBindings($bindings);

        $results = $this->connection->insert($cypher, $bindings);

        return (bool) $results;
    }

    /**
     * Create a new node with related nodes with one database hit.
     *
     * @param array $model
     * @param array $related
     *
     * @return \Vinelab\NeoEloquent\Eloquent\Model
     */
    public function createWith(array $model, array $related)
    {
        $cypher = $this->grammar->compileCreateWith($this, compact('model', 'related'));

        // Indicate that we need the result returned as is.
        return $this->connection->statement($cypher, [], true);
    }

    /**
     * Execute the query as a fresh "select" statement.
     *
     * @param array $columns
     *
     * @return array|static[]
     */
    public function getFresh($columns = ['*'])
    {
        if (null === $this->columns) {
            $this->columns = $columns;
        }

        return $this->runSelect();
    }

    /**
     * Run the query as a "select" statement against the connection.
     *
     * @return array
     */
    protected function runSelect()
    {
        return $this->connection->select($this->toCypher(), $this->getBindings());
    }

    /**
     * Get the Cypher representation of the traversal.
     *
     * @return string
     */
    public function toCypher()
    {
        return $this->grammar->compileSelect($this);
    }

    /**
     * Add a relationship MATCH clause to the query.
     *
     * @param \Vinelab\NeoEloquent\Eloquent\Model $parent       The parent model of the relationship
     * @param \Vinelab\NeoEloquent\Eloquent\Model $related      The related model
     * @param string                              $relatedNode  The related node' placeholder
     * @param string                              $relationship The relationship title
     * @param string                              $property     The parent's property we are matching against
     * @param string                              $value
     * @param string                              $direction    Possible values are in, out and in-out
     * @param string                              $boolean      And, or operators
     *
     * @return \Vinelab\NeoEloquent\Query\Builder|static
     */
    public function matchRelation($parent, $related, $relatedNode, $relationship, $property, $value = null, $direction = 'out', $boolean = 'and')
    {
        $parentLabels = $parent->getTable();
        $relatedLabels = $related->getTable();
        $parentNode = $this->modelAsNode($parentLabels);

        $this->matches[] = [
            'type' => 'Relation',
            'optional' => $boolean,
            'property' => $property,
            'direction' => $direction,
            'relationship' => $relationship,
            'parent' => [
                'node' => $parentNode,
                'labels' => $parentLabels,
            ],
            'related' => [
                'node' => $relatedNode,
                'labels' => $relatedLabels,
            ],
        ];

        $this->addBinding([$this->wrap($property) => $value], 'matches');

        return $this;
    }

    public function matchMorphRelation($parent, $relatedNode, $property, $value = null, $direction = 'out', $boolean = 'and')
    {
        $parentLabels = $parent->getTable();
        $parentNode = $this->modelAsNode($parentLabels);

        $this->matches[] = [
            'type' => 'MorphTo',
            'optional' => 'and',
            'property' => $property,
            'direction' => $direction,
            'related' => ['node' => $relatedNode],
            'parent' => [
                'node' => $parentNode,
                'labels' => $parentLabels,
            ],
        ];

        $this->addBinding([$property => $value], 'matches');

        return $this;
    }

    /**
     * the percentile of a given value over a group,
     * with a percentile from 0.0 to 1.0.
     * It uses a rounding method, returning the nearest value to the percentile.
     *
     * @param string $column
     *
     * @return mixed
     */
    public function percentileDisc($column, $percentile = 0.0)
    {
        return $this->aggregate(__FUNCTION__, [$column], $percentile);
    }

    /**
     * Retrieve the percentile of a given value over a group,
     * with a percentile from 0.0 to 1.0. It uses a linear interpolation method,
     * calculating a weighted average between two values,
     * if the desired percentile lies between them.
     *
     * @param string $column
     *
     * @return mixed
     */
    public function percentileCont($column, $percentile = 0.0)
    {
        return $this->aggregate(__FUNCTION__, [$column], $percentile);
    }

    /**
     * Retrieve the standard deviation for a given column.
     *
     * @param string $column
     *
     * @return mixed
     */
    public function stdev($column)
    {
        return $this->aggregate(__FUNCTION__, [$column]);
    }

    /**
     * Retrieve the standard deviation of an entire group for a given column.
     *
     * @param string $column
     *
     * @return mixed
     */
    public function stdevp($column)
    {
        return $this->aggregate(__FUNCTION__, [$column]);
    }

    /**
     * Get the collected values of the give column.
     *
     * @param string $column
     *
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function collect($column)
    {
        $row = $this->aggregate(__FUNCTION__, [$column]);

        $collected = [];

        foreach ($row as $value) {
            $collected[] = $value;
        }

        return new Collection($collected);
    }

    /**
     * Get the count of the disctinct values of a given column.
     *
     * @param string $column
     *
     * @return int
     */
    public function countDistinct($column)
    {
        return (int) $this->aggregate(__FUNCTION__, [$column]);
    }

    /**
     * Execute an aggregate function on the database.
     *
     * @param string $function
     * @param array  $columns
     *
     * @return mixed
     */
    public function aggregate($function, $columns = ['*'], $percentile = null)
    {
        $this->aggregate = array_merge([
            'label' => $this->from,
        ], compact('function', 'columns', 'percentile'));

        $previousColumns = $this->columns;

        $results = $this->get($columns);

        // Once we have executed the query, we will reset the aggregate property so
        // that more select queries can be executed against the database without
        // the aggregate value getting in the way when the grammar builds it.
        $this->aggregate = null;

        $this->columns = $previousColumns;

        $values = $this->getRecordsByPlaceholders($results);

        $value = reset($values);
        if (is_array($value)) {
            return current($value);
        } else {
            return $value;
        }
    }

    /**
     * Add a binding to the query.
     *
     * @param mixed  $value
     * @param string $type
     *
     * @return \Vinelab\NeoEloquent\Query\Builder
     */
    public function addBinding($value, $type = 'where')
    {
        if (is_array($value)) {
            $key = array_keys($value)[0];

            if (mb_strpos($key, '.') !== false) {
                $binding = $value[$key];
                unset($value[$key]);
                $key = explode('.', $key)[1];
                $value[$key] = $binding;
            }
        }

        if (! array_key_exists($type, $this->bindings)) {
            throw new \InvalidArgumentException("Invalid binding type: {$type}.");
        }

        if (is_array($value)) {
            $this->bindings[$type] = array_merge($this->bindings[$type], $value);
        } else {
            $this->bindings[$type][] = $value;
        }

        return $this;
    }

    /**
     * Convert a string into a Neo4j Label.
     *
     * @param string $label
     *
     * @return Everyman\Neo4j\Label
     */
    public function makeLabel($label)
    {
        return $this->client->makeLabel($label);
    }

    /**
     * Tranfrom a model's name into a placeholder
     * for fetched properties. i.e.:.
     *
     * MATCH (user:`User`)... "user" is what this method returns
     * out of User (and other labels).
     * PS: It consideres the first value in $labels
     *
     * @param array $labels
     *
     * @return string
     */
    public function modelAsNode(array $labels = null)
    {
        $labels = (null !== $labels) ? $labels : $this->from;

        return $this->grammar->modelAsNode($labels);
    }

    /**
     * Merge an array of where clauses and bindings.
     *
     * @param array $wheres
     * @param array $bindings
     */
    public function mergeWheres($wheres, $bindings)
    {
        $this->wheres = array_merge((array) $this->wheres, (array) $wheres);

        $this->bindings['where'] = array_merge_recursive($this->bindings['where'], (array) $bindings);
    }

    public function wrap($property)
    {
        return $this->grammar->getIdReplacement($property);
    }

    /**
     * Get a new instance of the query builder.
     *
     * @return \Vinelab\NeoEloquent\Query\Builder
     */
    public function newQuery()
    {
        return new self($this->connection, $this->grammar, $this->processor);
    }

    /**
     * Fromat the value into its string representation.
     *
     * @param mixed $value
     *
     * @return string
     */
    protected function formatValue($value)
    {
        // If the value is a date we'll format it according to the specified
        // date format.
        if ($value instanceof DateTime || $value instanceof Carbon) {
            $value = $value->format($this->grammar->getDateFormat());
        }

        return $value;
    }

    /*
     * Add/Drop labels
     * @param $labels array array of strings(labels)
     * @param $operation string 'add' or 'drop'
     * @return bool true if success, otherwise false
     */
    public function updateLabels($labels, $operation = 'add')
    {
        $cypher = $this->grammar->compileUpdateLabels($this, $labels, $operation);

        $result = $this->connection->update($cypher, $this->getBindings());

        return (bool) $result;
    }

    public function getNodesCount($result)
    {
        return count($this->getNodeRecords($result));
    }

    /**
     * Handle dynamic method calls into the method.
     *
     * @param string $method
     * @param array  $parameters
     *
     * @return mixed
     *
     * @throws \BadMethodCallException
     */
    public function __call($method, $parameters)
    {
        if (Str::startsWith($method, 'where')) {
            return $this->dynamicWhere($method, $parameters);
        }

        $className = get_class($this);

        throw new BadMethodCallException("Call to undefined method {$className}::{$method}()");
    }

    /**
     * Determine whether an array is associative.
     *
     * @param array $array
     *
     * @return bool
     */
    protected function isAssocArray($array)
    {
        return is_array($array) && array_keys($array) !== range(0, count($array) - 1);
    }
}
