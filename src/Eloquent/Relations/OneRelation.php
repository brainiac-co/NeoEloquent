<?php

namespace Vinelab\NeoEloquent\Eloquent\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

abstract class OneRelation extends BelongsTo implements RelationInterface
{
    /**
     * The edge direction for this relationship.
     *
     * @var string
     */
    protected $edgeDirection = 'out';

    /**
     * Initialize the relation on a set of models.
     *
     * @param array  $models
     * @param string $relation
     *
     * @return array
     */
    public function initRelation(array $models, $relation)
    {
        foreach ($models as $model) {
            // In the case of fetching nested relations, we will get an array
            // with the first key being the model we need, and the other being
            // the related model so we'll just take the first model out of the array.
            if (is_array($model)) {
                $model = reset($model);
            }

            $model->setRelation($relation, null);
        }

        return $models;
    }

    /**
     * Get an instance of the Edge[In, Out, etc.] relationship.
     *
     * @param \Illuminate\Database\Eloquent\Model $model
     * @param array                               $attributes
     *
     * @return \Vinelab\NeoEloquent\Eloquent\Edges\Edge[In,Out, etc.]
     */
    abstract public function getEdge(Model $model = null, $attributes = []);

    /**
     * Get the direction of the edge for this relationship.
     *
     * @return string
     */
    public function getEdgeDirection()
    {
        return $this->edgeDirection;
    }

    /**
     * Associate the model instance to the given parent.
     *
     * @param \Illuminate\Database\Eloquent\Model $model
     *
     * @return \Vinelab\NeoEloquent\Eloquent\Edges\Edge
     */
    public function associate($model, $attributes = [])
    {
        /*
         * For associated models we will need to create a unique relationship
         * between the parent and the related model. In Cypher we can use the
         * MERGE clause to make sure that the relationship doesn't happen more than once.
         *
         * An example query would be like:
         *
         * HasOne:
         * -------
         *
         * MATCH (user:`User`), (phone:`Phone`)
         * WHERE id(user) = 10892 AND id(phone) = 98522
         * MERGE (user)-[rel:PHONE]-(phone)
         * RETURN rel;
         *
         * BelongsTo:
         * ---------
         *
         * MATCH (account:`Account`), (user:`User`)
         * WHERE id(account) = 10892 AND id(user) = 98522
         * MERGE (account)<-[rel:ACCOUNT]-(user)
         * RETURN rel;
         */

        // Set the relation on the model
        $this->parent->setRelation($this->relationName, $model);

        /*
         * Due to the fact that relationships in Graph are entities themselves
         * we will need to treat them as such and in this case what we're looking for is
         * a relationship with an INCOMING direction towards the parent node, in other words
         * it is a relationship with an edge incoming towards the $parent model and we call it
         * an "Edge" relationship.
         */
        $relation = $this->getEdge($model, $attributes);

        $relation->save();

        return $relation;
    }

    /**
     * Get the edge between the parent model and the given model or
     * the related model determined by the relation function name.
     *
     * @param \Illuminate\Database\Eloquent\Model $model
     *
     * @return \Vinelab\NeoEloquent\Eloquent\Edges\Edge[In,Out, etc.]
     */
    public function edge(Model $model = null)
    {
        return $this->getEdge($model)->current();
    }

    /**
     * Gather the keys from an array of related models.
     *
     * @param array $models
     *
     * @return array
     */
    protected function getEagerModelKeys(array $models)
    {
        $keys = [];

        /*
         * First we need to gather all of the keys from the parent models so we know what
         * to query for via the eager loading query. We will add them to an array then
         * execute a "where in" statement to gather up all of those related records.
         */
        foreach ($models as $model) {
            // In the case of fetching nested relations, we will get an array
            // with the first key being the model we need, and the other being
            // the related model so we'll just take the first model out of the array.
            if (is_array($model)) {
                $model = reset($model);
            }

            if (null !== ($value = $model->{$this->ownerKey})) {
                $keys[] = $value;
            }
        }

        /*
         * If there are no keys that were not null we will just return an empty array in
         * it so the query doesn't fail, but will not return any results, which should
         * be what this developer is expecting in a case where this happens to them.
         */
        if (count($keys) == 0) {
            return [];
        }

        return array_values(array_unique($keys));
    }

    /**
     * Match the eagerly loaded results to their parents.
     *
     * @param array                                    $models
     * @param \Illuminate\Database\Eloquent\Collection $results
     * @param string                                   $relation
     *
     * @return array
     */
    public function match(array $models, Collection $results, $relation)
    {
        // We will need the parent node placeholder so that we use it to extract related results.
        $parent = $this->query->getQuery()->modelAsNode($this->parent->getTable());

        /*
         * Looping into all the parents to match back onto their children using
         * the primary key to map them onto the correct instances, every single
         * result will be having both instances at each Collection item, held by their
         * node placeholder.
         */
        foreach ($models as $model) {
            $matched = $results->filter(function ($result) use ($parent, $model) {
                if ($result[$parent] instanceof Model) {
                    // In the case of fetching nested relations, we will get an array
                    // with the first key being the model we need, and the other being
                    // the related model so we'll just take the first model out of the array.
                    if (is_array($model)) {
                        $identifier = $this->determineValueIdentifier($model);
                        $model = $model[$identifier];
                    }

                    return $model->getKey() == $result[$parent]->getKey();
                }
            });

            // Now that we have the matched parents we know where to add the relations.
            // Sometimes we have more than a match so we gotta catch them all!
            foreach ($matched as $match) {
                // In the case of fetching nested relations, we will get an array
                // with the first key being the model we need, and the other being
                // the related model so we'll just take the first model out of the array.
                if (is_array($model)) {
                    $identifier = $this->determineValueIdentifier($model);
                    $model = $model[$identifier];
                }

                $model->setRelation($relation, $match[$relation]);
            }
        }

        return $models;
    }

    public function getRelationName()
    {
        return $this->relationName;
    }

    public function getRelationType()
    {
        return $this->foreignKey;
    }

    public function getParentNode()
    {
        return $this->query->getQuery()->modelAsNode($this->parent->getTable());
    }

    public function getRelatedNode()
    {
        return $this->query->getQuery()->modelAsNode($this->related->getTable());
    }

    /**
     * Returns the parent model.
     *
     * @return Model
     */
    public function getParent()
    {
        return $this->parent;
    }

    /**
     * Returns the related model.
     *
     * @return Model
     */
    public function getRelated()
    {
        return $this->related;
    }

    public function getLocalKey()
    {
        return $this->ownerKey;
    }

    public function getParentLocalKeyValue()
    {
        return $this->parent->{$this->ownerKey};
    }

    /**
     * When matching eager loaded data, we need to determine
     * which identifier should be used to set the related models to.
     * This is done by iterating the given models and checking for
     * the matching class between the result and this relation's
     * parent model. When there's a match, the identifier at which
     * the match occurred is returned.
     *
     * @param  array  $models
     *
     * @return string
     */
    protected function determineValueIdentifier(array $models)
    {
        foreach ($models as $resultIdentifier => $model) {
            if (get_class($this->parent) === get_class($model)) {
                return $resultIdentifier;
            }
        }
    }
}
