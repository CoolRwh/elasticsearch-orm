<?php

namespace Coolr\ElastocSearchOrm\Query;

use Elasticsearch\Common\Exceptions\Missing404Exception;

class Builder
{
    protected $index = 'test';

    protected $type = '_doc';

    protected $primaryKey = 'id';

    protected $connection = 'default';

    protected $params = [
        'type'  => '',//类型
        'index' => '',
        'body'  => [
            '_source' => [],
            "query"   => [
                "bool" => [
                    'filter'   => [],
                    'must'     => [],
                    'should'   => [],
                    'must_not' => []
                ],
            ],
            'sort'    => []
        ],
    ];

    protected $indexBody = [];

    protected $operators = [
        "="   => "=",
        "<"   => 'lt',
        "<="  => 'lte',
        ">"   => 'gt',
        ">="  => 'gte',
        "!="  => 'ne',
        "<>"  => 'ne',
        "in"  => 'in',
        "nin" => 'nin',
    ];


    public function getIndex(): string
    {
        return $this->index;
    }


    public function getType(): string
    {
        return $this->type;
    }


    public function getPrimaryKey(): string
    {
        return $this->primaryKey;
    }


    public static function query(): Builder
    {
        return (new static());
    }


    public function where(string $column, $operator = null, $value = null, bool $score = false): Builder
    {
        switch (func_num_args()) {
            case 1:
                break;
            case 2:
                $this->setQueryByOperator('=', $column, $operator);
                break;
            case 3:
                $this->setQueryByOperator($operator, $column, $value);
                break;
            case 4:
                $this->setQueryByOperator($operator, $column, $value, $score);
                break;
        }
        return $this;
    }


    protected function setQueryByOperator($operator, string $column, $value, bool $score = false): Builder
    {
        switch ($operator) {
            case "=":
                if ($score) {
                    $this->addMust($this->term($column, $value));
                } else {
                    $this->addFilter($this->term($column, $value));
                }
                break;
            case ">":
            case ">=":
            case "<":
            case "<=":
                $this->addFilter(['range' => [$column => [$this->operators[$operator] => $value]]]);
                break;
            case "!=":
            case "<>":
                $this->addMustNot($this->term($column, $value));
                break;
            case "in":
                if ($score) {
                    $this->addMust($this->terms($column, $value));
                } else {
                    $this->addFilter($this->terms($column, $value));
                }
                break;
            case "nin":
                $this->addMustNot($this->terms($column, $value));
                break;
        }
        return $this;
    }


    protected function addMust(array $data): Builder
    {
        $this->params['body']['query']['bool']['must'][] = $data;

        return $this;
    }

    protected function addFilter(array $data): Builder
    {
        $this->params['body']['query']['bool']['filter'][] = $data;
        return $this;
    }

    protected function addMustNot(array $data): Builder
    {
        $this->params['body']['query']['bool']['must_not'][] = $data;
        return $this;
    }

    protected function addShould(array $data): Builder
    {
        $this->params['body']['query']['bool']['should'][] = $data;
        return $this;
    }

    /**
     * @param  string  $column
     * @param  array  $values
     * @param  bool  $score
     * @return $this
     */
    public function whereIn(string $column, array $values = [], bool $score = false): Builder
    {
        return $this->setQueryByOperator('in', $column, $values, $score);

    }

    /**
     * @param  string  $column
     * @param  array  $values
     * @param  bool  $score
     * @return $this
     */
    public function whereNotIn(string $column, array $values = [], bool $score = false): Builder
    {
        return $this->setQueryByOperator('nin', $column, $values, $score);
    }

    public function whereRow(array $data): Builder
    {
        $this->params = array_merge($this->params, $data);
        return $this;
    }

    public function whereFilterRow(array $data): Builder
    {
        return $this->addFilter($data);
    }

    public function whereNotExists(string $column): Builder
    {
        return $this->whereExists($column, true);
    }

    public function whereExists(string $column, $not = false): Builder
    {
        $data = ['exists' => ['field' => $column]];
        return $not === true ? $this->addMustNot($data) : $this->addMust($data);
    }


    /**
     * @param  string  $column
     * @param  array  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereBetween(string $column, array $values, string $boolean = 'and', bool $not = false): Builder
    {
        $range = [
            'range' => [
                $column => [
                    $this->operators[">="] => $values[0],
                    $this->operators["<="] => $values[1]
                ]

            ]
        ];
        return $not === true ? $this->addMustNot($range) : $this->addFilter($range);
    }

    /**
     * @param  string  $column
     * @param  array  $values
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotBetween(string $column, array $values, string $boolean = 'and'): Builder
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    public function whereMustRow(array $data): Builder
    {
        return $this->addMust($data);
    }

    public function whereMustNotRow(array $data): Builder
    {

        return $this->addMustNot($data);
    }

    public function whereShouldRow(array $data): Builder
    {
        return $this->addShould($data);
    }

    public function size(int $size = 10): Builder
    {
        $this->params = array_merge($this->params, ['size' => $size]);
        return $this;
    }


    public function select(array $columns = [])
    {
        $this->params['body']['_source'] = $columns;
        return $this;
    }

    public function orderBy(string $column, string $direction)
    {
        $this->params['body']['sort'][] = [$column => ['order' => strtolower($direction) === 'asc' ? 'asc' : 'desc']];
        return $this;
    }

    /**
     * 根据 返回 字段长度 排序
     * @param  string  $column
     * @param  string  $direction
     * @return $this
     */
    public function orderByValueLength(string $column, string $direction = 'asc')
    {
        $this->params['body']['sort'][] = [
            '_script' => [
                'script' => [
                    'source' => 'doc["'.$column.'"].size()>0 ?doc["'.$column.'"].value.length():0',
                    'lang'   => 'painless',
                ],
                'type'   => 'number',
                'order'  => strtolower($direction) === 'asc' ? 'asc' : 'desc'
            ],
        ];;
        return $this;
    }

    public function bodySort(array $sort)
    {
        $this->params['body']['sort'][] = $sort;
        return $this;
    }


    public function page(int $page = 1, int $size = 20)
    {
        $page = ($page - 1) * $size;
        $this->params = array_merge($this->params, ['from' => $page, 'size' => $size]);
        return $this->search();
    }


    public function aggs(array $data)
    {
        if (empty($this->params['body']['aggs'])) {
            $this->params['body']['aggs'] = [];
        }
        $this->params['body']['aggs'] = array_merge($this->params['body']['aggs'], $data);
        return $this;
    }

    public function aggsSum(string $column, string $as = '')
    {
        if (!isset($this->params['aggs'])) {
            $this->params['aggs'] = [];
        }
        $this->params['aggs'] = array_merge($this->params['aggs'], $this->makeAggsSum($column, $as));
        return $this;
    }

    public function aggsCount(string $column, string $as = '')
    {
        if (!isset($this->params['aggs'])) {
            $this->params['aggs'] = [];
        }
        $this->params['aggs'] = array_merge($this->params['aggs'], $this->makeAggsCount($column, $as));
        return $this;
    }

    public function makeAggsSum($column, $as): array
    {
        return [empty($as) ? $column : $as => ['sum' => ['field' => $column]]];
    }

    public function makeAggsCount($column, $as): array
    {
        return [empty($as) ? $column : $as => ['cardinality' => ['field' => $column]]];
    }

    /**字段值存在*/
    public function existsValue($column)
    {
        return $this->addMust(['exists' => ['field' => $column]]);
    }

    /**字段值不存在*/
    public function notExistsValue($column)
    {
        return $this->addMustNot(['exists' => ['field' => $column]]);
    }


    /**
     * @param $column
     * @param $value
     * @return $this
     */
    public function like($column, $value): Builder
    {
        $should[] = ['wildcard' => [$column => $value]];
        return $this->addMust(['bool' => ['should' => $should]]);
    }


    /**
     * @param  array  $data  ['列名称1'=>'*值1*','列名称2'=>'*值2*']
     * @param  int  $limit  最小 匹配数量
     * @return $this
     */
    public function likes(array $data = [], int $limit = 1)
    {
        $should = [];
        foreach ($data as $column => $value) {
            $should[] = ['wildcard' => [$column => $value]];
        }
        return $this->addMust([
            'bool' => [
                'minimum_should_match' => $limit,
                'should'               => $should
            ]]);
    }

    public function should(array $data = [], int $limit = 1, $score = false): Builder
    {
        $should = [
            'bool' => [
                'minimum_should_match' => $limit,
                'should'               => []
            ]];
        foreach ($data as $column => $value) {
            $should['bool']['should'][] = is_array($value) ? $this->terms($column, $value) : $this->term($column,
                $value);
        }

        return $score ? $this->addFilter($score) : $this->addMust($should);
    }


    public function count()
    {
        $body = $this->toBody();
        $body['index'] = $this->index;
        $body['type'] = $this->type;
        return $this->client()->count($body);
    }


    public function term(string $column, $value): array
    {
        return ['term' => [$column => $value]];
    }

    public function terms(string $column, array $value): array
    {
        return ['terms' => [$column => $value]];
    }

    public  function wildcard(string $field, $value): array
    {
        return ['wildcard' => [$field => $value]];
    }

    public  function match(string $field, $value): array
    {
        return ['match' => [$field => $value]];
    }


    /**
     * @param $id
     * @return array|callable|mixed
     */
    public function deleteById($id)
    {
        $body = [];
        $body['index'] = $this->index;
        $body['type'] = $this->type;
        $body['id'] = $id;
        try {
            return $this->client()->delete($body);
        } catch (\Exception $e) {
            if ($e instanceof Missing404Exception) {
                return json_decode($e->getMessage(), true) ?? array_merge($body, ['result' => 'not_found']);
            }
            throw new \RuntimeException($e->getMessage(), $e->getCode());
        }
    }


    public function delete()
    {
        $body = $this->toBody();
        $body['index'] = $this->index;
        $body['type'] = $this->type;
        if (empty($body['body'])) {
            throw new \RuntimeException("条件不能为空！");
        }

        $checkNull = (empty($body['body']['query']['bool']['filter'])
            && empty($body['body']['query']['bool']['must'])
            && empty($body['body']['query']['bool']['should'])
            && empty($body['body']['query']['bool']['must_not']));
        if ($checkNull) {
            throw new \RuntimeException("条件不能为空！");
        }
        return $this->client()->deleteByQuery($body);
    }


    public function index($id, $body)
    {
        return $this->client()->index([
            'index' => $this->index,
            'id'    => $id,
            'type'  => $this->type,
            'body'  => $body]);
    }

    public function create($id, $body)
    {
        return $this->client()->create([
            'index' => $this->index,
            'id'    => $id,
            'type'  => $this->type,
            'body'  => $body]);
    }

    public function update($id, $body)
    {
        return $this->client()->update([
            'index' => $this->index,
            'id'    => $id,
            'type'  => $this->type,
            'body'  => $body]);
    }

    public function updateByQuery($body)
    {
        return $this->client()->updateByQuery([
            'index' => $this->index,
            'type'  => $this->type,
            'body'  => $body]);
    }

    public function search()
    {
        $body = $this->toBody();
        $body['index'] = $this->index;
        $body['type'] = $this->type;
        return $this->client()->search($body);
    }

    public function getClientConfig()
    {
        $config = [
            'default' => ['hosts' => [], 'username' => "", 'password' => ""]
        ];
        return $config[$this->connection];
    }

    /**
     * @return mixed
     */
    public function client()
    {
        $config = empty($config) ? $this->getClientConfig() : $config;
        $hosts = explode(',', $config['hosts']);
        $username = $config['username'];
        $password = $config['password'];
        return ESClientBuilder::create()->setHosts($hosts)->setBasicAuthentication($username, $password)->build();
    }

    public function toBody(): array
    {
        return array_merge($this->params, ['index' => $this->index, 'type' => $this->type]);
    }

}