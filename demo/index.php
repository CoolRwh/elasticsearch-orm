<?php


use Coolr\ElastocSearchOrm\Model\ElasticSearchModel;

include "../vendor/autoload.php";


class SkuModel extends ElasticSearchModel
{

    protected $index ="sku";

    /**
     * @return $this
     */
    public function shouldSome()
    {
      return  $this->addMust(
            [
                'bool' => [
                    'minimum_should_match' => 1,
                    'should'               => [
                        $this->wildcard('name1','1111'),
                        $this->wildcard('name2','1111'),
                    ],
                ],
            ]
        );
    }

}


$res = SkuModel::query()->where('id',time())->shouldSome()->toBody();

echo index.phpjson_encode($res).PHP_EOL;



