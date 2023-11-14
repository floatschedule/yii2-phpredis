<?php

namespace dcb9\redis;

use Yii;
use Exception;
use yii\di\Instance;

class Cache extends \yii\caching\Cache
{
    /**
     * @var Connection|string|array the Redis [[Connection]] object or the application component ID of the Redis [[Connection]].
     * This can also be an array that is used to create a redis [[Connection]] instance in case you do not want do configure
     * redis connection as an application component.
     * After the Cache object is created, if you want to change this property, you should only assign it
     * with a Redis [[Connection]] object.
     */
    public $redis = 'redis';

    public $silentFail = false;

    /**
     * Initializes the redis Cache component.
     * This method will initialize the [[redis]] property to make sure it refers to a valid redis connection.
     * @throws \yii\base\InvalidConfigException if [[redis]] is invalid.
     */
    public function init()
    {
        parent::init();
        $this->redis = Instance::ensure($this->redis, Connection::className());
    }


    /**
     * @inheritdoc
     */
    public function exists($key)
    {
        $key = $this->buildKey($key);

        try {
            $this->redis->open();
            return (bool)$this->redis->exists($key);
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function getValue($key)
    {
        try {
            $this->redis->open();
            return $this->redis->get($key);
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function getValues($keys)
    {
        $result = [];
        try {
            $this->redis->open();
            $response = $this->redis->mget($keys);
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
            return $result;
        }
        
        $i = 0;
        foreach ($keys as $key) {
            $result[$key] = $response[$i++];
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    protected function setValue($key, $value, $expire)
    {
        try {
            $this->redis->open();
            if ($expire == 0) {
                return (bool)$this->redis->set($key, $value);
            } else {
                return (bool)$this->redis->setEx($key, $expire, $value);
            }
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function setValues($data, $expire)
    {
        try {
            $this->redis->open();
            $failedKeys = [];
            if ($expire == 0) {
                $this->redis->mSet($data);
            } else {
                $expire = (int)$expire;
                $this->redis->multi();
                $this->redis->mSet($data);
                $index = [];
                foreach ($data as $key => $value) {
                    $this->redis->expire($key, $expire);
                    $index[] = $key;
                }
                $result = $this->redis->exec();
                array_shift($result);
                foreach ($result as $i => $r) {
                    if ($r != 1) {
                        $failedKeys[] = $index[$i];
                    }
                }
            }

            return $failedKeys;
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }

            return [];
        }
    }


    /**
     * @inheritdoc
     */
    protected function addValue($key, $value, $expire)
    {
        try {
            $this->redis->open();
            if ($expire == 0) {
                return (bool)$this->redis->setNx($key, $value);
            }
    
            return (bool)$this->redis->rawCommand('SET', $key, $value, 'EX', $expire, 'NX');
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
            return false;
        }
    }

    /**
     * @inheritdoc
     */
    protected function deleteValue($key)
    {
        try {
            $this->redis->open();
            return (bool)$this->redis->del($key);
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
            return false;
        }
    }

    /**
     * @inheritdoc
     */
    protected function flushValues()
    {
        try {
            $this->redis->open();
            return $this->redis->flushdb();
        } catch (Exception $e) {
            Yii::warning(
                __METHOD__ . ' Open Redis connection error:' . $e->getMessage()
            );

            if (!$this->silentFail) {
                throw $e;
            }
            return false;
        }
    }
}
