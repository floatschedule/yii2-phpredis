<?php

namespace dcb9\redis;

use Redis;
use Yii;
use yii\base\Configurable;
use RedisException;

/**
 * Class Connection
 * @package dcb9\redis
 */
class Connection extends Redis implements Configurable
{
    /**
     * @var string the hostname or ip address to use for connecting to the redis server. Defaults to 'localhost'.
     * If [[unixSocket]] is specified, hostname and port will be ignored.
     */
    public $hostname = 'localhost';
    /**
     * @var integer the port to use for connecting to the redis server. Default port is 6379.
     * If [[unixSocket]] is specified, hostname and port will be ignored.
     */
    public $port = 6379;
    /**
     * @var string the unix socket path (e.g. `/var/run/redis/redis.sock`) to use for connecting to the redis server.
     * This can be used instead of [[hostname]] and [[port]] to connect to the server using a unix socket.
     * If a unix socket path is specified, [[hostname]] and [[port]] will be ignored.
     */
    public $unixSocket;
    /**
     * @var string the password for establishing DB connection. Defaults to null meaning no AUTH command is send.
     * See http://redis.io/commands/auth
     */
    public $password;
    /**
     * @var integer the redis database to use. This is an integer value starting from 0. Defaults to 0.
     */
    public $database = 0;
    /**
     * @var float value in seconds (optional, default is 0.0 meaning unlimited)
     */
    public $connectionTimeout = 0.0;
    /**
     * @var float value in seconds (optional, default is 0.0 meaning unlimited)
     */
    public $readTimeout = 0.0;

    /**
     * Retry interval - value in ms
     *
     * @var integer
     */
    public $retryInterval = 0;

    /**
     * Number of retries.
     *
     * @var integer
     */
    public $retries       = 0;
    /**
     * Constructor.
     * The default implementation does two things:
     *
     * - Initializes the object with the given configuration `$config`.
     * - Call [[init()]].
     *
     * If this method is overridden in a child class, it is recommended that
     *
     * - the last parameter of the constructor is a configuration array, like `$config` here.
     * - call the parent implementation at the end of the constructor.
     *
     * @param array $config name-value pairs that will be used to initialize the object properties
     */
    public function __construct($config = [])
    {
        if (!empty($config)) {
            Yii::configure($this, $config);
        }
    }

    private $multiExecCommand   = false;
    private $multiCommands      = [];
    private $redisRetryCommands = [
        'retrySet'      => 'SET',
        'retryGet'      => 'GET',
        'retryKeys'     => 'KEYS',
        'retryDel'      => 'DEL',
        'retryTTL'      => 'TTL',
        'retryMulti'    => 'MULTI',
        'retrySAdd'     => 'SADD',
        'retryExec'     => 'EXEC',
        'retryExpire'   => 'EXPIRE',
        'retrySMembers' => 'sMembers',
        'retryUnlink'   => 'UNLINK',
        'retryPublish'  => 'PUBLISH',
    ];

    /**
     * Allows issuing all supported commands via magic methods.
     *
     * @param string $command - name of the missing method to execute
     * @param array  $params  - method call arguments
     * 
     * @return mixed
     */
    public function __call($command, $params)
    {
        if (in_array($command, array_keys($this->redisRetryCommands))) {
            if ($command === 'retryMulti') {
                $this->multiExecCommand = true;
                $this->multiCommands    = [];
                $this->multiCommands[]  = $this->computeRawCommand($command, $params);
                return $this;
            }
            if ($this->multiExecCommand === true && $command !== 'retryExec') {
                // Chain all commands between retryMulti and retryExec.
                $this->multiCommands[]  = $this->computeRawCommand($command, $params);
                return $this;
            }
            if ($this->multiExecCommand === true && $command === 'retryExec') {
                $this->multiCommands[]  = $this->computeRawCommand($command, $params);
                $this->multiExecCommand = false;
                $responseMultiCommand   = $this->executeMultiCommand($this->multiCommands);
                $this->multiCommands    = [];
                return $responseMultiCommand;
            }
            if ($this->multiExecCommand === false) {
                return $this->executeCommand($this->computeRawCommand($command, $params));
            }
        }

        return parent::__call($command, $params);
    }

    private function computeRawCommand($command, $params)
    {
        $completeCommand   = [];
        $completeCommand[] = $this->redisRetryCommands[$command];
        foreach ($params as $param) {
            $completeCommand[] = $param;
        }
        return $completeCommand;
    }

    /**
     * Returns the fully qualified name of this class.
     * @return string the fully qualified name of this class.
     */
    public static function className()
    {
        return get_called_class();
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws RedisException if connection fails
     * @example 问题详细描述 https://bugs.php.net/bug.php?id=46851  php_redis.so 版本为4.0.2时会出现一条警告
     * @see connect()
     * @param string    $host
     * @param int       $port
     * @param float     $timeout
     * @param int       $retry_interval
     * @return bool
     */
    public function open( $host = null, $port = null, $timeout = null, $retry_interval = 0 )
    {
        if ($this->unixSocket !== null) {
            $isConnected = $this->connect($this->unixSocket);
        } else {
            if(is_null($host)){
                $host = $this->hostname;
            }
            if(is_null($port)){
                $port = $this->port;
            }
            if(is_null($timeout)){
                $timeout = $this->connectionTimeout;
            }
            
            $isConnected = $this->connect($host, $port, $timeout, null, $retry_interval, $this->readTimeout);
        }

        if ($isConnected === false) {
            throw new RedisException('Connect to redis server error.');
        }

        if ($this->password !== null) {
            $this->auth($this->password);
        }

        if ($this->database !== null) {
            $this->select($this->database);
        }
    }

    /**
     * @return bool
     */
    public function ping()
    {
        return parent::ping() === '+PONG';
    }

    public function flushDB($async = null)
    {
        return parent::flushDB($async);
    }

    /**
     * Execute Multi exec command.
     *
     * @param array $commands - Command to run on Redis.
     * 
     * @return mixed
     */
    public function executeMultiCommand(array $commands = [])
    {
        // Run Redis command with retries.
        if ($this->retries > 0) {
            return $this->retryExecuteMultiCommand($commands);
        }

        return $this->executeMultiCommandOnce($commands);
    }

    /**
     * Execute Redis command once. Will throw an Redis Exception in case there is a connection error.
     *
     * @param array $commands - Command to run on Redis.
     * 
     * @return mixed
     */
    protected function executeMultiCommandOnce(array $commands = [])
    {
        $this->open();
        $responses = [];
        foreach ($commands as $i => $command) {
            $responses[$i] = $this->sendRawCommand($command);
        }
        return $responses;
    }

    /**
     * Execute Redis MULTI command with retry and retry interval.
     * No Exception is thrown in case there is a connection error.
     *
     * @param array $commands - Command (multi dimensional array of commands).
     *
     * @return mixed
     */
    protected function retryExecuteMultiCommand(array $commands = [])
    {
        $tries = $this->retries;
        while ($tries-- > 0) {
            try {
                // Try to open a Redis connection and execute the command.
                return $this->executeMultiCommandOnce($commands);
            } catch (RedisException $exception) {
                // Log any exception with Yii error.
                Yii::error($exception, __METHOD__);
                // In case Redis is not accessible, close the connection, wait for the retry interval.
                $this->close();
                if ($this->retryInterval > 0) {
                    usleep($this->retryInterval * 1000);
                }
            }
        }
    }

    /**
     * Execute Redis command.
     *
     * @param array $command - Command to run on Redis.
     * 
     * @return mixed
     */
    public function executeCommand(array $command = [])
    {
        // Run Redis command with retries.
        if ($this->retries > 0) {
            return $this->retryExecuteCommand($command);
        }

        return $this->executeCommandOnce($command);
    }

    /**
     * Execute Redis command once. Will throw an Redis Exception in case there is a connection error.
     *
     * @param array $command - Command to run on Redis.
     * 
     * @return mixed
     */
    protected function executeCommandOnce(array $command = [])
    {
        $this->open();
        return $this->sendRawCommand($command);
    }

    /**
     * Execute Redis command with retry and retry interval. No Exception is thrown in case there is a connection error.
     *
     * @param array $command - Command.
     *
     * @return mixed
     */
    protected function retryExecuteCommand(array $command = [])
    {
        $tries = $this->retries;
        while ($tries-- > 0) {
            try {
                // Try to open a Redis connection and execute the command.
                return $this->executeCommandOnce($command);
            } catch (RedisException $exception) {
                // Log any exception with Yii error.
                Yii::error($exception, __METHOD__);
                // In case Redis is not accessible, close the connection, wait for the retry interval.
                $this->close();
                if ($this->retryInterval > 0) {
                    usleep($this->retryInterval * 1000);
                }
            }
        }
    }

    /**
     * Run Redis rawCommand with function array.
     *
     * @param array $command - Command
     *
     * @return void
     */
    protected function sendRawCommand(array $command)
    {
        $response = call_user_func_array(array($this, 'rawCommand'), $command);
        return $response;
    }
}
