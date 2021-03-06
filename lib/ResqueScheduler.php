<?php
/**
* ResqueScheduler core class to handle scheduling of jobs in the future.
*
* @package		ResqueScheduler
* @author		Chris Boulton <chris@bigcommerce.com>
* @copyright	(c) 2012 Chris Boulton
* @license		http://www.opensource.org/licenses/mit-license.php
*/
class ResqueScheduler
{
	const VERSION = "0.1.1";

  /**
   *
   * Accepts a new schedule configuration of the form:
   *
   *   {'some_name' => {"cron" => "5/* * * *",
   *                  "class" => "DoSomeWork",
   *                  "args" => "work on this string",
   *                  "description" => "this thing works it"s butter off"},
   *    ...}
   *
   * 'some_name' can be anything and is used only to describe and reference
   * the scheduled job
   *
   * :cron can be any cron scheduling string :job can be any resque job class
   *
   * :every can be used in lieu of :cron. see rufus-scheduler's 'every' usage
   * for valid syntax. If :cron is present it will take precedence over :every.
   *
   * :class must be a resque worker class
   *
   * :args can be any yaml which will be converted to a ruby literal and
   * passed in a params. (optional)
   *
   * :rails_envs is the list of envs where the job gets loaded. Envs are
   * comma separated (optional)
   *
   * :description is just that, a description of the job (optional). If
   * params is an array, each element in the array is passed as a separate
   * param, otherwise params is passed in as the only parameter to perform.
   */

    public static $schedules;

    /**  ¿ CONSTRUCT ?? */
    public static function schedule( $hash ){

        self::cleanSchedules();

        $scheduleHash = self::prepareSchedule($hash);

        foreach($scheduleHash as $name => $config){
            self::setSchedule($name, $config);
        }

        return self::reloadSchedules();


    }


    /**
     * Returns the schedule hash
     */
    public static function getScheduleHash(){

    }

    public static function schedules(){

        if(!self::$schedules || empty(self::$schedules) ) {
            self::$schedules = self::getSchedules();
        }

        return self::$schedules;
    }

    /**
     * reloads the schedule from redis
     */
    public static function reloadSchedules(){
        self::$schedules = self::getSchedules();
        return self::$schedules;
    }

    /**
     * gets the schedule as it exists in redis
     */
    public static function getSchedules(){
        $redis = Resque::redis();

        if(!$redis->exists('schedules')) return array();

        static $schedules = array();

        foreach($redis->hgetall('schedules') as $name => $config){
            $schedules[$name] = json_decode($config, true);
        }

        return $schedules;
    }


    /**
     * Create or update a schedule with the provided name and configuration.
     *
     * Note: values for class and custom_job_class need to be strings,
     * not constants.
     *
     *    Resque::setSchedule('some_job', {:class => 'SomeJob',
     *                                     :every => '15mins',
     *                                     :queue => 'high',
     *
     */
    public static function setSchedule($name, $config){

        $existing_config = self::getSchedule($name);
        $persist = isset($config['persist']) && $config['persist'];

        if( !$existing_config || $config != $existing_config){

            $redis = Resque::redis();
            $redis->hset('schedules', $name, json_encode($config));
            $redis->sadd('schedules_changed', $name);

            if ($persist) $redis.sadd('persisted_schedules', $name);
        }

        return $config;
    }

    public static function cleanSchedules(){

        $redis = Resque::redis();
        if( $redis->exists('schedules') ){

            foreach($redis->hkeys('schedules') as $name ){
                if(!self::isSchedulePersisted($name)){
                    self::removeSchedule($name);
                }
            }
        }
        self::$schedules = null;
        return true;
    }

    /**
     * retrive the schedule configuration for the given name
     */
    public static function getSchedule($name){
        return json_decode(Resque::redis()->hget('schedules', $name), true);
    }

    public static function isSchedulePersisted($name){
        return Resque::redis()->hget('persisted_schedules', $name);
    }

    /**
     * remove a given schedule by name
     */
    public static function removeSchedule($name){

        $redis = Resque::redis();
        $redis->hdel('schedules', $name);
        $redis->srem('persisted_schedules', $name);
        $redis->sadd('schedules_changed', $name);

    }

    public static function prepareSchedule($shedules){

        $prepared_hash = array();

        foreach($shedules as $name => $job){

            if (!isset($job['class'])) {
                $job['class'] = $name;
            }

            $prepared_hash[$name] = $job;
        }

        return $prepared_hash;
    }

    public static function lastEnqueuedAt($jobName, $date){
        Resque::redis()->hset('delayed:last_enqueued_at', $jobName, $date);
    }

    public static function getLastEnqueuedAt($jobName){
        return Resque::redis()->hget('delayed:last_enqueued_at', $jobName);
    }

    public static function scheduleForUpdate($date, $jobName){
        Resque::redis()->zadd('schedules:update_at', $date, $jobName);
    }

    public static function runScheduleForUpdate($timestamp, $interval){

        $min = $timestamp - 1;
        $max = $timestamp + $interval + 1;

        $update = Resque::redis()->zrangebyscore('schedules:update_at', "($min", "($max");

        if(!empty($update)){
            foreach($update as $name){
                Resque::redis()->sadd('schedules_changed', $name);
            }
            Resque::redis()->zremrangebyscore('schedules:update_at', "($min", "($max");
        }

    }

	
	/**
	 * Enqueue a job in a given number of seconds from now.
	 *
	 * Identical to Resque::enqueue, however the first argument is the number
	 * of seconds before the job should be executed.
	 *
	 * @param int $in Number of seconds from now when the job should be executed.
	 * @param string $queue The name of the queue to place the job in.
	 * @param string $class The name of the class that contains the code to execute the job.
	 * @param array $args Any optional arguments that should be passed when the job is executed.
	 */
	public static function enqueueIn($in, $queue, $class, array $args = array())
	{
		self::enqueueAt(time() + $in, $queue, $class, $args);
	}

	/**
	 * Enqueue a job for execution at a given timestamp.
	 *
	 * Identical to Resque::enqueue, however the first argument is a timestamp
	 * (either UNIX timestamp in integer format or an instance of the DateTime
	 * class in PHP).
	 *
	 * @param DateTime|int $at Instance of PHP DateTime object or int of UNIX timestamp.
	 * @param string $queue The name of the queue to place the job in.
	 * @param string $class The name of the class that contains the code to execute the job.
	 * @param array $args Any optional arguments that should be passed when the job is executed.
	 */
	public static function enqueueAt($at, $queue, $class, $args = array())
	{
		self::validateJob($class, $queue);

		$job = self::jobToHash($queue, $class, $args);
		self::delayedPush($at, $job);
		
		Resque_Event::trigger('afterSchedule', array(
			'at'    => $at,
			'queue' => $queue,
			'class' => $class,
			'args'  => $args,
		));
	}

	/**
	 * Directly append an item to the delayed queue schedule.
	 *
	 * @param DateTime|int $timestamp Timestamp job is scheduled to be run at.
	 * @param array $item Hash of item to be pushed to schedule.
	 */
	public static function delayedPush($timestamp, $item)
	{
		$timestamp = self::getTimestamp($timestamp);
		$redis = Resque::redis();
		$redis->rpush('delayed:' . $timestamp, json_encode($item));

		$redis->zadd('delayed_queue_schedule', $timestamp, $timestamp);
	}

	/**
	 * Get the total number of jobs in the delayed schedule.
	 *
	 * @return int Number of scheduled jobs.
	 */
	public static function getDelayedQueueScheduleSize()
	{
		return (int)Resque::redis()->zcard('delayed_queue_schedule');
	}

	/**
	 * Get the number of jobs for a given timestamp in the delayed schedule.
	 *
	 * @param DateTime|int $timestamp Timestamp
	 * @return int Number of scheduled jobs.
	 */
	public static function getDelayedTimestampSize($timestamp)
	{
		$timestamp = self::toTimestamp($timestamp);
		return Resque::redis()->llen('delayed:' . $timestamp, $timestamp);
	}

    /**
     * Remove a delayed job from the queue
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * also, this is an expensive operation because all delayed keys have tobe
     * searched
     *
     * @param $queue
     * @param $class
     * @param $args
     * @return int number of jobs that were removed
     */
    public static function removeDelayed($queue, $class, $args)
    {
       $destroyed=0;
       $item=json_encode(self::jobToHash($queue, $class, $args));
       $redis=Resque::redis();

       foreach($redis->keys('delayed:*') as $key)
       {
           $key=$redis->removePrefix($key);
           $destroyed+=$redis->lrem($key,0,$item);
       }

       return $destroyed;
    }

    /**
     * removed a delayed job queued for a specific timestamp
     *
     * note: you must specify exactly the same
     * queue, class and arguments that you used when you added
     * to the delayed queue
     *
     * @param $timestamp
     * @param $queue
     * @param $class
     * @param $args
     * @return mixed
     */
    public static function removeDelayedJobFromTimestamp($timestamp, $queue, $class, $args)
    {
        $key = 'delayed:' . self::getTimestamp($timestamp);
        $item = json_encode(self::jobToHash($queue, $class, $args));
        $redis = Resque::redis();
        $count = $redis->lrem($key, 0, $item);
        self::cleanupTimestamp($key, $timestamp);

        return $count;
    }
	
	/**
	 * Generate hash of all job properties to be saved in the scheduled queue.
	 *
	 * @param string $queue Name of the queue the job will be placed on.
	 * @param string $class Name of the job class.
	 * @param array $args Array of job arguments.
	 */

	private static function jobToHash($queue, $class, $args)
	{
		return array(
			'class' => $class,
			'args'  => array($args),
			'queue' => $queue,
		);
	}

	/**
	 * If there are no jobs for a given key/timestamp, delete references to it.
	 *
	 * Used internally to remove empty delayed: items in Redis when there are
	 * no more jobs left to run at that timestamp.
	 *
	 * @param string $key Key to count number of items at.
	 * @param int $timestamp Matching timestamp for $key.
	 */
	private static function cleanupTimestamp($key, $timestamp)
	{
		$timestamp = self::getTimestamp($timestamp);
		$redis = Resque::redis();

		if ($redis->llen($key) == 0) {
			$redis->del($key);
			$redis->zrem('delayed_queue_schedule', $timestamp);
		}
	}

	/**
	 * Convert a timestamp in some format in to a unix timestamp as an integer.
	 *
	 * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
	 * @return int Timestamp
	 * @throws ResqueScheduler_InvalidTimestampException
	 */
	private static function getTimestamp($timestamp)
	{
		if ($timestamp instanceof DateTime) {
			$timestamp = $timestamp->getTimestamp();
		}
		
		if ((int)$timestamp != $timestamp) {
			throw new ResqueScheduler_InvalidTimestampException(
				'The supplied timestamp value could not be converted to an integer.'
			);
		}

		return (int)$timestamp;
	}

	/**
	 * Find the first timestamp in the delayed schedule before/including the timestamp.
	 *
	 * Will find and return the first timestamp upto and including the given
	 * timestamp. This is the heart of the ResqueScheduler that will make sure
	 * that any jobs scheduled for the past when the worker wasn't running are
	 * also queued up.
	 *
	 * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
	 *                                Defaults to now.
	 * @return int|false UNIX timestamp, or false if nothing to run.
	 */
	public static function nextDelayedTimestamp($at = null)
	{
		if ($at === null) {
			$at = time();
		}
		else {
			$at = self::getTimestamp($at);
		}
	
		$items = Resque::redis()->zrangebyscore('delayed_queue_schedule', '-inf', $at, array('limit' => array(0, 1)));
		if (!empty($items)) {
			return $items[0];
		}
		
		return false;
	}	
	
	/**
	 * Pop a job off the delayed queue for a given timestamp.
	 *
	 * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
	 * @return array Matching job at timestamp.
	 */
	public static function nextItemForTimestamp($timestamp)
	{
		$timestamp = self::getTimestamp($timestamp);
		$key = 'delayed:' . $timestamp;
		
		$item = json_decode(Resque::redis()->lpop($key), true);
		
		self::cleanupTimestamp($key, $timestamp);
		return $item;
	}

	/**
	 * Ensure that supplied job class/queue is valid.
	 *
	 * @param string $class Name of job class.
	 * @param string $queue Name of queue.
	 * @throws Resque_Exception
	 */
	private static function validateJob($class, $queue)
	{
		if (empty($class)) {
			throw new Resque_Exception('Jobs must be given a class.');
		}
		else if (empty($queue)) {
			throw new Resque_Exception('Jobs must be put in a queue.');
		}
		
		return true;
	}
}
