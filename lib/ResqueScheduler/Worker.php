<?php
/**
 * ResqueScheduler worker to handle scheduling of delayed tasks.
 *
 * @package		ResqueScheduler
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @copyright	(c) 2012 Chris Boulton
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class ResqueScheduler_Worker
{
	const LOG_NONE = 0;
	const LOG_NORMAL = 1;
	const LOG_VERBOSE = 2;

    protected $dynamic = true;

	/**
	 * @var LoggerInterface Logging object that impliments the PSR-3 LoggerInterface
	 */
	public $logger;
	
	/**
	 * @var int Current log level of this worker.
	 */
	public $logLevel = 0;
	
	/**
	 * @var int Interval to sleep for between checking schedules.
	 */
	protected $interval = 5;

    protected $scheduledJobs;

    /**
     * @var string The hostname of this worker.
     */
    private $hostname;

    /**
     * @var string String identifying this worker.
     */
    private $id;

    public function __construct()
    {
        $this->logger = new Resque_Log();
        if(function_exists('gethostname')) {
            $hostname = gethostname();
        }
        else {
            $hostname = php_uname('n');
        }
        $this->hostname = $hostname;
        $this->id = $this->hostname . ':'.getmypid() . ':' . 'Scheduler';
    }

    /**
	* The primary loop for a worker.
	*
	* Every $interval (seconds), the scheduled queue will be checked for jobs
	* that should be pushed to Resque.
	*
	* @param int $interval How often to check schedules.
	*/
	public function work($interval = null)
	{
		if ($interval !== null) {
			$this->interval = $interval;
		}

		$this->updateProcLine('Starting');

        # Load the schedule into rufus
        # If dynamic is set, load that schedule otherwise use normal load
        if ( $this->dynamic ){
            $this->reloadSchedule();
        }
        else{
            $this->loadSchedule();
        }

		
		while (true) {
            $this->handleDelayedItems();
            $this->updateScheduled();

			$this->sleep();
		}
	}

    public function updateScheduled(){

        if ( Resque::redis()->scard('schedules_changed') > 0 ){

            $this->updateProcLine('Updating schedule');

            $reloadSchedules = ResqueScheduler::reloadSchedules();

            while( $schedule_name = Resque::redis()->spop('schedules_changed') ){
                if( array_key_exists($schedule_name,  $reloadSchedules) ){
                    $this->unscheduleJob($schedule_name);
                    $this->loadScheduleJob($schedule_name, $reloadSchedules[$schedule_name]);
                }else{
                    $this->unscheduleJob($schedule_name);
                }
            }

            $this->updateProcLine('Schedules Loaded');
        }else{
            $this->logger->log(
                Psr\Log\LogLevel::DEBUG,
                'Not Update Schedules {time}',
                array( 'time' => date('d-m-Y H:i:s')
                )
            );
        }

        ResqueScheduler::runScheduleForUpdate(time(), $this->interval);
    }


    public function loadScheduleJob($name, $config){


        $this->scheduledJobs[$name] = $config;

        $this->logger->log(
            Psr\Log\LogLevel::INFO,
            'Enqueuing {class} ({name})',
            array(
                'class' => $config['class'],
                'name'  => $name
            )
        );
        $cron = \Cron\CronExpression::factory( $config['cron'] );
        $matches = $cron->getMultipleRunDates(2);

        $config['schedule_at'] =  $matches[0]->getTimestamp();
        $config['update_at'] = $matches[1]->getTimestamp();

        if( $config['schedule_at'] != ResqueScheduler::getlastEnqueuedAt($name) ){
            $this->logger->log(
                Psr\Log\LogLevel::INFO,
                'Scheduling {name} At {schedule_at} with next update At {update_at}',
                array(
                    'name' => $name,
                    'schedule_at' => date('d-m-Y H:i:s', $config['schedule_at']),
                    'update_at' => date('d-m-Y H:i:s', $config['update_at'])
                )
            );
            ResqueScheduler::lastEnqueuedAt($name, $config['schedule_at'] );
            ResqueScheduler::scheduleForUpdate($config['update_at'], $name);
            Resque::redis()->srem('schedules_changed', $name);
            $this->enqueueFromConfig($config);
        }

    }

    public function reloadSchedule(){
        $this->updateProcLine('Reloading Schedule');
        $this->clearSchedule();
        $this->loadSchedule();
    }

    public function clearSchedule(){
        $this->scheduledJobs = array();
        //Falta Planificador CRON ?

    }

    public function loadSchedule(){
        $this->updateProcLine('Loading Schedule');
        /* if ( $this->dynamic )  */ResqueScheduler::reloadSchedules();

        $schedules = ResqueScheduler::schedules();
        if( empty( $schedules ) ) $this->logger->log(Psr\Log\LogLevel::NOTICE,'Schedule empty! Set Resque.schedule');
        $this->scheduledJobs = array();

        foreach($schedules as $name => $config ){
            $this->loadScheduleJob($name, $config);
        }

        Resque::redis()->del('schedules_changed');
        $this->updateProcLine('Schedules Loaded');

    }

    public function unscheduleJob($name){
        if(isset($this->scheduledJobs[$name])){
            ResqueScheduler::removeSchedule($name);
            //$this->scheduledJobs[$name]->unschedule();
            unset($this->scheduledJobs[$name]);
        }
    }

	/**
	 * Handle delayed items for the next scheduled timestamp.
	 *
	 * Searches for any items that are due to be scheduled in Resque
	 * and adds them to the appropriate job queue in Resque.
	 *
	 * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
	 */
	public function handleDelayedItems($timestamp = null)
	{
        while (($timestamp = ResqueScheduler::nextDelayedTimestamp($timestamp)) !== false) {
			$this->updateProcLine('Processing Delayed Items');
			$this->enqueueDelayedItemsForTimestamp($timestamp);
		}
	}
	
	/**
	 * Schedule all of the delayed jobs for a given timestamp.
	 *
	 * Searches for all items for a given timestamp, pulls them off the list of
	 * delayed jobs and pushes them across to Resque.
	 *
	 * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
	 */
	public function enqueueDelayedItemsForTimestamp($timestamp)
	{
        $item = null;
		while ($item = ResqueScheduler::nextItemForTimestamp($timestamp)) {
			$this->enqueueFromConfig($item);
		}
	}

    protected function enqueueFromConfig($config){

        if(isset($config['cron'])) {

            //ResqueScheduler::removeDelayed($config['args']['queue'], $config['class'], $config['args']);
            $this->logger->log(Psr\Log\LogLevel::NOTICE,'queueing {class} in {queue} Scheduled {schedule_at}', array('class' => $config['class'], 'queue' => $config['args']['queue'], 'schedule_at' => $config['schedule_at']));

            ResqueScheduler::enqueueAt($config['schedule_at'], $config['args']['queue'], $config['class'], $config['args']);


        }else{

            $this->logger->log(Psr\Log\LogLevel::INFO,'queueing {class} in {queue} [delayed]', array('class' => $config['class'], 'queue' => $config['queue']));

            Resque_Event::trigger('beforeDelayedEnqueue', array(
                'queue' => $config['queue'],
                'class' => $config['class'],
                'args'  => $config['args'],
            ));

            $payload = array_merge(array($config['queue'], $config['class']), $config['args']);
            call_user_func_array('Resque::enqueue', $payload);
        }


    }
	
	/**
	 * Sleep for the defined interval.
	 */
	protected function sleep()
	{
		sleep($this->interval);
	}
	
	/**
	 * Update the status of the current worker process.
	 *
	 * On supported systems (with the PECL proctitle module installed), update
	 * the name of the currently running process to indicate the current state
	 * of a worker.
	 *
	 * @param string $status The updated process title.
	 */
	private function updateProcLine($status)
	{
		$processTitle = 'resque-scheduler-' . ResqueScheduler::VERSION . ': ' . $status;
        if(function_exists('cli_set_process_title')) {
            cli_set_process_title($processTitle);
        }
        else if(function_exists('setproctitle')) {
            setproctitle($processTitle);
        }
	}
	
	/**
	 * Output a given log message to STDOUT.
	 *
	 * @param string $message Message to output.
	 */
	public function log($message)
	{
		if($this->logLevel == self::LOG_NORMAL) {
			fwrite(STDOUT, "*** " . $message . "\n");
		}
		else if($this->logLevel == self::LOG_VERBOSE) {
			fwrite(STDOUT, "** [" . strftime('%T %Y-%m-%d') . "] " . $message . "\n");
		}
	}

    /**
     * Inject the logging object into the worker
     *
     * @param Psr\Log\LoggerInterface $logger
     */
    public function setLogger(Psr\Log\LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
