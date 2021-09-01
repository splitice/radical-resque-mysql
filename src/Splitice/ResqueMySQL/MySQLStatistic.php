<?php

namespace Splitice\ResqueMySQL;

use Predis\ClientInterface;
use Resque\Component\Job\Event\JobFailedEvent;
use Resque\Component\Statistic\StatisticInterface;
use Resque\Component\Worker\Event\WorkerJobEvent;

/**
 * Default redis backend for storing failed jobs.
 */
class MySQLStatistic implements
    StatisticInterface
{
    public function __construct()
    {
    }

    /**
     * Get the value of the supplied statistic counter for the specified statistic.
     *
     * @param string $stat The name of the statistic to get the stats for.
     * @return mixed Value of the statistic.
     */
    public function get($stat)
    {
    	//return MySQLStatistic::get('requeue:stat:');
    	return 0;
    }

    /**
     * Increment the value of the specified statistic by a certain amount (default is 1)
     *
     * @param string $stat The name of the statistic to increment.
     * @param int $by The amount to increment the statistic by.
     * @return boolean True if successful, false if not.
     */
    public function increment($stat, $by = 1)
    {
		//MySQLStatistic::increment('requeue:stat:' . $stat, $by);
    	return true;
    }

    /**
     * Decrement the value of the specified statistic by a certain amount (default is 1)
     *
     * @param string $stat The name of the statistic to decrement.
     * @param int $by The amount to decrement the statistic by.
     * @return boolean True if successful, false if not.
     */
    public function decrement($stat, $by = 1)
    {
    	//MySQLStatistic::decrement('requeue:stat:' . $stat, $by);
		return true;
    }

    /**
     * Delete a statistic with the given name.
     *
     * @param string $stat The name of the statistic to delete.
     * @return boolean True if successful, false if not.
     */
    public function clear($stat)
    {
    	//MySQLStatistic::clear($stat);
    	return true;
    }

    /**
     * @param WorkerJobEvent $event
     */
    public function jobProcessed(WorkerJobEvent $event)
    {
        $this->increment('processed');
        $this->increment('processed:' . $event->getWorker()->getId());
    }

    /**
     * @param JobFailedEvent $event
     */
    public function jobFailed(JobFailedEvent $event)
    {
        $this->increment('failed');
        $this->increment('failed:' . $event->getWorker()->getId());
    }
}
