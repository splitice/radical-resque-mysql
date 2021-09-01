<?php

namespace Splitice\ResqueMySQL;

use Radical\Database\Model\TableReferenceInstance;
use Resque\Component\Job\Model\JobInterface;
use Resque\Component\Job\Model\JobTrackerInterface;
use Resque\Component\Job\Model\TrackableJobInterface;

/**
 * Status tracker/information for a job.
 */
class MySQLJobTracker implements
    JobTrackerInterface
{
    public static $completedStates = array(
        JobInterface::STATE_FAILED,
        JobInterface::STATE_COMPLETE,
    );
	/**
	 * @var TableReferenceInstance
	 */
	private $table;


	public function __construct(TableReferenceInstance $table)
    {
		$this->table = $table;
	}

    /**
     * {@inheritDoc}
     */
    public function isTracking(JobInterface $job)
    {
    	return true;
    }

    /**
     * {@inheritDoc}
     */
    public function track(TrackableJobInterface $job)
    {
        $j = $this->table->fromId($job->getId());
        if(!$j) return;
        $j->setStatus($job->getState());
        $j->setUpdated(\Radical\DB::toTimeStamp(time()));
        $j->update();
	}

    /**
     * {@inheritDoc}
     */
    public function get(JobInterface $job)
    {
        if (!$this->isTracking($job)) {
            return false;
        }

		$j = $this->table->fromId($job->getId());
		if(!$j) return false;

        return $j->getStatus();
    }

    /**
     * {@inheritDoc}
     */
    public function stop(JobInterface $job)
    {
    }

    /**
     * {@inheritDoc}
     */
    public function isComplete(JobInterface $job)
    {
        $state = $this->get($job);

        if (in_array($state, static::$completedStates)) {
            return true;
        }

        return false;
    }
}
