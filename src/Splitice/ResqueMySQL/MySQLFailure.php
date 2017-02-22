<?php

namespace Splitice\ResqueMySQL;

use Radical\Database\Model\TableReferenceInstance;
use Resque\Component\Job\Model\JobInterface;
use Resque\Component\Queue\Model\OriginQueueAwareInterface;
use Resque\Component\Queue\Model\QueueInterface;
use Resque\Component\Worker\Model\WorkerInterface;
use Resque\Component\Job\Failure\FailureInterface;

/**
 * Default redis backend for storing failed jobs.
 */
class MySQLFailure implements FailureInterface
{
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
    public function save(JobInterface $job, \Exception $exception, WorkerInterface $worker)
    {
        $queue = ($job instanceof OriginQueueAwareInterface) ? $job->getOriginQueue() : null;

        $j = $this->table->fromId($job->getId());
		$j->setFailure(json_encode(
			array(
				'failed_at' => date('c'),
				'payload' => $job,
				'exception' => get_class($exception),
				'error' => $exception->getMessage(),
				'backtrace' => explode("\n", $exception->getTraceAsString()),
				'worker' => $worker->getId(),
				'queue' => ($queue instanceof QueueInterface) ? $queue->getName() : null,
			)
		));
		$j->setUpdated(\Radical\DB::toTimeStamp(time()));
		$j->update();
    }

    /**
     * {@inheritDoc}
     */
    public function count()
    {
    	return 0;
    }

    /**
     * {@inheritDoc}
     */
    public function clear()
    {
    }
}
