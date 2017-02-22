<?php

namespace Splitice\ResqueMySQL;

use Radical\Database\Model\TableReferenceInstance;
use Radical\Database\SQL\Parts\Expression\Comparison;
use Resque\Component\Core\Event\EventDispatcherInterface;
use Resque\Component\Job\Model\FilterableJobInterface;
use Resque\Component\Job\Model\Job;
use Resque\Component\Job\Model\JobInterface;
use Resque\Component\Queue\Event\QueueJobEvent;
use Resque\Component\Queue\Model\OriginQueueAwareInterface;
use Resque\Component\Queue\Model\QueueInterface;
use Resque\Component\Queue\ResqueQueueEvents;
use Resque\Component\Queue\Storage\QueueStorageInterface;

/**
 * Resque Redis queue
 *
 * Uses redis to store the queue.
 */
class MySQLQueueStorage implements
    QueueStorageInterface
{
	protected $table;

    /**
     * Constructor.
     *
     */
    public function __construct(TableReferenceInstance $table)
    {
    	$this->table = $table;
    }

    /**
	 * {@inheritDoc}
	 */
    public function enqueue(QueueInterface $queue, JobInterface $job)
	{
		$new = $this->table->getNew();
		$new->setQueue($queue->getName());
		$new->setPayload($job->encode());
		//$new->setId($job->getId());

		$new->insert();
		return true;
	}

    /**
	 * {@inheritDoc}
	 */
    public function dequeue(QueueInterface $queue)
	{
		$owner = gethostname().'-'.getmypid();

		foreach ($this->table->getAll(array('job_owner'=>$owner, 'job_queued'=>'yes', 'job_queue'=>$queue->getName())) as $v){
			$v->setQueued('no');
			$v->update();
			return Job::decode($v->getPayload());
		}

		\Radical\DB::transaction(function() use($owner, $queue){
			$sql = 'UPDATE '.$this->table->getTable().' SET job_owner='.\Radical\DB::e($owner).' WHERE job_owner IS NULL
			 	AND job_queued="yes" AND job_queue='.\Radical\DB::e($queue->getName()).'
			 	ORDER BY job_id ASC LIMIT 1';
			\Radical\DB::q($sql);
		});

		foreach ($this->table->getAll(array('job_owner'=>$owner, 'job_queued'=>'yes', 'job_queue'=>$queue->getName())) as $v){
			$v->setQueued('no');
			$v->update();
			return Job::decode($v->getPayload());
		}
		return false;
	}

    /**
	 * {@inheritDoc}
	 */
    public function remove(QueueInterface $queue, $filter = array())
	{
		/*$jobsRemoved = 0;

		$queueKey = $this->getRedisKey($queue);
		$tmpKey = $queueKey . ':removal:' . time() . ':' . uniqid();
		$enqueueKey = $tmpKey . ':enqueue';

		// Move each job from original queue to a temporary list and leave
		while (\true) {
			$payload = $this->redis->rpoplpush($queueKey, $tmpKey);
			if (!empty($payload)) {
				$job = Job::decode($payload); // @todo should be something like $this->jobEncoderThingy->decode()
				if ($job instanceof FilterableJobInterface && $job::matchFilter($job, $filter)) {
					$this->redis->rpop($tmpKey);
					$jobsRemoved++;
				} else {
					$this->redis->rpoplpush($tmpKey, $enqueueKey);
				}
			} else {
				break;
			}
		}

		// Move back from enqueue list to original queue
		while (\true) {
			$payload = $this->redis->rpoplpush($enqueueKey, $queueKey);
			if (empty($payload)) {
				break;
			}
		}

		$this->redis->del($tmpKey);
		$this->redis->del($enqueueKey);

		return $jobsRemoved;*/
		return 0;
    }

    /**
     * {@inheritDoc}
     */
    public function count(QueueInterface $queue)
    {
        return count($this->table->getAll(array('job_queue'=>$queue->getName(),'job_queued'=>'yes')));
    }
}
