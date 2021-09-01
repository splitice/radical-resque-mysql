<?php

namespace Splitice\ResqueMySQL;

use Radical\Database\Model\TableReferenceInstance;
use Radical\Database\SQL\Parts\Expression\Comparison;
use Radical\Database\SQL\Parts\Expression\In;
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
		$new->setFork($job->getFork() ? 'yes' : 'no');
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
        $updated = false;

		while(true) {
		    // Find jobs that we own
		    $set = $this->table->getAll(array('job_owner' => $owner, 'job_queued' => 'yes'));
            $set->sql->where_and(new Comparison('job_queue',new In(explode(',', $queue->getName()))));
            $set->sql->limit(1);
            foreach ($set as $v) {
                $v->setQueued('no');
                $v->update();
                $job = Job::decode($v->getPayload());
                $job->_mysqlId = $v->getId();
                $job->setFork($v->getFork() == 'yes');
                return $job;
            }

            // We did nothing last round
            if($updated) return false;

            while(true) {
                $stolen = false;

                // Get sampling of possible jobs for the queues
                $set = new In(explode(',', $queue->getName()));
                $sql = 'SELECT job_id FROM ' . $this->table->getTable() . ' WHERE job_owner IS NULL AND job_queued="yes" AND job_queue ' . $set->toSQL() . ' ORDER BY job_id ASC LIMIT 8';
                $res = \Radical\DB::q($sql);

                // Read the jobs
                $updated = true;
                $jobs = [];
                while ($row = $res->Fetch()) {
                    $updated = false;
                    $jobs[] = $row;
                }

                // No jobs queued
                if(empty($jobs)) break;

                // Shuffle jobs
                shuffle($jobs);

                // Attempt to steal a job
                foreach($jobs as $row){
                    $sql = 'UPDATE ' . $this->table->getTable() . ' SET job_owner=' . \Radical\DB::e($owner) . ' WHERE job_id=' . \Radical\DB::e($row['job_id']) . ' AND job_owner IS NULL AND job_queued="yes" LIMIT 1';
                    $result = \Radical\DB::q($sql);
                    if(!$result->affected_rows){
                        $stolen = true;
                    } else {
                        $stolen = false;
                        break;
                    }
                }

                if($stolen){
                    sleep(rand(550000,980000)/10000);
                } else{
                    break;
                }
            }

        }
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
        $set = $this->table->getAll(array('job_queued'=>'yes'));
        $set->sql->where_and(new Comparison('job_queue',new In(explode(',', $queue->getName()))));
        return count($set);
    }
}
