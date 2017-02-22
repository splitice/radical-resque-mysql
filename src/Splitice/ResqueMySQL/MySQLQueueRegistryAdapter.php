<?php

namespace Splitice\ResqueMySQL;

use Radical\Database\Model\TableReferenceInstance;
use Resque\Component\Queue\Model\QueueInterface;
use Resque\Component\Queue\Registry\QueueRegistryAdapterInterface;

/**
 * Redis queue registry adapter
 *
 * Connects redis in to the Resque core, and stores queues the original Resque way.
 */
class MySQLQueueRegistryAdapter implements
    QueueRegistryAdapterInterface
{
	protected $table;

    public function __construct(TableReferenceInstance $table)
    {
    	$this->table = $table;
    }

    /**
     * {@inheritDoc}
     */
    public function save(QueueInterface $queue)
    {
    	$this->table->insert(['qr_name'=>$queue->getName()],true)->execute();

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function has(QueueInterface $queue)
    {
        return $this->table->fromId($queue->getName()) ? true : false;
    }

    /**
     * {@inheritDoc}
     */
    public function delete(QueueInterface $queue)
	{
		$jq = $this->table->fromId($queue->getName());
		if($jq){
			$jq->delete();
			return true;
		}
		return false;
    }

    /**
     * {@inheritDoc}
     */
    public function all()
    {
        return $this->table->getAll();
    }
}
