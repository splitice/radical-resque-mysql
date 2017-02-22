<?php

namespace Splitice\ResqueMySQL;

use Radical\Database\Model\TableReferenceInstance;
use Resque\Component\Core\Event\EventDispatcherInterface;
use Resque\Component\Core\Exception\ResqueRuntimeException;
use Resque\Component\Queue\Model\OriginQueueAwareInterface;
use Resque\Component\Worker\Event\WorkerEvent;
use Resque\Component\Worker\Factory\WorkerFactoryInterface;
use Resque\Component\Worker\Model\WorkerInterface;
use Resque\Component\Worker\Registry\WorkerRegistryInterface;
use Resque\Component\Worker\ResqueWorkerEvents;
use Splitice\ResqueMySQL\Bridge\PredisBridge;

/**
 * Resque redis worker registry
 */
class MySQLWorkerRegistry implements
    WorkerRegistryInterface
{
    /**
     * @var WorkerFactoryInterface
     */
    protected $workerFactory;

    /**
     * @var EventDispatcherInterface
     */
    protected $eventDispatcher;
	/**
	 * @var TableReferenceInstance
	 */
	private $table;

	public function __construct(
    	TableReferenceInstance $table,
        EventDispatcherInterface $eventDispatcher,
        WorkerFactoryInterface $workerFactory
    ) {
        $this->eventDispatcher = $eventDispatcher;
        $this->workerFactory = $workerFactory;
		$this->table = $table;
	}

    /**
     * {@inheritDoc}
     */
    public function register(WorkerInterface $worker)
    {
        $id = $worker->getId();
        if ($this->isRegistered($worker)) {
            throw new ResqueRuntimeException(sprintf(
                'Cannot double register worker %s, deregister it before calling register again',
                $id
            ));
        }

        $this->table->insert(array('jw_id'=>$id, 'jw_started'=>\Radical\DB::toTimeStamp(time())))->execute();

        $this->eventDispatcher->dispatch(ResqueWorkerEvents::REGISTERED, new WorkerEvent($worker));

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function isRegistered(WorkerInterface $worker)
    {
        return $this->table->fromId($worker->getId()) ? true : false;
    }

    /**
     * {@inheritDoc}
     */
    public function deregister(WorkerInterface $worker)
    {
        $jw = $this->table->fromId($worker->getId());
        if($jw){
        	$jw->delete();
		}

        $this->eventDispatcher->dispatch(ResqueWorkerEvents::UNREGISTERED, new WorkerEvent($worker));

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function all()
    {
        /** @var WorkerInterface[] $instances */
        $instances = array();
        foreach ($this->table->getAll() as $jw) {
            $instances[] = $this->workerFactory->createWorkerFromId($jw->getId());
        }

        return $instances;
    }

    /**
     * {@inheritDoc}
     */
    public function count()
    {
        return count($this->table->getAll());
    }

    /**
     * {@inheritDoc}
     */
    public function findWorkerById($workerId)
    {
        $worker = $this->workerFactory->createWorkerFromId($workerId);

        if (false === $this->isRegistered($worker)) {

            return null;
        }

        return $worker;
    }

    /**
     * {@inheritDoc}
     */
    public function persist(WorkerInterface $worker)
    {
        $currentJob = $worker->getCurrentJob();

        if (null === $currentJob) {
        	//$jw = $this->table->fromId($worker->getId());
			//$jw->delete();

            $this->eventDispatcher->dispatch(ResqueWorkerEvents::PERSISTED, new WorkerEvent($worker));

            return $this;
        }

        /*$payload = json_encode(
            array(
                'queue' => ($currentJob instanceof OriginQueueAwareInterface) ? $currentJob->getOriginQueue() : null,
                'run_at' => date('c'),
                'payload' => $currentJob->encode(),
            )
        );

        $this->redis->set('worker:' . $worker->getId(), $payload);*/

        $this->eventDispatcher->dispatch(ResqueWorkerEvents::PERSISTED, new WorkerEvent($worker));

        return $this;
    }
}
