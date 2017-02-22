<?php
namespace Splitice\DB;

use Radical\Database\Model\Table;

/**
 * @package Splitice\DB
 */
class JobQueue extends Table
{
	const TABLE_PREFIX = 'job_';
	const TABLE = 'job_queue';

	protected $id;
	protected $status;
	protected $updated;
	protected $payload;
	protected $owner;
	protected $queue;
	protected $queued;
}