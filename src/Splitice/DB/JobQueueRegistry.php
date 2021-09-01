<?php
namespace Splitice\DB;

use Radical\Database\Model\Table;

/**
 * @package Splitice\DB
 */
class JobQueueRegistry extends Table
{
	const TABLE_PREFIX = 'qr_';
	const TABLE = 'job_queue_registry';

	protected $name;

}